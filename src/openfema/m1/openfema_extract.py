# =============================================================================
# IMPORTS
# =============================================================================
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.destinations import filesystem
from src.openfema.utils.ids import new_load_id, utc_ingest_date, utc_now_ts
from src.openfema.utils.manifest import write_run_manifest_success
from src.openfema.utils.paths import landing_root, ops_manifest_path


# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================
def run_openfema_extract():
    """Execute the OpenFEMA data extraction pipeline."""

    paginator = {
        "type": "offset",
        "limit": 1000,
        "offset": 0,
        "limit_param": "$top",
        "offset_param": "$skip",
        "total_path": "metadata.count",
        "stop_after_empty_page": True,
    }

    openfema_config = {
        "client": {
            "base_url": dlt.config.get("sources.openfema.configs.base_url", str),
            **(
                {"auth": {"token": token}}
                if (token := dlt.secrets.get("sources.openfema.configs.token", None))
                else {}
            ),
            "headers": {"Accept": "application/json"},
        },
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "$orderby": "lastRefresh asc, id asc",
                    "$filter": "lastRefresh ge '{incremental.start_value}'",
                    "$metadata": "true",
                    "$count": "true",
                },
                "incremental": {
                    "cursor_path": "lastRefresh",
                    "initial_value": "1970-01-01T00:00:00.000Z",
                },
                "paginator": paginator,
            },
            "write_disposition": "append",
            "columns": {
                "id": {"data_type": "text"},
                "lastRefresh": {"data_type": "timestamp"},
            },
        },
        "resources": [
            {
                "name": "DisasterDeclarationsSummaries",
                "endpoint": {
                    "path": "v2/DisasterDeclarationsSummaries",
                    "data_selector": "DisasterDeclarationsSummaries",
                },
            },
            {
                "name": "FemaWebDisasterSummaries",
                "endpoint": {
                    "path": "v1/FemaWebDisasterSummaries",
                    "data_selector": "FemaWebDisasterSummaries",
                },
            },
        ],
    }

    run_id = new_load_id()
    ingest_date = utc_ingest_date()
    run_ts_utc = utc_now_ts()
    bucket_url = landing_root()

    manifest_url = ops_manifest_path(ingest_date, run_id)

    pipeline = dlt.pipeline(
        pipeline_name=dlt.config.get("sources.openfema.configs.pipeline_name", str),
        destination=filesystem(
            bucket_url=bucket_url,
            layout="{table_name}/ingest_date={ingest_date}/load_id={run_id}/{file_id}.{ext}",
            extra_placeholders={
                "ingest_date": ingest_date,
                "run_id": run_id,
            },
        ),
        dataset_name=dlt.config.get("sources.openfema.configs.dataset_name", str),
    )

    openfema_source = rest_api_source(openfema_config)

    requested_resources = dlt.config.get("sources.openfema.configs.resources", list)
    available_resources = [r["name"] for r in openfema_config["resources"]]

    if not requested_resources:
        requested_resources = available_resources

    for resource in requested_resources:
        if resource not in available_resources:
            raise ValueError(
                f"Resource '{resource}' is not supported. Available: {available_resources}"
            )

    source_to_run = openfema_source.with_resources(*requested_resources)

    load_info = pipeline.run(
        source_to_run,
        schema_contract=dlt.config.get("schema_contract"),
        loader_file_format="parquet",
    )

    manifest = write_run_manifest_success(
        load_id=run_id,
        ingest_date=ingest_date,
        run_ts_utc=run_ts_utc,
        bucket_url=bucket_url,
        landing_root_url=bucket_url,
        manifest_url=manifest_url,
        resources=requested_resources,
        load_info=load_info,
        source_url=dlt.config.get("sources.openfema.configs.base_url", str),
        extracted_at_utc=run_ts_utc,
    )

    print(f"[openfema] status=SUCCEEDED run_id={run_id} ingest_date={ingest_date}")
    print(f"[openfema] resources={requested_resources}")
    print(f"[openfema] manifest_url={manifest_url}")
    print(
        f"[openfema] records_loaded_by_resource="
        f"{manifest.get('records_loaded_by_resource')}"
    )

    return load_info


if __name__ == "__main__":
    run_openfema_extract()
