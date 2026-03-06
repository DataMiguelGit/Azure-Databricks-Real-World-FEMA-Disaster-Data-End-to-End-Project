# =============================================================================
# IMPORTS
# =============================================================================
import sys
from pathlib import Path

# Add project root to Python path to enable 'src' imports
project_root = Path(__file__).resolve().parents[3]  # Goes up to DEV_Openfema_Project
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Imports below require sys.path modification above
import argparse  # noqa: E402
import dlt  # noqa: E402
from dlt.sources.rest_api import rest_api_source  # noqa: E402
from dlt.destinations import filesystem  # noqa: E402
from src.openfema.utils.ids import new_load_id, utc_ingest_date  # noqa: E402
from src.openfema.utils.paths import landing_root  # noqa: E402


# =============================================================================
# ARGUMENT PARSER
# =============================================================================
def parse_args():
    """Parse command-line arguments for paginator configuration."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--paginator-limit", type=int, required=True)
    parser.add_argument("--paginator-maximum-offset", type=int, required=True)
    return parser.parse_args()


# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================
def run_openfema_extract():
    """Execute the OpenFEMA data extraction pipeline."""

    # -------------------------------------------------------------------------
    # 1. Parse command-line arguments
    # -------------------------------------------------------------------------
    args = parse_args()

    # -------------------------------------------------------------------------
    # 2. Configure paginator for API requests
    # -------------------------------------------------------------------------
    paginator = {
        "type": "offset",
        "limit": args.paginator_limit,
        "offset": 0,
        "limit_param": "$top",
        "offset_param": "$skip",
        "total_path": "metadata.count",
        "stop_after_empty_page": True,
    }

    # Set maximum offset if specified (skip if -1)
    if args.paginator_maximum_offset != -1:
        paginator["maximum_offset"] = args.paginator_maximum_offset

    # -------------------------------------------------------------------------
    # 3. Configure OpenFEMA REST API source
    # -------------------------------------------------------------------------
    openfema_config = {
        # API client configuration
        "client": {
            "base_url": dlt.config.get("sources.openfema.configs.base_url", str),
            **(
                {"auth": {"token": token}}
                if (token := dlt.secrets.get("sources.openfema.configs.token", None))
                else {}
            ),
            "headers": {"Accept": "application/json"},
        },
        # Default configuration for all resources
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
        # Available resources to extract
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

    # -------------------------------------------------------------------------
    # 4. Generate run metadata (load ID, ingest date, destination path)
    # -------------------------------------------------------------------------
    run_id = new_load_id()
    ingest_date = utc_ingest_date()
    bucket_url = landing_root()

    # -------------------------------------------------------------------------
    # 5. Initialize dlthub pipeline with filesystem destination custom
    # -------------------------------------------------------------------------
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
        loader_file_format="parquet",
    )

    # -------------------------------------------------------------------------
    # 6. Create REST API source from configuration
    # -------------------------------------------------------------------------
    openfema_source = rest_api_source(openfema_config)

    # -------------------------------------------------------------------------
    # 7. Validate requested resources against available resources
    # -------------------------------------------------------------------------
    requested_resources = dlt.config.get("sources.openfema.configs.resources", list)
    available_resources = [r["name"] for r in openfema_config["resources"]]

    for resource in requested_resources:
        if resource not in available_resources:
            raise ValueError(
                f"Resource '{resource}' is not supported. Available: {available_resources}"
            )

    # -------------------------------------------------------------------------
    # 8. Execute pipeline and return load information
    # -------------------------------------------------------------------------
    load_info = pipeline.run(
        openfema_source, schema_contract=dlt.config.get("schema_contract")
    )
    return load_info


if __name__ == "__main__":
    run_openfema_extract()
