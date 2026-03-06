import dlt
from dlt.sources.rest_api import rest_api_source

# Configuration
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
            "paginator": {
                "type": "offset",
                "limit": 5,
                "offset": 0,
                "limit_param": "$top",
                "offset_param": "$skip",
                "maximum_offset": 100,
                "total_path": "metadata.count",
                "stop_after_empty_page": True,
            },
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

# Source and Pipeline
pipeline = dlt.pipeline(
    pipeline_name=dlt.config.get("sources.openfema.configs.pipeline_name", str),
    destination=dlt.config.get("sources.openfema.configs.destination", str),
    dataset_name=dlt.config.get("sources.openfema.configs.dataset_name", str),
)

openfema_source = rest_api_source(openfema_config)

# Extract Function


def run_openfema_extract():

    requested_resources = dlt.config.get("sources.openfema.configs.resources", list)
    available_resources = [r["name"] for r in openfema_config["resources"]]

    for resource in requested_resources:
        if resource not in available_resources:
            raise ValueError(
                f"Resource '{resource}' is not supported. Available: {available_resources}"
            )

    load_info = pipeline.run(
        openfema_source, schema_contract=dlt.config.get("schema_contract")
    )
    return load_info


if __name__ == "__main__":
    run_openfema_extract()