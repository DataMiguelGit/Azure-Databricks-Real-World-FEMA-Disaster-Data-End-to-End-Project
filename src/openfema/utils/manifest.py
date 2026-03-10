import json
import gzip
from typing import Any
from urllib.parse import urlparse

from fsspec.core import url_to_fs

_RESOURCE_KEYS = ("resource", "resource_name", "table_name", "name")
_COUNT_KEYS = (
    "row_count",
    "rows",
    "records",
    "items_count",
    "loaded_items",
    "inserted_count",
)


def _abfs_storage_options(url: str) -> dict[str, str | bool]:
    """
    Build best-effort ABFS credentials from dlt secrets.
    Kept local to avoid changing ingestion architecture.
    """
    try:
        import dlt
    except Exception:
        return {}

    account_name = dlt.secrets.get(
        "destination.filesystem.credentials.azure_storage_account_name", None
    )
    account_key = dlt.secrets.get(
        "destination.filesystem.credentials.azure_storage_account_key", None
    )
    sas_token = dlt.secrets.get(
        "destination.filesystem.credentials.azure_storage_sas_token", None
    )

    options: dict[str, str | bool] = {}

    parsed = urlparse(url)
    url_has_account_name = bool(parsed.username) and bool(parsed.hostname) and (
        parsed.hostname.endswith("blob.core.windows.net")
        or parsed.hostname.endswith("dfs.core.windows.net")
    )

    if isinstance(account_name, str) and account_name and not url_has_account_name:
        options["account_name"] = account_name
    if isinstance(account_key, str) and account_key:
        options["account_key"] = account_key
    if isinstance(sas_token, str) and sas_token:
        options["sas_token"] = sas_token

    if "account_key" in options or "sas_token" in options:
        options["anon"] = False

    return options


def _url_to_fs(url: str):
    if url.startswith(("abfs://", "abfss://", "az://")):
        return url_to_fs(url, **_abfs_storage_options(url))
    return url_to_fs(url)


def _walk(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk(item)


def _load_info_to_dict(load_info: Any) -> dict[str, Any]:
    if load_info is None:
        return {}

    if isinstance(load_info, dict):
        return load_info

    for attr in ("asdict", "to_dict", "dict"):
        fn = getattr(load_info, attr, None)
        if callable(fn):
            try:
                data = fn()
                if isinstance(data, dict):
                    return data
            except Exception:
                pass

    return {"repr": str(load_info)}


def extract_row_count_by_resource(
    load_info: Any,
    resources: list[str],
) -> dict[str, int | None]:
    """
    Best-effort extraction of per-resource row counts from load_info,
    without depending on a specific dlt internal version.
    Returns None for any resource whose count cannot be determined.
    """
    counts: dict[str, int | None] = {resource: None for resource in resources}
    payload = _load_info_to_dict(load_info)

    for node in _walk(payload):
        if not isinstance(node, dict):
            continue

        resource_name = None
        for key in _RESOURCE_KEYS:
            value = node.get(key)
            if isinstance(value, str) and value in counts:
                resource_name = value
                break

        if resource_name is None:
            continue

        count_value = None
        for key in _COUNT_KEYS:
            value = node.get(key)
            if isinstance(value, int):
                count_value = value
                break

        if count_value is not None:
            counts[resource_name] = count_value

    return counts


def list_files_under(url: str) -> list[str]:
    fs, path = _url_to_fs(url)

    try:
        found = sorted(fs.find(path))
    except FileNotFoundError:
        return []
    except Exception:
        return []

    files: list[str] = []
    for item in found:
        try:
            if hasattr(fs, "isdir") and fs.isdir(item):
                continue
        except Exception:
            pass

        if hasattr(fs, "unstrip_protocol"):
            files.append(fs.unstrip_protocol(item))
        else:
            files.append(item)

    return files


def write_json_to_url(url: str, payload: dict[str, Any]) -> None:
    fs, path = _url_to_fs(url)
    parent = path.rsplit("/", 1)[0]

    fs.makedirs(parent, exist_ok=True)

    with fs.open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _rows_from_file(url: str) -> int | None:
    fs, path = _url_to_fs(url)

    try:
        if url.endswith(".parquet"):
            import pyarrow.parquet as pq

            with fs.open(path, "rb") as f:
                return int(pq.ParquetFile(f).metadata.num_rows)

        if url.endswith(".jsonl"):
            with fs.open(path, "rt", encoding="utf-8") as f:
                return sum(1 for _ in f)

        if url.endswith(".jsonl.gz"):
            with fs.open(path, "rb") as f:
                with gzip.open(f, "rt", encoding="utf-8") as gz:
                    return sum(1 for _ in gz)
    except Exception:
        return None

    return None


def _count_rows_from_files(files: list[str]) -> int | None:
    has_known = False
    total = 0
    for file_url in files:
        rows = _rows_from_file(file_url)
        if rows is None:
            continue
        has_known = True
        total += rows
    return total if has_known else None


def write_run_manifest_success(
    *,
    load_id: str,
    ingest_date: str,
    run_ts_utc: str,
    bucket_url: str,
    landing_root_url: str,
    manifest_url: str,
    resources: list[str],
    load_info: Any = None,
    source_url: str | None = None,
    extracted_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build and write the manifest for a successful run."""
    dlt_row_count_by_resource = extract_row_count_by_resource(load_info, resources)
    row_count_by_resource: dict[str, int | None] = {}
    files_written_by_resource: dict[str, int] = {}

    resource_stats: list[dict[str, Any]] = []
    all_files: list[str] = []

    for resource in resources:
        landing_prefix = (
            f"{landing_root_url.rstrip('/')}/{resource}"
            f"/ingest_date={ingest_date}/load_id={load_id}"
        )
        files = list_files_under(landing_prefix)
        all_files.extend(files)
        files_written_by_resource[resource] = len(files)

        rows_loaded = _count_rows_from_files(files)
        if rows_loaded is None:
            rows_loaded = dlt_row_count_by_resource.get(resource)
        row_count_by_resource[resource] = rows_loaded

        resource_stats.append(
            {
                "resource": resource,
                "landing_prefix": landing_prefix,
                "files_written": len(files),
                "records_loaded": rows_loaded,
                "row_count": rows_loaded,
                "files": files,
            }
        )

    total_rows = [
        value for value in row_count_by_resource.values() if isinstance(value, int)
    ]

    manifest: dict[str, Any] = {
        "schema_version": "1.0",
        "source_system": "openfema",
        "status": "SUCCEEDED",
        "error_message": None,
        "load_id": load_id,
        "ingest_date": ingest_date,
        "run_ts_utc": run_ts_utc,
        "generated_at_utc": run_ts_utc,
        "bucket_url": bucket_url,
        "resources_ran": resources,
        "files_written": all_files,
        "files_written_by_resource": files_written_by_resource,
        "records_loaded_by_resource": row_count_by_resource,
        "row_count_by_resource": row_count_by_resource,
        "resource_stats": resource_stats,
        "totals": {
            "resources_ran": len(resources),
            "files_written": len(all_files),
            "rows": sum(total_rows) if total_rows else None,
        },
    }

    if source_url is not None:
        manifest["source_url"] = source_url

    if extracted_at_utc is not None:
        manifest["extracted_at_utc"] = extracted_at_utc

    write_json_to_url(manifest_url, manifest)
    return manifest
