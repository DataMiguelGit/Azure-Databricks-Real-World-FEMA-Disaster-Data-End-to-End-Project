def _abfss(container: str, path: str) -> str:
    return f"abfss://{container}@dlsfuturede.dfs.core.windows.net/{path}"


def landing_root() -> str:
    return _abfss("landing", f"openfema/")


def ops_manifest_path(ingest_date: str, load_id: str) -> str:

    return _abfss(
        "ops",
        f"openfema/manifests/ingest_date={ingest_date}/load_id={load_id}/manifest.json",
    )


def logs_run_dir(ingest_date: str, load_id: str) -> str:
    return _abfss("logs", f"openfema/ingest_date={ingest_date}/load_id={load_id}/")
