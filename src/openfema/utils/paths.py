import os
from pyspark.sql import SparkSession

_ALLOWED_ENVS = {"dev", "stg", "prod"}


def get_environment() -> str:
    """
    Detects the current execution environment with the following priority:

    1. ENV environment variable (injected from databricks.yml via jobs/pipelines)
    2. Spark config bundle.target (when running in bundle deployments)
    3. Default: "dev"

    Returns:
        str: Environment name (dev, stg, prod)
    """

    env = os.getenv("ENV")  # Priority 1: Explicit ENV variable
    if env:
        env = env.lower().strip()
        if env in _ALLOWED_ENVS:
            return env

    try:
        spark = SparkSession.builder.getOrCreate()
        bundle_target = spark.conf.get(
            "bundle.target", None
        )  # Priority 2: Spark config
        if bundle_target:
            bundle_target = bundle_target.lower().strip()
            if bundle_target in _ALLOWED_ENVS:
                return bundle_target
    except Exception:
        pass

    return "dev"  # Priority 3: Default


def _abfss(container: str, path: str) -> str:
    return f"abfss://{container}@dlsfuturede.dfs.core.windows.net/{path}"


def landing_base(entity: str, ingest_date: str) -> str:
    env = get_environment()
    return _abfss("landing", f"openfema/{env}/{entity}/ingest_date={ingest_date}/")


def landing_run_dir(entity: str, ingest_date: str, load_id: str) -> str:
    env = get_environment()
    return _abfss(
        "landing",
        f"openfema/{env}/{entity}/ingest_date={ingest_date}/load_id={load_id}/",
    )


def ops_manifest_path(ingest_date: str, load_id: str) -> str:
    env = get_environment()
    return _abfss(
        "ops",
        f"openfema/{env}/manifests/ingest_date={ingest_date}/load_id={load_id}/manifest.json",
    )


def logs_run_dir(ingest_date: str, load_id: str) -> str:
    env = get_environment()
    return _abfss(
        "logs", f"openfema/{env}/ingest_date={ingest_date}/load_id={load_id}/"
    )
