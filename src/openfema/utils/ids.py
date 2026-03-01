import uuid
from datetime import datetime, timezone


def new_load_id() -> str:
    return str(uuid.uuid4())


def utc_ingest_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def utc_now_ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
