from src.openfema.utils.ids import new_load_id, utc_ingest_date, utc_now_ts
from src.openfema.utils.paths import landing_root, ops_manifest_path


def test_ids_smoke():
    load_id = new_load_id()
    assert isinstance(load_id, str)
    assert len(load_id) == 36

    d = utc_ingest_date()
    assert isinstance(d, str)
    assert len(d) == 10

    ts = utc_now_ts()
    assert isinstance(ts, str)
    assert "T" in ts


def test_paths_contract_smoke():
    ingest_date = "2026-03-09"
    load_id = "00000000-0000-0000-0000-000000000000"

    assert (
        landing_root()
        == "abfss://landing@dlsfuturede.dfs.core.windows.net/openfema/"
    )
    assert ops_manifest_path(ingest_date, load_id) == (
        "abfss://ops@dlsfuturede.dfs.core.windows.net/"
        "openfema/manifests/ingest_date=2026-03-09/"
        "load_id=00000000-0000-0000-0000-000000000000/manifest.json"
    )
