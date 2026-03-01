from src.openfema.utils.ids import new_load_id, utc_ingest_date, utc_now_ts

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