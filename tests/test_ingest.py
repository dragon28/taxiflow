import duckdb
import pyarrow.parquet as pq
import pytest

from taxiflow.batch.ingest import SchemaError, ingest_paths
from taxiflow.sample import write_sample


def count(db, where=""):
    with duckdb.connect(str(db)) as con:
        return con.execute(f"select count(*) from raw.yellow_trips {where}").fetchone()[0]


def test_rerun_is_noop(tmp_path):
    p = write_sample(tmp_path / "raw", n=1000)
    db = tmp_path / "w.duckdb"
    first = ingest_paths(db, [p])[0]
    expected = pq.read_metadata(p).num_rows
    assert first.status == "loaded" and first.rows == expected
    assert ingest_paths(db, [p])[0].status == "skipped"
    assert count(db) == expected


def test_identical_rows_are_not_collapsed(tmp_path):
    """The fixture contains exact duplicate trips; raw keeps every source row."""
    p = write_sample(tmp_path / "raw", n=1000)
    db = tmp_path / "w.duckdb"
    ingest_paths(db, [p])
    with duckdb.connect(str(db)) as con:
        distinct = con.execute(
            "select count(*) from (select distinct * exclude (source_row, loaded_at) "
            "from raw.yellow_trips)"
        ).fetchone()[0]
    assert distinct < count(db)


def test_changed_file_is_replaced_not_appended(tmp_path):
    raw = tmp_path / "raw"
    db = tmp_path / "w.duckdb"
    ingest_paths(db, [write_sample(raw, n=1000, seed=1)])
    p = write_sample(raw, n=500, seed=2)  # same file name, new content
    r = ingest_paths(db, [p])[0]
    assert r.status == "replaced"
    assert count(db) == pq.read_metadata(p).num_rows


def test_schema_error_rolls_back_and_is_recorded(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    bad = raw / "yellow_tripdata_2024-02.parquet"
    duckdb.sql(f"copy (select 1 as VendorID) to '{bad}' (format parquet)")
    db = tmp_path / "w.duckdb"
    with pytest.raises(SchemaError, match="missing required column"):
        ingest_paths(db, [bad])
    with duckdb.connect(str(db)) as con:
        assert con.execute("select status from raw.ingest_manifest").fetchone()[0] == "failed"
        assert con.execute("select count(*) from raw.yellow_trips").fetchone()[0] == 0
