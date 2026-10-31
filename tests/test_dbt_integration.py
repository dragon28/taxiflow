import os
import subprocess
from pathlib import Path

import duckdb
import pytest

from taxiflow.cli import main

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"


@pytest.mark.integration
def test_full_pipeline_and_dbt_build(data_dir):
    main(["sample", "--rows", "5000"])
    main(["ingest"])
    main(["ingest"])  # idempotent rerun
    main(["produce"])
    main(["consume"])
    env = {**os.environ, "TAXIFLOW_DATA": str(data_dir)}
    r = subprocess.run(
        ["dbt", "build", "--profiles-dir", ".", "--target-path", str(data_dir / "dbt_target")],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stdout[-4000:]
    with duckdb.connect(str(data_dir / "warehouse.duckdb"), read_only=True) as con:
        reasons = dict(
            con.execute(
                "select dq_reason, count(*) from intermediate.int_trips_quarantine group by 1"
            ).fetchall()
        )
        assert {
            "dropoff_before_pickup",
            "unknown_zone",
            "implausible_distance",
            "pickup_outside_file_month",
        } <= set(reasons)
        assert con.execute("select count(*) from marts.mart_stream_windows").fetchone()[0] > 0
