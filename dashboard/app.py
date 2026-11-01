"""Streamlit dashboard: batch demand, data quality, and stream windows.  `make dashboard`"""

import os
import subprocess
import sys
from pathlib import Path

import duckdb
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("TAXIFLOW_DATA", ROOT / "data")).resolve()
st.set_page_config(page_title="TaxiFlow", layout="wide")
st.title("TaxiFlow: NYC taxi batch + streaming analytics")


@st.cache_resource(show_spinner=False)
def bootstrap() -> None:
    """Fresh checkout (e.g. Streamlit Community Cloud): build the offline demo once."""
    if (DATA / "warehouse.duckdb").exists():
        return
    env = {**os.environ, "TAXIFLOW_DATA": str(DATA)}
    os.environ["TAXIFLOW_DATA"] = str(DATA)
    cli = [sys.executable, "-m", "taxiflow.cli"]
    with st.spinner("First start: building the demo warehouse (about a minute)..."):
        for args in (["sample"], ["ingest"], ["produce"], ["consume"]):
            subprocess.run(cli + args, check=True, cwd=ROOT, env=env)
        dbt = str(ROOT / "dbt")
        subprocess.run(
            [
                sys.executable,
                "-c",
                "from dbt.cli.main import cli; cli()",
                "build",
                "--project-dir",
                dbt,
                "--profiles-dir",
                dbt,
            ],
            check=True,
            cwd=dbt,
            env=env,
        )


bootstrap()


@st.cache_resource
def con():
    return duckdb.connect(str(DATA / "warehouse.duckdb"), read_only=True)


def q(sql):
    return con().execute(sql).df()


batch, quality, stream = st.tabs(["Batch demand", "Data quality", "Stream (replay)"])

with batch:
    k = q(
        "select sum(trips) trips, round(sum(revenue), 0) revenue, "
        "count(distinct pickup_date) n_days "
        "from marts.mart_zone_hourly_demand"
    )
    c1, c2, c3 = st.columns(3)
    c1.metric("Valid trips", f"{int(k.trips[0]):,}")
    c2.metric("Revenue (excl. refunds)", f"${k.revenue[0]:,.0f}")
    c3.metric("Days", int(k.n_days[0]))
    st.subheader("Trips by hour of day")
    st.bar_chart(
        q(
            "select pickup_hour, sum(trips) trips from marts.mart_zone_hourly_demand "
            "group by 1 order by 1"
        ),
        x="pickup_hour",
        y="trips",
    )
    st.subheader("Top pickup zones")
    st.dataframe(
        q(
            "select zone_name, borough, sum(trips) trips, round(sum(revenue),0) revenue "
            "from marts.mart_zone_hourly_demand group by all order by trips desc limit 15"
        ),
        width="stretch",
    )

with quality:
    st.caption("Every raw row is either a valid fact or quarantined with a reason.")
    st.dataframe(q("select * from marts.mart_dq_summary"), width="stretch")
    st.dataframe(q("select * from raw.ingest_manifest order by file_name"), width="stretch")

with stream:
    st.caption("Historical trips replayed as events (event time = dropoff). Not live NYC data.")
    w = q("select * from marts.mart_stream_windows")
    late = q("select count(*) n from staging.stg_stream_late").n[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("On-time unique events", f"{int(w.trips.sum()):,}")
    c2.metric("5-min windows", len(w))
    c3.metric("Late events (quarantined)", int(late))
    st.line_chart(w, x="window_start", y="trips")
