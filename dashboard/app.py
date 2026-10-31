"""Streamlit dashboard: batch demand, data quality, and stream windows.  `make dashboard`"""

import os
from pathlib import Path

import duckdb
import streamlit as st

DATA = Path(os.environ.get("TAXIFLOW_DATA", Path(__file__).resolve().parents[1] / "data"))
st.set_page_config(page_title="TaxiFlow", layout="wide")
st.title("TaxiFlow: NYC taxi batch + streaming analytics")


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
    st.dataframe(
        q("select * from raw.ingest_manifest order by file_name"), width="stretch"
    )

with stream:
    st.caption("Historical trips replayed as events (event time = dropoff). Not live NYC data.")
    w = q("select * from marts.mart_stream_windows")
    late = q("select count(*) n from staging.stg_stream_late").n[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("On-time unique events", f"{int(w.trips.sum()):,}")
    c2.metric("5-min windows", len(w))
    c3.metric("Late events (quarantined)", int(late))
    st.line_chart(w, x="window_start", y="trips")
