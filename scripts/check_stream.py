"""Assert stream output reconciles with the replay. Used by the Redpanda CI job.

Usage: python scripts/check_stream.py <fixture parquet> <stream dir>
"""

import sys

import duckdb

from taxiflow.stream.events import build_replay, load_trip_events

src, out = sys.argv[1], sys.argv[2]
from pathlib import Path  # noqa: E402

_, stats = build_replay(load_trip_events(Path(src)), seed=7)
con = duckdb.connect()
ev = f"read_parquet('{out}/events/**/*.parquet', hive_partitioning=true)"
late = f"read_parquet('{out}/late/*.parquet')"
on_time = con.execute(f"select count(distinct event_id) from {ev}").fetchone()[0]
late_ids = {r[0] for r in con.execute(f"select distinct event_id from {late}").fetchall()}
overlap = con.execute(
    f"select count(*) from (select distinct event_id from {ev}) e join (select distinct event_id "
    f"from {late}) l using (event_id)"
).fetchone()[0]
dead = sum(1 for p in Path(out, "dead_letter").glob("*.jsonl") for _ in open(p))
print(
    f"events={stats.events} on_time_unique={on_time} late_unique={len(late_ids)} "
    f"overlap={overlap} dead_letter={dead}"
)
assert overlap == 0, "an event is both on-time and late"
assert on_time + len(late_ids) == stats.events, "events lost or invented"
assert set(stats.late_ids) <= late_ids, "injected late events not all caught"
assert dead >= stats.invalid, "malformed messages not all dead-lettered"
print("OK")
