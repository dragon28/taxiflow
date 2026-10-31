"""Turn historical TLC trips into a replayable event stream (simulated streaming).

Event time = trip dropoff (the moment a trip "completes"). To exercise the consumer,
the replay deliberately injects: local disorder, duplicate deliveries, very late
events and malformed payloads. All chaos is seeded and therefore reproducible.
"""

from __future__ import annotations

import datetime as dt
import json
import random
from dataclasses import dataclass, field
from pathlib import Path

import duckdb


@dataclass
class ReplayStats:
    events: int = 0
    duplicates: int = 0
    late: int = 0
    invalid: int = 0
    messages: int = 0
    late_ids: list[str] = field(default_factory=list)


def load_trip_events(parquet: Path, limit: int | None = None) -> list[dict]:
    lim = f"LIMIT {int(limit)}" if limit else ""
    p = str(parquet).replace("'", "''")
    rows = duckdb.sql(
        f"""SELECT file_row_number, tpep_pickup_datetime, tpep_dropoff_datetime,
       PULocationID, DOLocationID, trip_distance, total_amount, payment_type
FROM read_parquet('{p}', file_row_number = true)
WHERE tpep_dropoff_datetime IS NOT NULL
ORDER BY tpep_dropoff_datetime, file_row_number {lim}"""
    ).fetchall()
    return [
        {
            "event_id": f"{parquet.name}:{r[0]}",
            "schema_version": 1,
            "event_time": r[2].isoformat(),
            "pickup_time": r[1].isoformat() if r[1] else None,
            "pu_location_id": r[3],
            "do_location_id": r[4],
            "trip_distance": r[5],
            "total_amount": r[6],
            "payment_type": r[7],
        }
        for r in rows
    ]


def build_replay(
    events: list[dict],
    seed: int = 7,
    jitter_s: float = 120,
    dup_rate: float = 0.02,
    late_count: int = 20,
    invalid_count: int = 5,
) -> tuple[list[bytes], ReplayStats]:
    rng = random.Random(seed)
    stats = ReplayStats(events=len(events))
    evs = list(events)

    # 1) very late events: pulled from the first 80% and delivered at the very end
    pool = range(0, int(len(evs) * 0.8))
    late_idx = set(rng.sample(pool, k=min(late_count, len(pool))))
    late = [evs[i] for i in sorted(late_idx)]
    evs = [e for i, e in enumerate(evs) if i not in late_idx]
    stats.late = len(late)
    stats.late_ids = [e["event_id"] for e in late]

    # 2) local disorder: delivery order = event_time + random jitter of up to `jitter_s`
    #    seconds, so disorder is time-bounded and stays inside the allowed lateness.
    if jitter_s > 0:
        evs.sort(
            key=lambda e: (
                dt.datetime.fromisoformat(e["event_time"])
                + dt.timedelta(seconds=rng.uniform(0, jitter_s))
            )
        )

    # 3) duplicate deliveries (at-least-once producers do this)
    out: list[dict] = []
    for e in evs:
        out.append(e)
        if rng.random() < dup_rate:
            out.append(e)
            stats.duplicates += 1
    out.extend(late)

    msgs = [json.dumps(e, separators=(",", ":")).encode() for e in out]

    # 4) malformed payloads
    bad = [
        b'{"event_id": "broken',
        b'{"schema_version":1}',
        b"not json",
        b"[]",
        b'{"event_id":"x","event_time":"yesterday"}',
    ]
    for i in range(invalid_count):
        msgs.insert(rng.randrange(len(msgs) + 1), bad[i % len(bad)])
    stats.invalid = invalid_count
    stats.messages = len(msgs)
    return msgs, stats
