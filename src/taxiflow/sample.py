"""Deterministic synthetic fixture in the exact NYC TLC yellow-taxi schema.

Used for CI and offline demos so nothing depends on network downloads.
It deliberately contains data-quality problems seen in the real files.
"""

from __future__ import annotations

import datetime as dt
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        ("VendorID", pa.int32()),
        ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("passenger_count", pa.int64()),
        ("trip_distance", pa.float64()),
        ("RatecodeID", pa.int64()),
        ("store_and_fwd_flag", pa.string()),
        ("PULocationID", pa.int32()),
        ("DOLocationID", pa.int32()),
        ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()),
        ("extra", pa.float64()),
        ("mta_tax", pa.float64()),
        ("tip_amount", pa.float64()),
        ("tolls_amount", pa.float64()),
        ("improvement_surcharge", pa.float64()),
        ("total_amount", pa.float64()),
        ("congestion_surcharge", pa.float64()),
        ("Airport_fee", pa.float64()),
    ]
)

# Busy Manhattan / airport zones get most of the demand, like the real data.
HOT_ZONES = [132, 138, 161, 162, 163, 164, 186, 230, 236, 237, 48, 68, 79, 142, 170, 234, 239]
HOUR_WEIGHTS = [3, 2, 1, 1, 1, 2, 4, 6, 7, 6, 6, 6, 7, 7, 7, 8, 9, 10, 10, 9, 8, 7, 6, 4]


def _month_bounds(month: str) -> tuple[dt.datetime, dt.datetime]:
    y, m = map(int, month.split("-"))
    start = dt.datetime(y, m, 1)
    end = dt.datetime(y + (m == 12), m % 12 + 1, 1)
    return start, end


def generate_rows(month: str = "2024-01", n: int = 20_000, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    start, end = _month_bounds(month)
    days = (end - start).days
    rows = []
    for _ in range(n):
        pickup = start + dt.timedelta(
            days=rng.randrange(days),
            hours=rng.choices(range(24), weights=HOUR_WEIGHTS)[0],
            seconds=rng.randrange(3600),
        )
        dist = round(rng.lognormvariate(0.6, 0.8), 2)
        minutes = max(1.0, dist * rng.uniform(3, 6) + rng.uniform(0, 5))
        payment = rng.choices([1, 2, 3, 4], weights=[75, 22, 2, 1])[0]
        fare = round(3 + dist * 2.5 + minutes * 0.7, 2)
        tip = round(fare * rng.choice([0, 0.15, 0.2, 0.25]), 2) if payment == 1 else 0.0
        airport = 1.75 if rng.random() < 0.05 else 0.0
        total = round(fare + 1.0 + 0.5 + tip + 1.0 + 2.5 + airport, 2)
        rows.append(
            {
                "VendorID": rng.choice([1, 2]),
                "tpep_pickup_datetime": pickup,
                "tpep_dropoff_datetime": pickup + dt.timedelta(minutes=minutes),
                "passenger_count": rng.choices([1, 2, 3, 4, None], weights=[70, 15, 5, 5, 5])[0],
                "trip_distance": dist,
                "RatecodeID": 1,
                "store_and_fwd_flag": "N",
                "PULocationID": rng.choice(HOT_ZONES)
                if rng.random() < 0.7
                else rng.randint(1, 263),
                "DOLocationID": rng.choice(HOT_ZONES)
                if rng.random() < 0.6
                else rng.randint(1, 263),
                "payment_type": payment,
                "fare_amount": fare,
                "extra": 1.0,
                "mta_tax": 0.5,
                "tip_amount": tip,
                "tolls_amount": 0.0,
                "improvement_surcharge": 1.0,
                "total_amount": total,
                "congestion_surcharge": 2.5,
                "Airport_fee": airport,
            }
        )
    _inject_anomalies(rows, rng)
    rows.sort(key=lambda r: r["tpep_pickup_datetime"])
    return rows


def _inject_anomalies(rows: list[dict], rng: random.Random) -> None:
    """Mutate a small, deterministic share of rows into realistic bad/odd records."""
    idx = rng.sample(range(len(rows)), k=min(len(rows), 260))
    for i in idx[0:60]:  # dropoff before pickup
        r = rows[i]
        r["tpep_dropoff_datetime"] = r["tpep_pickup_datetime"] - dt.timedelta(minutes=7)
    for i in idx[60:80]:  # stray old timestamps outside the file month (seen in real TLC files)
        r = rows[i]
        r["tpep_pickup_datetime"] = r["tpep_pickup_datetime"].replace(year=2009)
        r["tpep_dropoff_datetime"] = r["tpep_dropoff_datetime"].replace(year=2009)
    for i in idx[80:120]:  # zone id not in lookup
        rows[i]["PULocationID"] = 999
    for i in idx[120:160]:  # refunds / disputes: negative but legitimate adjustments
        r = rows[i]
        r["payment_type"] = 4
        for c in ("fare_amount", "extra", "mta_tax", "improvement_surcharge", "total_amount"):
            r[c] = -abs(r[c])
    for i in idx[160:200]:  # absurd distance (odometer glitches)
        rows[i]["trip_distance"] = 12_000.0
    for i in idx[200:260]:  # exact duplicates of real trips: must NOT be collapsed by ingestion
        rows.append(dict(rows[i]))


def write_sample(out_dir: Path, month: str = "2024-01", n: int = 20_000, seed: int = 42) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"yellow_tripdata_{month}.parquet"
    table = pa.Table.from_pylist(generate_rows(month, n, seed), schema=SCHEMA)
    pq.write_table(table, path)
    return path
