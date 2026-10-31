"""Central paths/settings. Everything lives under TAXIFLOW_DATA (default ./data)."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TLC_TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"


def data_dir() -> Path:
    return Path(os.environ.get("TAXIFLOW_DATA", ROOT / "data")).resolve()


def raw_dir() -> Path:
    return data_dir() / "raw"


def warehouse_path() -> Path:
    return data_dir() / "warehouse.duckdb"


def stream_dir() -> Path:
    return data_dir() / "stream"


def broker_dir() -> Path:
    return data_dir() / "broker"
