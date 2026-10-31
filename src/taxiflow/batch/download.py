"""Download monthly NYC TLC yellow-taxi Parquet files (optional; demo uses the fixture)."""

from __future__ import annotations

import urllib.request
from pathlib import Path

from taxiflow.config import TLC_TRIP_URL


def month_range(spec: str) -> list[str]:
    """'2024-01' or '2024-01:2024-03' -> ['2024-01', '2024-02', '2024-03']."""
    start, _, end = spec.partition(":")
    end = end or start
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def download_month(month: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / f"yellow_tripdata_{month}.parquet"
    if dest.exists():
        return dest
    tmp = dest.with_suffix(".part")
    req = urllib.request.Request(
        TLC_TRIP_URL.format(month=month), headers={"User-Agent": "taxiflow"}
    )
    with urllib.request.urlopen(req, timeout=120) as resp, open(tmp, "wb") as fh:
        while chunk := resp.read(1 << 20):
            fh.write(chunk)
    tmp.replace(dest)  # atomic: never leaves a half-written file under the final name
    return dest
