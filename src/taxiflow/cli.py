"""taxiflow CLI: sample | download | ingest | produce | consume | status"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from dataclasses import asdict
from pathlib import Path

from taxiflow import config


def cmd_sample(a: argparse.Namespace) -> None:
    p = __import__("taxiflow.sample", fromlist=["x"]).write_sample(
        config.raw_dir(), a.month, a.rows, a.seed
    )
    print(f"wrote {p}")


def cmd_download(a: argparse.Namespace) -> None:
    from taxiflow.batch.download import download_month, month_range

    for m in month_range(a.months):
        print(f"downloaded {download_month(m, config.raw_dir())}")


def cmd_ingest(a: argparse.Namespace) -> None:
    from taxiflow.batch.ingest import ingest_paths

    paths = [Path(p) for p in a.paths] or sorted(config.raw_dir().glob("yellow_tripdata_*.parquet"))
    if not paths:
        sys.exit("no input files; run `taxiflow sample` or `taxiflow download` first")
    for r in ingest_paths(config.warehouse_path(), paths):
        print(f"{r.status:9} {r.file} rows={r.rows}")


def _broker(a: argparse.Namespace):
    from taxiflow.stream.broker import make_broker

    return make_broker(a.broker, config.broker_dir(), a.bootstrap)


def cmd_produce(a: argparse.Namespace) -> None:
    from taxiflow.stream.events import build_replay, load_trip_events

    src = (
        Path(a.source)
        if a.source
        else sorted(config.raw_dir().glob("yellow_tripdata_*.parquet"))[0]
    )
    msgs, stats = build_replay(load_trip_events(src, a.limit), seed=a.seed)
    b = _broker(a)
    log = config.broker_dir() / "trips.log"
    if a.broker == "file" and log.exists() and log.stat().st_size and not a.force:
        print(f"skip: {log} already holds a replay (use --force to append another copy)")
        return
    b.produce(msgs)
    s = asdict(stats)
    s.pop("late_ids")
    print(f"produced from {src.name}: {json.dumps(s)}")


def cmd_consume(a: argparse.Namespace) -> None:
    from taxiflow.stream.consumer import run_consumer

    b = _broker(a)
    try:
        st = run_consumer(
            b,
            config.stream_dir(),
            a.batch_size,
            a.max_batches,
            dt.timedelta(minutes=a.lateness_minutes),
        )
    finally:
        b.close()
    print(f"consumed: {json.dumps(asdict(st))}")


def cmd_status(a: argparse.Namespace) -> None:
    import duckdb

    with duckdb.connect(str(config.warehouse_path()), read_only=True) as con:
        for row in con.execute("SELECT * FROM raw.ingest_manifest ORDER BY file_name").fetchall():
            print(row)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="taxiflow")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sample", help="write deterministic synthetic TLC-schema fixture")
    s.add_argument("--month", default="2024-01")
    s.add_argument("--rows", type=int, default=20_000)
    s.add_argument("--seed", type=int, default=42)
    s.set_defaults(fn=cmd_sample)

    s = sub.add_parser("download", help="download real TLC months, e.g. 2024-01:2024-03")
    s.add_argument("months")
    s.set_defaults(fn=cmd_download)

    s = sub.add_parser("ingest", help="idempotently load Parquet files into raw")
    s.add_argument("paths", nargs="*")
    s.set_defaults(fn=cmd_ingest)

    for name, fn in (("produce", cmd_produce), ("consume", cmd_consume)):
        s = sub.add_parser(name)
        s.add_argument("--broker", choices=["file", "kafka"], default="file")
        s.add_argument("--bootstrap", default="localhost:19092")
        s.set_defaults(fn=fn)
        if name == "produce":
            s.add_argument("--source")
            s.add_argument("--limit", type=int)
            s.add_argument("--seed", type=int, default=7)
            s.add_argument("--force", action="store_true", help="append even if log has data")
        else:
            s.add_argument("--batch-size", type=int, default=1000)
            s.add_argument("--max-batches", type=int)
            s.add_argument("--lateness-minutes", type=int, default=15)

    s = sub.add_parser("status", help="show ingestion manifest")
    s.set_defaults(fn=cmd_status)

    a = ap.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
