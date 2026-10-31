"""Stream consumer: validate -> dedupe -> event-time watermark -> durable Parquet sink.

Delivery semantics: at-least-once delivery + idempotent persistence.
  1. write output files for a batch (deterministic names part-<first>-<last>, atomic rename)
  2. commit broker offsets
  3. persist watermark state
A crash before (2) replays the same batch into the same file names -> overwrite, no dupes.
A crash between (2) and (3) restarts with an older watermark -> more lenient, nothing lost.
Duplicates spanning restarts are removed downstream (dbt dedupes on event_id).

Late policy: watermark = max event_time seen - allowed_lateness. Events older than the
watermark are routed to stream/late/ (counted, not silently dropped).
"""

from __future__ import annotations

import datetime as dt
import json
import os
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from taxiflow.stream.broker import Broker

REQUIRED = ("event_id", "event_time", "pu_location_id", "total_amount")

EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_time", pa.timestamp("us")),
        ("pickup_time", pa.timestamp("us")),
        ("pu_location_id", pa.int32()),
        ("do_location_id", pa.int32()),
        ("trip_distance", pa.float64()),
        ("total_amount", pa.float64()),
        ("payment_type", pa.int32()),
        ("source_offset", pa.int64()),
    ]
)
LATE_SCHEMA = EVENT_SCHEMA.append(pa.field("watermark", pa.timestamp("us")))


@dataclass
class BatchResult:
    accepted: list[dict] = field(default_factory=list)
    late: list[dict] = field(default_factory=list)
    invalid: list[dict] = field(default_factory=list)
    duplicates: int = 0


def parse_event(raw: bytes, offset: int) -> dict:
    try:
        ev = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"not json: {e}") from None
    if not isinstance(ev, dict):
        raise ValueError("payload is not an object")
    missing = [k for k in REQUIRED if ev.get(k) is None]
    if missing:
        raise ValueError(f"missing fields: {missing}")
    try:
        event_time = dt.datetime.fromisoformat(ev["event_time"])
        pickup = dt.datetime.fromisoformat(ev["pickup_time"]) if ev.get("pickup_time") else None
    except (TypeError, ValueError):
        raise ValueError(f"bad timestamp: {ev.get('event_time')!r}") from None
    return {
        "event_id": str(ev["event_id"]),
        "event_time": event_time,
        "pickup_time": pickup,
        "pu_location_id": int(ev["pu_location_id"]),
        "do_location_id": int(ev["do_location_id"])
        if ev.get("do_location_id") is not None
        else None,
        "trip_distance": ev.get("trip_distance"),
        "total_amount": float(ev["total_amount"]),
        "payment_type": ev.get("payment_type"),
        "source_offset": offset,
    }


class StreamProcessor:
    def __init__(
        self,
        allowed_lateness: dt.timedelta = dt.timedelta(minutes=15),
        max_event_time: dt.datetime | None = None,
        seen_capacity: int = 200_000,
    ):
        self.allowed_lateness = allowed_lateness
        self.max_event_time = max_event_time
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._cap = seen_capacity

    @property
    def watermark(self) -> dt.datetime | None:
        return None if self.max_event_time is None else self.max_event_time - self.allowed_lateness

    def _mark_seen(self, event_id: str) -> None:
        self._seen[event_id] = None
        if len(self._seen) > self._cap:
            self._seen.popitem(last=False)

    def process(self, messages: list[tuple[int, bytes]]) -> BatchResult:
        res = BatchResult()
        for offset, raw in messages:
            try:
                ev = parse_event(raw, offset)
            except ValueError as e:
                res.invalid.append(
                    {
                        "offset": offset,
                        "error": str(e),
                        "raw": raw.decode("utf-8", errors="replace"),
                    }
                )
                continue
            if ev["event_id"] in self._seen:
                res.duplicates += 1
                continue
            self._mark_seen(ev["event_id"])
            wm = self.watermark
            if wm is not None and ev["event_time"] < wm:
                res.late.append({**ev, "watermark": wm})
                continue
            res.accepted.append(ev)
            if self.max_event_time is None or ev["event_time"] > self.max_event_time:
                self.max_event_time = ev["event_time"]
        return res


def _atomic_write_parquet(rows: list[dict], schema: pa.Schema, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), tmp)
    os.replace(tmp, path)


def write_batch(out: Path, first: int, last: int, res: BatchResult) -> None:
    tag = f"part-{first:012d}-{last:012d}"
    by_date: dict[str, list[dict]] = {}
    for ev in res.accepted:
        by_date.setdefault(ev["event_time"].date().isoformat(), []).append(ev)
    for day, rows in by_date.items():
        _atomic_write_parquet(
            rows, EVENT_SCHEMA, out / "events" / f"event_date={day}" / f"{tag}.parquet"
        )
    if res.late:
        _atomic_write_parquet(res.late, LATE_SCHEMA, out / "late" / f"{tag}.parquet")
    if res.invalid:
        p = out / "dead_letter" / f"{tag}.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text("".join(json.dumps(r) + "\n" for r in res.invalid))
        os.replace(tmp, p)


@dataclass
class ConsumerStats:
    batches: int = 0
    messages: int = 0
    accepted: int = 0
    late: int = 0
    invalid: int = 0
    duplicates_in_session: int = 0
    watermark: str | None = None


def _load_state(out: Path) -> dt.datetime | None:
    try:
        s = json.loads((out / "_state" / "consumer.json").read_text())
        return dt.datetime.fromisoformat(s["max_event_time"]) if s.get("max_event_time") else None
    except FileNotFoundError:
        return None


def _save_state(out: Path, proc: StreamProcessor, stats: ConsumerStats) -> None:
    p = out / "_state" / "consumer.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    met = proc.max_event_time.isoformat() if proc.max_event_time else None
    tmp.write_text(json.dumps({"max_event_time": met, "last_session": asdict(stats)}, indent=2))
    os.replace(tmp, p)


def run_consumer(
    broker: Broker,
    out: Path,
    batch_size: int = 1000,
    max_batches: int | None = None,
    allowed_lateness: dt.timedelta = dt.timedelta(minutes=15),
    crash_before_commit: bool = False,
) -> ConsumerStats:
    """Consume until drained (or max_batches). `crash_before_commit` is a test hook."""
    proc = StreamProcessor(allowed_lateness, max_event_time=_load_state(out))
    stats = ConsumerStats()
    while max_batches is None or stats.batches < max_batches:
        msgs = broker.poll(batch_size)
        if not msgs:
            break
        res = proc.process(msgs)
        write_batch(out, msgs[0][0], msgs[-1][0], res)
        if crash_before_commit:
            raise RuntimeError("simulated crash after write, before offset commit")
        broker.commit()
        stats.batches += 1
        stats.messages += len(msgs)
        stats.accepted += len(res.accepted)
        stats.late += len(res.late)
        stats.invalid += len(res.invalid)
        stats.duplicates_in_session += res.duplicates
        stats.watermark = proc.watermark.isoformat() if proc.watermark else None
        _save_state(out, proc, stats)
    return stats
