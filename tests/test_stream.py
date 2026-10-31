import datetime as dt
import json

import duckdb
import pytest

from taxiflow.sample import write_sample
from taxiflow.stream.broker import FileBroker
from taxiflow.stream.consumer import StreamProcessor, run_consumer
from taxiflow.stream.events import build_replay, load_trip_events


def ev(i, minute):
    t = dt.datetime(2024, 1, 1, 12, 0) + dt.timedelta(minutes=minute)
    return json.dumps(
        {
            "event_id": f"e{i}",
            "event_time": t.isoformat(),
            "pu_location_id": 1,
            "total_amount": 10.0,
        }
    ).encode()


def test_processor_dedupes_quarantines_late_and_invalid():
    p = StreamProcessor(allowed_lateness=dt.timedelta(minutes=10))
    r = p.process(
        [
            (0, ev(1, 0)),
            (1, ev(2, 30)),
            (2, ev(1, 0)),  # duplicate
            (3, ev(3, 25)),  # within lateness
            (4, ev(4, 5)),  # behind watermark (20)
            (5, b"garbage"),
        ]
    )
    assert [e["event_id"] for e in r.accepted] == ["e1", "e2", "e3"]
    assert [e["event_id"] for e in r.late] == ["e4"]
    assert r.duplicates == 1 and len(r.invalid) == 1


@pytest.fixture
def replay(tmp_path):
    src = write_sample(tmp_path / "raw", n=3000)
    msgs, stats = build_replay(load_trip_events(src), seed=7)
    broker = FileBroker(tmp_path / "broker")
    broker.produce(msgs)
    return tmp_path, msgs, stats


def unique_events(out):
    return duckdb.sql(
        f"select count(distinct event_id), count(*) from read_parquet('{out}/events/**/*.parquet')"
    ).fetchone()


def test_end_to_end_replay_accounts_for_every_message(replay):
    tmp, msgs, stats = replay
    out = tmp / "stream"
    s = run_consumer(FileBroker(tmp / "broker"), out, batch_size=500)
    assert s.messages == len(msgs)
    assert s.invalid == stats.invalid
    assert s.late == stats.late  # injected very-late events are all caught
    distinct, total = unique_events(out)
    assert distinct == total == stats.events - stats.late
    assert s.accepted + s.late + s.invalid + s.duplicates_in_session == len(msgs)


def test_crash_before_commit_replays_without_duplicate_output(replay):
    tmp, msgs, _ = replay
    out = tmp / "stream"
    with pytest.raises(RuntimeError, match="simulated crash"):
        run_consumer(FileBroker(tmp / "broker"), out, batch_size=500, crash_before_commit=True)
    assert FileBroker(tmp / "broker").committed() == 0
    run_consumer(FileBroker(tmp / "broker"), out, batch_size=500)  # restart
    distinct, total = unique_events(out)
    assert distinct == total  # same batch rewritten to same file names, not appended
    assert FileBroker(tmp / "broker").committed() == len(msgs)


def test_restart_resumes_from_committed_offset(replay):
    tmp, msgs, _ = replay
    out = tmp / "stream"
    a = run_consumer(FileBroker(tmp / "broker"), out, batch_size=500, max_batches=2)
    b = run_consumer(FileBroker(tmp / "broker"), out, batch_size=500)
    assert a.messages + b.messages == len(msgs)


class FlakyEmptyBroker(FileBroker):
    """Returns empty polls first, like a Kafka consumer during group rebalance."""

    def __init__(self, root, empties):
        super().__init__(root)
        self.empties = empties

    def poll(self, max_messages):
        if self.empties:
            self.empties -= 1
            return []
        return super().poll(max_messages)


def test_initial_empty_polls_do_not_end_consumption(replay):
    tmp, msgs, _ = replay
    assert run_consumer(FlakyEmptyBroker(tmp / "broker", 2), tmp / "s1").messages == 0
    s = run_consumer(FlakyEmptyBroker(tmp / "broker", 2), tmp / "s2", idle_polls=3)
    assert s.messages == len(msgs)
