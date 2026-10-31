"""Minimal broker abstraction.

FileBroker: an append-only local log with committed offsets -> runs anywhere, used in CI.
KafkaBroker: Redpanda/Kafka via confluent-kafka (optional extra, needs `make up`).

Both expose: produce(msgs), poll(max) -> [(offset, bytes)], commit() (commits what the
last poll returned). Callers commit only AFTER their output is durable.
"""

from __future__ import annotations

import itertools
import os
from pathlib import Path
from typing import Protocol


class Broker(Protocol):
    def produce(self, messages: list[bytes]) -> None: ...
    def poll(self, max_messages: int) -> list[tuple[int, bytes]]: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


class FileBroker:
    def __init__(self, root: Path, topic: str = "trips", group: str = "taxiflow-consumer"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.log = self.root / f"{topic}.log"
        self.offset_file = self.root / f"{topic}.{group}.offset"
        self._position: int | None = None

    def produce(self, messages: list[bytes]) -> None:
        with open(self.log, "ab") as fh:
            for m in messages:
                if b"\n" in m:
                    raise ValueError("FileBroker messages must not contain newlines")
                fh.write(m + b"\n")

    def committed(self) -> int:
        try:
            return int(self.offset_file.read_text().strip())
        except FileNotFoundError:
            return 0

    def poll(self, max_messages: int) -> list[tuple[int, bytes]]:
        start = self.committed() if self._position is None else self._position
        if not self.log.exists():
            return []
        with open(self.log, "rb") as fh:
            lines = list(itertools.islice(fh, start, start + max_messages))
        batch = [(start + i, line.rstrip(b"\n")) for i, line in enumerate(lines)]
        self._position = start + len(batch)
        return batch

    def commit(self) -> None:
        if self._position is None:
            return
        tmp = self.offset_file.with_suffix(".tmp")
        tmp.write_text(str(self._position))
        os.replace(tmp, self.offset_file)

    def close(self) -> None:
        pass


class KafkaBroker:  # pragma: no cover - exercised by the `kafka` CI job against Redpanda
    """Single-partition topic only: output file names are derived from offsets."""

    def __init__(
        self,
        bootstrap: str = "localhost:19092",
        topic: str = "trips",
        group: str = "taxiflow-consumer",
    ):
        from confluent_kafka import Consumer, Producer

        self.topic = topic
        self._producer = Producer(
            {"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"}
        )
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._subscribed = False
        self._errors: list[str] = []

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self._errors.append(str(err))

    def produce(self, messages: list[bytes]) -> None:
        for m in messages:
            while True:
                try:
                    self._producer.produce(self.topic, m, on_delivery=self._on_delivery)
                    break
                except BufferError:  # local queue full: let it drain, then retry
                    self._producer.poll(0.5)
            self._producer.poll(0)
        remaining = self._producer.flush(60)
        if remaining or self._errors:
            raise RuntimeError(
                f"{remaining} undelivered, {len(self._errors)} delivery errors: {self._errors[:3]}"
            )

    def poll(self, max_messages: int) -> list[tuple[int, bytes]]:
        if not self._subscribed:
            self._consumer.subscribe([self.topic])
            self._subscribed = True
        msgs = self._consumer.consume(num_messages=max_messages, timeout=5.0)
        out = []
        for m in msgs:
            if m.error():
                raise RuntimeError(m.error())
            if m.partition() != 0:
                raise RuntimeError("taxiflow expects a single-partition topic (partition 0)")
            out.append((m.offset(), m.value()))
        return out

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()


def make_broker(kind: str, root: Path, bootstrap: str = "localhost:19092") -> Broker:
    if kind == "file":
        return FileBroker(root)
    if kind == "kafka":
        return KafkaBroker(bootstrap)
    raise ValueError(f"unknown broker {kind!r}")
