# Validation report

## Real NYC TLC data (run locally, 2026-10-31)

Machine: Fedora Linux, 24 CPU threads, 61 GB RAM. Separate data dir (`TAXIFLOW_DATA=data/real`);
the demo fixture has the same file name, so never download into the demo directory.

| File | Bytes | Rows |
|---|---|---|
| yellow_tripdata_2024-01.parquet | 49,961,641 | 2,964,624 |
| yellow_tripdata_2024-02.parquet | 50,349,284 | 3,007,526 |

* Schema check passed on both files; no code changes were needed for real data.
* First ingest: 5,972,150 rows loaded in 1.7 s (wall clock, warm cache). Rerun: both files skipped.
* `dbt build`: 39/39 steps succeeded in 5.3 s.
* Reconciliation: 5,972,150 raw = 5,968,472 valid + 3,678 quarantined.
* Quarantine reasons: duration_too_long 3,508; implausible_distance 74; dropoff_before_pickup 63;
  pickup_outside_file_month 33.
* Stream replay of a bounded slice (first 200,000 events of Jan 2024, file broker):
  203,965 messages -> 199,980 accepted, 3,960 duplicates removed, 20 late, 5 malformed.
  Every stream event joins back to a raw trip (dbt relationships test).

## Redpanda (GitHub Actions job `redpanda`)

Runs on every push: Redpanda in Docker, single-partition topic, fixture replay, consumer stopped after
3 batches and resumed from committed offsets, then reconciliation (`scripts/check_stream.py`),
zero consumer-group lag, and `dbt build`. See the CI badge in the README.

## Known limitations

* Kafka recovery is at-least-once: after a restart, batch boundaries may differ, so duplicate events
  can be written to new files; `stg_stream_events` removes them. Crash-before-commit is unit-tested
  only with the file broker.
* Single-partition topics only (offsets name the output files); the consumer refuses other partitions.
* Full-month streaming is not tuned (the file broker re-scans the log on each poll); use `--limit`.
* `total_amount < 0` is treated as a refund/dispute, so the `negative_amount` rule never fires.
  In this real data, no rows were rejected for negative amounts.
