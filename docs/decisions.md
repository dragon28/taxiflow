# Architecture decisions

1. **DuckDB as the warehouse.** Local, free, columnar, reads Parquet directly. Trade-off: one
   writer process. Mitigation: the stream consumer never writes to DuckDB; it writes Parquet.
2. **Pluggable broker.** `FileBroker` (append-only log + committed offset) makes the streaming
   path runnable and testable without Docker. `KafkaBroker` targets Redpanda with the same API.
3. **Record identity = (source_file, source_row).** TLC has no trip id; business-column hashes
   merge legitimate duplicates.
4. **At-least-once + idempotent sink** instead of claiming exactly-once.
5. **Quarantine with reason codes** instead of filtering in place, plus a reconciliation test.
6. **Synthetic fixture for CI**, clearly labelled, so builds never depend on public downloads.
