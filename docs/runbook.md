# Runbook

* Rerun a month: `uv run taxiflow ingest` (unchanged files are skipped).
* A file failed: `uv run taxiflow status` shows the error in the manifest; fix and rerun.
* Inspect rejected rows: `select * from intermediate.int_trips_quarantine`.
* Consumer crashed: just restart `taxiflow consume`; it resumes from the committed offset.
* Late / bad events: `data/stream/late/`, `data/stream/dead_letter/`.
* Start over (destructive): `make reset`.
