"""Idempotent, schema-validated loading of TLC Parquet files into DuckDB `raw`.

Identity of a raw record = (source_file, source_row). TLC has no trip id, and hashing
business columns would wrongly collapse legitimate identical-looking trips.

Rerun semantics (tracked in raw.ingest_manifest):
  * same file name + same sha256 + completed  -> skipped (no-op)
  * same file name + different sha256         -> that file's rows are replaced atomically
  * failure                                   -> transaction rolled back, manifest marked failed
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import duckdb

TIMESTAMP = "timestamp"
NUMERIC = "numeric"
TEXT = "text"

# column -> (logical type, required, target SQL type)
COLUMNS: dict[str, tuple[str, bool, str]] = {
    "VendorID": (NUMERIC, True, "INTEGER"),
    "tpep_pickup_datetime": (TIMESTAMP, True, "TIMESTAMP"),
    "tpep_dropoff_datetime": (TIMESTAMP, True, "TIMESTAMP"),
    "passenger_count": (NUMERIC, False, "INTEGER"),
    "trip_distance": (NUMERIC, True, "DOUBLE"),
    "RatecodeID": (NUMERIC, False, "INTEGER"),
    "store_and_fwd_flag": (TEXT, False, "VARCHAR"),
    "PULocationID": (NUMERIC, True, "INTEGER"),
    "DOLocationID": (NUMERIC, True, "INTEGER"),
    "payment_type": (NUMERIC, True, "INTEGER"),
    "fare_amount": (NUMERIC, True, "DOUBLE"),
    "extra": (NUMERIC, False, "DOUBLE"),
    "mta_tax": (NUMERIC, False, "DOUBLE"),
    "tip_amount": (NUMERIC, True, "DOUBLE"),
    "tolls_amount": (NUMERIC, False, "DOUBLE"),
    "improvement_surcharge": (NUMERIC, False, "DOUBLE"),
    "total_amount": (NUMERIC, True, "DOUBLE"),
    "congestion_surcharge": (NUMERIC, False, "DOUBLE"),
    "Airport_fee": (NUMERIC, False, "DOUBLE"),
}

_NUMERIC_TYPES = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
)


class SchemaError(ValueError):
    pass


@dataclass
class IngestResult:
    file: str
    status: str  # loaded | replaced | skipped
    rows: int


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def _q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    cols = ",\n".join(f'    "{c}" {t}' for c, (_, _, t) in COLUMNS.items())
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS raw.yellow_trips (
{cols},
    source_file VARCHAR NOT NULL,
    source_row BIGINT NOT NULL,
    loaded_at TIMESTAMP NOT NULL
)"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS raw.ingest_manifest (
    file_name VARCHAR PRIMARY KEY,
    sha256 VARCHAR,
    status VARCHAR,
    row_count BIGINT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    error VARCHAR
)"""
    )


def validate_schema(con: duckdb.DuckDBPyConnection, path: Path) -> dict[str, str]:
    """Return {canonical column: actual column name}; raise SchemaError on incompatibility."""
    described = con.execute(f"DESCRIBE SELECT * FROM read_parquet({_q(str(path))})").fetchall()
    actual = {name.lower(): (name, typ.upper()) for name, typ, *_ in described}
    mapping, problems = {}, []
    for col, (logical, required, _) in COLUMNS.items():
        hit = actual.get(col.lower())
        if hit is None:
            if required:
                problems.append(f"missing required column {col}")
            continue
        name, typ = hit
        ok = (
            (logical == TIMESTAMP and typ.startswith("TIMESTAMP"))
            or (logical == NUMERIC and typ.startswith(_NUMERIC_TYPES))
            or (logical == TEXT and typ in ("VARCHAR", "BLOB"))
        )
        if not ok:
            problems.append(f"{col}: expected {logical}, got {typ}")
        mapping[col] = name
    if problems:
        raise SchemaError(f"{path.name}: " + "; ".join(problems))
    return mapping


def ingest_file(con: duckdb.DuckDBPyConnection, path: Path) -> IngestResult:
    path = Path(path)
    ensure_schema(con)
    sha = _sha256(path)
    prev = con.execute(
        "SELECT sha256, status FROM raw.ingest_manifest WHERE file_name = ?", [path.name]
    ).fetchone()
    if prev and prev[0] == sha and prev[1] == "completed":
        return IngestResult(path.name, "skipped", 0)

    try:
        mapping = validate_schema(con, path)
        select = ",\n".join(
            f'CAST("{mapping[c]}" AS {t}) AS "{c}"'
            if c in mapping
            else f'CAST(NULL AS {t}) AS "{c}"'
            for c, (_, _, t) in COLUMNS.items()
        )
        con.execute("BEGIN TRANSACTION")
        con.execute("DELETE FROM raw.yellow_trips WHERE source_file = ?", [path.name])
        con.execute(
            f"""INSERT INTO raw.yellow_trips
SELECT {select}, {_q(path.name)}, file_row_number, now()
FROM read_parquet({_q(str(path))}, file_row_number = true)"""
        )
        loaded = con.execute(
            "SELECT count(*) FROM raw.yellow_trips WHERE source_file = ?", [path.name]
        ).fetchone()[0]
        expected = con.execute(f"SELECT count(*) FROM read_parquet({_q(str(path))})").fetchone()[0]
        if loaded != expected:  # reconciliation check
            raise RuntimeError(f"row count mismatch: loaded {loaded}, file has {expected}")
        con.execute(
            """INSERT OR REPLACE INTO raw.ingest_manifest
VALUES (?, ?, 'completed', ?, now(), now(), NULL)""",
            [path.name, sha, loaded],
        )
        con.execute("COMMIT")
    except Exception as exc:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass  # no open transaction (e.g. failed during validation)
        con.execute(
            """INSERT OR REPLACE INTO raw.ingest_manifest
VALUES (?, ?, 'failed', NULL, now(), now(), ?)""",
            [path.name, sha, str(exc)[:500]],
        )
        raise
    return IngestResult(path.name, "replaced" if prev else "loaded", loaded)


def ingest_paths(db_path: Path, paths: list[Path]) -> list[IngestResult]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        return [ingest_file(con, p) for p in sorted(paths)]
