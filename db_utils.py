"""
SQLite helpers for storing worker partial results and computing final aggregates.
Switching to PostgreSQL/MySQL: replace sqlite3 code with appropriate connectors & SQL.
"""

import sqlite3
from contextlib import contextmanager
from typing import Optional, Tuple, Dict, Any
from datetime import datetime

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS worker_results (
    worker_id      TEXT NOT NULL,
    chunk_id       INTEGER NOT NULL,
    rows_processed INTEGER NOT NULL,
    total_sales    REAL NOT NULL,
    min_price      REAL NOT NULL,
    max_price      REAL NOT NULL,
    avg_price      REAL NOT NULL,
    inserted_at    TEXT NOT NULL,
    PRIMARY KEY (worker_id, chunk_id)
);
"""

UPSERT_SQL = """
INSERT INTO worker_results (worker_id, chunk_id, rows_processed, total_sales, min_price, max_price, avg_price, inserted_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(worker_id, chunk_id) DO UPDATE SET
    rows_processed=excluded.rows_processed,
    total_sales=excluded.total_sales,
    min_price=excluded.min_price,
    max_price=excluded.max_price,
    avg_price=excluded.avg_price,
    inserted_at=excluded.inserted_at;
"""

AGGREGATE_SQL = """
SELECT
    SUM(rows_processed) AS total_rows,
    SUM(total_sales)    AS total_sales,
    MIN(min_price)      AS min_price,
    MAX(max_price)      AS max_price,
    -- Weighted average price by rows (approximation): average of chunk means weighted by rows
    CASE WHEN SUM(rows_processed) > 0
         THEN SUM(avg_price * rows_processed) / SUM(rows_processed)
         ELSE NULL
    END AS avg_price
FROM worker_results;
"""

@contextmanager
def connect(db_path: str):
    conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)  # autocommit
    try:
        yield conn
    finally:
        conn.close()

def init_db(db_path: str) -> None:
    with connect(db_path) as conn:
        cur = conn.cursor()
        for stmt in SCHEMA_SQL.strip().split(";"):
            if stmt.strip():
                cur.execute(stmt)
        cur.close()

def upsert_partial(db_path: str, record: Dict[str, Any]) -> None:
    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            UPSERT_SQL,
            (
                record["worker_id"],
                record["chunk_id"],
                record["rows_processed"],
                record["total_sales"],
                record["min_price"],
                record["max_price"],
                record["avg_price"],
                record.get("inserted_at") or datetime.utcnow().isoformat()
            )
        )
        cur.close()

def final_aggregate(db_path: str) -> Dict[str, Any]:
    with connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(AGGREGATE_SQL)
        row = cur.fetchone()
        cur.close()
    keys = ["total_rows", "total_sales", "min_price", "max_price", "avg_price"]
    return dict(zip(keys, row)) if row else {k: None for k in keys}
