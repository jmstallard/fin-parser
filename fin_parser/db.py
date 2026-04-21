"""
fin_parser/db.py
SQLite schema and connection helper.
Designed to grow with the project — Phase 1 tables now, stubs for later.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from fin_parser.config import DB_PATH

# ── Schema ─────────────────────────────────────────────────────────────────

DDL = """
-- Phase 1: raw filing metadata
CREATE TABLE IF NOT EXISTS filings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    cik         TEXT    NOT NULL,
    ticker      TEXT,
    company     TEXT    NOT NULL,
    form_type   TEXT    NOT NULL,   -- '10-K', '10-Q', etc.
    period      TEXT    NOT NULL,   -- '2023-12-31'
    filed_date  TEXT    NOT NULL,
    accession   TEXT    NOT NULL UNIQUE,
    raw_path    TEXT,               -- local path to downloaded file
    fetched_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Phase 1: extracted financial metrics
CREATE TABLE IF NOT EXISTS metrics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id   INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
    metric      TEXT    NOT NULL,   -- 'revenue', 'eps', 'free_cash_flow', ...
    value       REAL,
    unit        TEXT,               -- 'USD', 'shares', 'ratio'
    period      TEXT    NOT NULL,
    extracted_at TEXT   NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_metrics_filing ON metrics(filing_id);
CREATE INDEX IF NOT EXISTS idx_metrics_metric  ON metrics(metric);

-- Phase 2: red flags surfaced by the analysis agent
CREATE TABLE IF NOT EXISTS red_flags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id   INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
    flag_type   TEXT    NOT NULL,   -- 'dilution', 'goodwill_writedown', ...
    severity    TEXT,               -- 'low', 'medium', 'high'
    detail      TEXT,
    flagged_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Phase 2: valuation snapshots
CREATE TABLE IF NOT EXISTS valuations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id   INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
    method      TEXT    NOT NULL,   -- 'dcf', 'wacc', 'irr'
    result      REAL,
    inputs_json TEXT,               -- JSON blob of inputs used
    computed_at TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""


# ── Connection helper ──────────────────────────────────────────────────────

def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")  # safe for concurrent reads
    return conn


@contextmanager
def transaction(db_path: Path = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    """Context manager: auto-commits on success, rolls back on exception."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH) -> None:
    """Create all tables. Safe to call multiple times (IF NOT EXISTS)."""
    with transaction(db_path) as conn:
        conn.executescript(DDL)
    print(f"Database ready: {db_path}")


if __name__ == "__main__":
    init_db()
