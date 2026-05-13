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
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    cik           TEXT    NOT NULL,
    ticker        TEXT,
    company       TEXT    NOT NULL,
    form_type     TEXT    NOT NULL,   -- '10-K', '10-Q', 'NI 43-101', 'PEA', 'FS', etc.
    period        TEXT    NOT NULL,   -- '2023-12-31'
    filed_date    TEXT    NOT NULL,
    accession     TEXT    NOT NULL UNIQUE,
    raw_path      TEXT,               -- local path to downloaded file
    jurisdiction  TEXT    NOT NULL DEFAULT 'US',    -- 'US' | 'CA'
    source        TEXT    NOT NULL DEFAULT 'EDGAR', -- 'EDGAR' | 'SEDAR+' | 'UPLOAD'
    -- Optional asset/mine name. Set only for per-project NI 43-101 / PEA /
    -- PFS / FS reports so a single issuer can carry multiple technical
    -- reports (e.g. Agnico Eagle's 'Detour Lake' + 'Hope Bay') without
    -- collapsing in the dashboard. NULL for corporate filings.
    project       TEXT,
    fetched_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);
-- NOTE: indexes on jurisdiction/source are created in _apply_migrations so
-- they can safely run after the columns are added to pre-existing DBs.
CREATE INDEX IF NOT EXISTS idx_filings_form_type ON filings(form_type);

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

-- Mining-specific technical metrics from NI 43-101 / PEA / PFS / FS reports.
-- Kept in a separate table because units vary widely (t, Mt, g/t, USD/oz, years, %).
CREATE TABLE IF NOT EXISTS mining_metrics (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id    INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
    metric       TEXT    NOT NULL,   -- 'npv_after_tax', 'irr', 'aisc', 'mine_life', ...
    value        REAL,
    unit         TEXT,               -- 'USD_M', 'percent', 'years', 'g_per_tonne', 'Mt', 'USD_per_oz', ...
    commodity    TEXT,               -- 'gold', 'copper', 'silver', 'lithium', ...
    category     TEXT,               -- 'economics', 'reserves', 'resources', 'operating', 'geology'
    period       TEXT    NOT NULL,
    extracted_at TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_mining_metrics_filing ON mining_metrics(filing_id);
CREATE INDEX IF NOT EXISTS idx_mining_metrics_metric ON mining_metrics(metric);

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
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id    INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
    method       TEXT    NOT NULL,   -- 'dcf', 'wacc', 'irr'
    result       REAL,               -- headline number (intrinsic value/share)
    inputs_json  TEXT,                -- JSON blob of inputs used
    outputs_json TEXT,                -- JSON blob of computed outputs
                                      -- (WACC components, EV, PV, IRR, P/E, sensitivity)
    computed_at  TEXT    NOT NULL DEFAULT (datetime('now'))
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


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """
    Lightweight migrations for pre-existing databases that were created before
    later columns were added. Each block is idempotent.

    IMPORTANT: runs AFTER the main DDL so CREATE TABLE IF NOT EXISTS won't
    trip on missing columns. Indexes that depend on added columns live here
    (not in DDL) so they can't fire before their column exists.
    """
    existing_cols = {row["name"] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "jurisdiction" not in existing_cols:
        conn.execute("ALTER TABLE filings ADD COLUMN jurisdiction TEXT NOT NULL DEFAULT 'US'")
    if "source" not in existing_cols:
        conn.execute("ALTER TABLE filings ADD COLUMN source TEXT NOT NULL DEFAULT 'EDGAR'")
    if "project" not in existing_cols:
        # Nullable — existing rows stay NULL, corporate filings stay NULL,
        # only per-project technical reports get a value.
        conn.execute("ALTER TABLE filings ADD COLUMN project TEXT")
    # Indexes on potentially-just-added columns
    conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_jurisdiction ON filings(jurisdiction)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_source       ON filings(source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_project      ON filings(project)")

    # valuations.outputs_json — stores the full ValuationResult (WACC
    # components, DCF figures, IRR, P/E, sensitivity table) so the
    # dashboard can render them without re-running the engine. Older
    # rows leave it NULL and the dashboard falls back to the bare
    # intrinsic-value-per-share number.
    val_cols = {row["name"] for row in conn.execute("PRAGMA table_info(valuations)").fetchall()}
    if "outputs_json" not in val_cols:
        conn.execute("ALTER TABLE valuations ADD COLUMN outputs_json TEXT")


def init_db(db_path: Path = DB_PATH) -> None:
    """Create all tables. Safe to call multiple times (IF NOT EXISTS)."""
    with transaction(db_path) as conn:
        conn.executescript(DDL)
        _apply_migrations(conn)
    print(f"Database ready: {db_path}")


if __name__ == "__main__":
    init_db()
