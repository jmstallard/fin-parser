"""
fin_parser/ingestion/repository.py
Persistence layer for filings and metrics.
"""
from __future__ import annotations

from typing import Any

from fin_parser.db import get_connection, transaction


def save_filing(filing: dict[str, Any], raw_path: str | None = None) -> int:
    """Insert or update a filing. Returns the row id."""
    with transaction() as conn:
        cur = conn.execute(
            """
            INSERT INTO filings (cik, company, form_type, period, filed_date, accession, raw_path)
            VALUES (:cik, :company, :form_type, :period, :filed_date, :accession, :raw_path)
            ON CONFLICT(accession) DO UPDATE SET
                raw_path = COALESCE(:raw_path, raw_path)
            RETURNING id
            """,
            {
                "cik":        filing["cik"],
                "company":    filing["company"],
                "form_type":  filing["form_type"],
                "period":     filing["period"],
                "filed_date": filing["filed_date"],
                "accession":  filing["accession"],
                "raw_path":   raw_path,
            },
        )
        row = cur.fetchone()
        return row["id"]


def save_metrics(filing_id: int, metrics: dict[str, Any], period: str) -> None:
    """Insert extracted metrics for a filing."""
    with transaction() as conn:
        for metric, value in metrics.items():
            if value is None:
                continue
            conn.execute(
                """
                INSERT INTO metrics (filing_id, metric, value, unit, period)
                VALUES (?, ?, ?, ?, ?)
                """,
                (filing_id, metric, float(value), "USD_millions", period),
            )


def get_metrics(filing_id: int) -> list[dict[str, Any]]:
    """Return all metrics for a filing."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT metric, value, unit FROM metrics WHERE filing_id = ?",
        (filing_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_filing_by_accession(accession: str) -> dict[str, Any] | None:
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM filings WHERE accession = ?", (accession,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None
