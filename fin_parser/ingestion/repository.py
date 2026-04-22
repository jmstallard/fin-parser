"""
fin_parser/ingestion/repository.py
Persistence layer for filings and metrics.
"""
from __future__ import annotations

from typing import Any

from fin_parser.db import get_connection, transaction


def save_filing(
    filing: dict[str, Any],
    raw_path: str | None = None,
    jurisdiction: str | None = None,
    source: str | None = None,
    project: str | None = None,
) -> int:
    """
    Insert or update a filing. Returns the row id.

    `jurisdiction` is 'US' | 'CA'. `source` is 'EDGAR' | 'SEDAR+' | 'UPLOAD'.
    `project` is an optional asset/mine name for per-project NI 43-101s.
    If not explicitly passed, the values are taken from the `filing` dict
    (keys of the same name) or fall back to 'US' / 'EDGAR' / None for
    backward compatibility with the original EDGAR-only ingestion path.
    """
    jurisdiction = jurisdiction or filing.get("jurisdiction") or "US"
    source = source or filing.get("source") or "EDGAR"
    # project is nullable — treat an empty string the same as None so the
    # CLI can pass either with the same effect.
    project = project if project is not None else filing.get("project")
    if project is not None and not str(project).strip():
        project = None

    with transaction() as conn:
        cur = conn.execute(
            """
            INSERT INTO filings (
                cik, ticker, company, form_type, period, filed_date,
                accession, raw_path, jurisdiction, source, project
            )
            VALUES (
                :cik, :ticker, :company, :form_type, :period, :filed_date,
                :accession, :raw_path, :jurisdiction, :source, :project
            )
            ON CONFLICT(accession) DO UPDATE SET
                raw_path     = COALESCE(:raw_path, raw_path),
                jurisdiction = :jurisdiction,
                source       = :source,
                project      = COALESCE(:project, project)
            RETURNING id
            """,
            {
                "cik":          filing["cik"],
                "ticker":       filing.get("ticker"),
                "company":      filing["company"],
                "form_type":    filing["form_type"],
                "period":       filing["period"],
                "filed_date":   filing["filed_date"],
                "accession":    filing["accession"],
                "raw_path":     raw_path,
                "jurisdiction": jurisdiction,
                "source":       source,
                "project":      project,
            },
        )
        row = cur.fetchone()
        return row["id"]


def save_metrics(filing_id: int, metrics: dict[str, Any], period: str) -> None:
    """Insert extracted financial metrics for a filing.

    Behaves as an upsert at the (filing_id, period) level: existing rows
    for this filing+period are deleted first so that re-running extraction
    doesn't silently double the row count. This is what the user will expect
    — each extraction pass should replace prior output for that period, not
    append to it."""
    with transaction() as conn:
        conn.execute(
            "DELETE FROM metrics WHERE filing_id = ? AND period = ?",
            (filing_id, period),
        )
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


# ── Mining metrics ─────────────────────────────────────────────────────────

# Canonical unit for every mining metric we know about. The extractor returns
# values in these units so downstream display logic doesn't have to guess.
MINING_METRIC_UNITS: dict[str, tuple[str, str]] = {
    # (unit, category)
    "npv_after_tax":             ("USD_M",        "economics"),
    "npv_pre_tax":               ("USD_M",        "economics"),
    "irr_after_tax":             ("percent",      "economics"),
    "irr_pre_tax":               ("percent",      "economics"),
    "payback_period":            ("years",        "economics"),
    "discount_rate":             ("percent",      "economics"),
    "initial_capex":             ("USD_M",        "economics"),
    "sustaining_capex":          ("USD_M",        "economics"),
    "lom_revenue":               ("USD_M",        "economics"),
    "lom_free_cash_flow":        ("USD_M",        "economics"),
    "commodity_price_assumption": ("USD_per_unit", "economics"),
    "mine_life":                 ("years",        "operating"),
    "annual_production":         ("units_per_yr", "operating"),
    "aisc":                      ("USD_per_unit", "operating"),
    "cash_cost":                 ("USD_per_unit", "operating"),
    "opex_per_tonne":            ("USD_per_t",    "operating"),
    "strip_ratio":               ("ratio",        "operating"),
    "throughput":                ("t_per_day",    "operating"),
    "recovery_rate":             ("percent",      "operating"),
    "proven_reserves":           ("Mt",           "reserves"),
    "probable_reserves":         ("Mt",           "reserves"),
    "proven_probable_reserves":  ("Mt",           "reserves"),
    "measured_resources":        ("Mt",           "resources"),
    "indicated_resources":       ("Mt",           "resources"),
    "measured_indicated_resources": ("Mt",        "resources"),
    "inferred_resources":        ("Mt",           "resources"),
    "head_grade":                ("grade",        "geology"),
    "contained_metal":           ("contained",    "geology"),
    # Derived — sits in the 'resources' category so it renders near the
    # M&I and Inferred rows that it's computed from.
    "inferred_to_mi_ratio":      ("ratio",        "resources"),
}


def save_mining_metrics(
    filing_id: int,
    metrics: dict[str, Any],
    period: str,
    commodity: str | None = None,
) -> None:
    """
    Persist NI 43-101 / PEA / PFS / FS technical metrics.

    `metrics` is a dict where each key is a known mining metric name.
    Unit + category are looked up in MINING_METRIC_UNITS; unknown keys are
    stored with a generic unit/category so we never silently drop extracted
    data.
    """
    with transaction() as conn:
        # Same upsert-at-period semantics as save_metrics: re-running the
        # mining extractor on the same NI 43-101 should replace, not duplicate.
        conn.execute(
            "DELETE FROM mining_metrics WHERE filing_id = ? AND period = ?",
            (filing_id, period),
        )
        for metric, value in metrics.items():
            if value is None:
                continue
            unit, category = MINING_METRIC_UNITS.get(metric, ("unknown", "other"))
            conn.execute(
                """
                INSERT INTO mining_metrics
                    (filing_id, metric, value, unit, commodity, category, period)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (filing_id, metric, float(value), unit, commodity, category, period),
            )


def get_metrics(filing_id: int) -> list[dict[str, Any]]:
    """Return all (financial) metrics for a filing."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT metric, value, unit FROM metrics WHERE filing_id = ?",
        (filing_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_mining_metrics(filing_id: int) -> list[dict[str, Any]]:
    """Return all mining/technical metrics for a filing."""
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT metric, value, unit, commodity, category
        FROM mining_metrics
        WHERE filing_id = ?
        """,
        (filing_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_filings_by_jurisdiction(jurisdiction: str | None = None) -> list[dict[str, Any]]:
    """
    Return filings filtered by jurisdiction. Pass None (or 'ALL') for no filter.
    """
    conn = get_connection()
    if jurisdiction and jurisdiction.upper() != "ALL":
        rows = conn.execute(
            """
            SELECT id, cik, ticker, company, form_type, period, filed_date,
                   accession, raw_path, jurisdiction, source, project
            FROM filings
            WHERE jurisdiction = ?
            ORDER BY company, filed_date DESC
            """,
            (jurisdiction.upper(),),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, cik, ticker, company, form_type, period, filed_date,
                   accession, raw_path, jurisdiction, source, project
            FROM filings
            ORDER BY company, filed_date DESC
            """
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
