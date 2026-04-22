"""
fin_parser/ingestion/sedar_plus.py

Ingestion for Canadian filings sourced from SEDAR+.

SEDAR+ has no free public API equivalent to SEC EDGAR, so the ingestion
model here is deliberately simple: the user downloads the PDF from
https://www.sedarplus.ca (or the issuer's IR site) and hands the local
file path to `register_uploaded_filing`. We copy it into RAW_DIR under a
deterministic name and register it in the `filings` table with
jurisdiction='CA' and source='SEDAR+'.

This mirrors the shape of filings produced by `edgar.get_filings` so the
rest of the pipeline (extraction, repository, dashboard) can treat
Canadian filings uniformly.
"""
from __future__ import annotations

import hashlib
import re
import shutil
from datetime import date
from pathlib import Path
from typing import Any

from fin_parser.config import RAW_DIR

# Canonical set of Canadian filing/report types we recognise. The
# dashboard and extractor use this to decide whether to apply the
# mining-specific extraction prompt.
CA_FORM_TYPES: set[str] = {
    # Mining technical reports (NI 43-101)
    "NI 43-101",
    "Technical Report",
    "PEA",
    "PFS",
    "FS",
    "DFS",
    # Continuous disclosure (non-technical, included for completeness)
    "Annual Report",
    "AIF",      # Annual Information Form
    "MD&A",
    "Financial Statements",
    "Interim MD&A",
    "Interim Financial Statements",
}

# Report types whose body is a mining technical report and therefore
# warrants the mining extractor + mining metrics table.
MINING_REPORT_TYPES: set[str] = {
    "NI 43-101",
    "Technical Report",
    "PEA",
    "PFS",
    "FS",
    "DFS",
}


def is_mining_report(form_type: str) -> bool:
    """True if the given form_type should be routed through mining extraction."""
    return form_type.strip().upper().replace("-", " ") in {
        t.upper().replace("-", " ") for t in MINING_REPORT_TYPES
    }


def _synthetic_accession(
    ticker: str,
    form_type: str,
    period: str,
    pdf_path: Path,
    project: str | None = None,
) -> str:
    """
    Build a stable unique accession for a manually-uploaded filing.

    EDGAR uses 18-char accessions like '0000320193-24-000123'. We need a
    UNIQUE key that's equally stable so re-running the same upload
    doesn't create duplicate rows. We derive it from ticker + form_type
    + optional project + period + content hash, prefixed 'SEDAR-'.

    Including `project` in the human-readable portion makes it obvious at
    a glance which mine a 43-101 belongs to when listing raw files; the
    content hash still enforces uniqueness either way.
    """
    h = hashlib.sha256()
    h.update(pdf_path.read_bytes() if pdf_path.exists() else b"")
    short_hash = h.hexdigest()[:12]
    safe_form = form_type.replace(" ", "_").replace("/", "-")
    if project:
        safe_project = re.sub(r"[^A-Za-z0-9]+", "_", project).strip("_")
        return f"SEDAR-{ticker.upper()}-{safe_form}-{safe_project}-{period}-{short_hash}"
    return f"SEDAR-{ticker.upper()}-{safe_form}-{period}-{short_hash}"


def register_uploaded_filing(
    pdf_path: Path,
    ticker: str,
    company: str,
    form_type: str,
    period: str,
    filed_date: str | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    """
    Copy `pdf_path` into RAW_DIR and return a filing dict in the same
    shape produced by `edgar.get_filings`, plus `jurisdiction='CA'` and
    `source='SEDAR+'`. Does NOT insert into the DB; pair with
    `repository.save_filing(filing, raw_path=...)`.

    Parameters
    ----------
    pdf_path   : path to the PDF on disk (must exist)
    ticker     : issuer ticker (e.g. 'AEM.TO', 'ABX', 'FNV')
    company    : issuer legal name
    form_type  : one of CA_FORM_TYPES (e.g. 'NI 43-101', 'PEA', 'FS')
    period     : effective date of the report, 'YYYY-MM-DD'
    filed_date : SEDAR+ filing date, 'YYYY-MM-DD' (defaults to today)
    project    : optional mine/asset name for per-project technical reports.
                 Lets a single issuer carry multiple 43-101s without any
                 of them being hidden by dashboard dedup. Leave None for
                 corporate filings.
    """
    pdf_path = Path(pdf_path).expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    if not pdf_path.suffix.lower() == ".pdf":
        raise ValueError(f"Expected a .pdf file, got: {pdf_path.name}")

    form_type = form_type.strip()
    if form_type not in CA_FORM_TYPES:
        # Not a hard error — the user may be experimenting — but warn.
        print(
            f"[sedar_plus] warning: '{form_type}' is not a recognised Canadian "
            f"form type. Known types: {sorted(CA_FORM_TYPES)}"
        )

    # Normalise an empty-string project to None so the rest of the pipeline
    # only ever has to distinguish 'has project' vs 'no project'.
    if project is not None and not str(project).strip():
        project = None
    elif project is not None:
        project = str(project).strip()

    filed_date = filed_date or date.today().isoformat()
    accession = _synthetic_accession(ticker, form_type, period, pdf_path, project=project)

    # Store under RAW_DIR with a deterministic name so repeat uploads
    # don't explode the folder.
    dest = RAW_DIR / f"{accession}.pdf"
    if not dest.exists():
        shutil.copy2(pdf_path, dest)
        print(f"  Copied {pdf_path.name} -> {dest}")
    else:
        print(f"  [cache hit] {dest.name}")

    # For Canadian issuers we have no CIK — reuse the ticker slot so the
    # rest of the pipeline (which keys on `cik`) keeps working.
    pseudo_cik = f"CA-{ticker.upper()}"

    return {
        "cik":          pseudo_cik,
        "ticker":       ticker.upper(),
        "company":      company,
        "form_type":    form_type,
        "period":       period,
        "filed_date":   filed_date,
        "accession":    accession,
        "primary_doc":  dest.name,
        "raw_path":     str(dest),
        "jurisdiction": "CA",
        "source":       "SEDAR+",
        "project":      project,
    }
