"""
fin_parser/ingestion/edgar.py
Fetch filing metadata and documents from the SEC EDGAR REST API.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import requests

from fin_parser.config import (
    EDGAR_BASE_URL,
    EDGAR_RATE_LIMIT_DELAY,
    EDGAR_USER_AGENT,
    RAW_DIR,
)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip"})
    return s


SESSION = _session()


def _get(url: str) -> dict[str, Any]:
    """GET JSON from EDGAR, respecting rate limit."""
    time.sleep(EDGAR_RATE_LIMIT_DELAY)
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()  # type: ignore[no-any-return]


def ticker_to_cik(ticker: str) -> str:
    """Resolve a ticker symbol to a zero-padded 10-digit CIK."""
    data = _get("https://www.sec.gov/files/company_tickers.json")
    ticker_upper = ticker.upper()
    for entry in data.values():
        if entry.get("ticker", "").upper() == ticker_upper:
            return str(entry["cik_str"]).zfill(10)
    raise ValueError(f"Ticker '{ticker}' not found in EDGAR.")


def get_filings(
    cik: str,
    form_type: str = "10-K",
    limit: int = 5,
) -> list[dict[str, Any]]:
    """
    Return the most recent `limit` filings of `form_type` for a given CIK.
    Includes the primary document filename from the submissions API.
    """
    cik_padded = cik.zfill(10)
    data = _get(f"{EDGAR_BASE_URL}/submissions/CIK{cik_padded}.json")

    company_name: str = data.get("name", "Unknown")
    recent = data.get("filings", {}).get("recent", {})

    forms         = recent.get("form", [])
    accessions    = recent.get("accessionNumber", [])
    dates         = recent.get("filingDate", [])
    periods       = recent.get("reportDate", [])
    primary_docs  = recent.get("primaryDocument", [])

    results = []
    for form, accession, date, period, primary_doc in zip(
        forms, accessions, dates, periods, primary_docs
    ):
        if form == form_type:
            acc_clean = accession.replace("-", "")
            results.append({
                "cik": cik_padded,
                "company": company_name,
                "form_type": form,
                "accession": accession,
                "accession_clean": acc_clean,
                "filed_date": date,
                "period": period,
                "primary_doc": primary_doc,  # e.g. "aapl-20250927.htm"
            })
            if len(results) >= limit:
                break

    return results


def _quarter(date_str: str) -> str:
    month = int(date_str[5:7])
    return f"QTR{(month - 1) // 3 + 1}"


def download_filing(filing: dict[str, Any]) -> Path:
    """Download the primary document for a filing to RAW_DIR."""
    cik = filing["cik"]
    acc_clean = filing["accession_clean"]
    primary_doc = filing.get("primary_doc", "")

    if not primary_doc:
        raise RuntimeError(f"No primary document found for accession {filing['accession']}")

    cik_int = int(cik)
    doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{primary_doc}"

    # Use the original filename for local storage
    suffix = Path(primary_doc).suffix or ".htm"
    local_path = RAW_DIR / f"{cik}_{acc_clean}{suffix}"

    if local_path.exists():
        print(f"  [cache hit] {local_path.name}")
        return local_path

    print(f"  Downloading {doc_url}")
    time.sleep(EDGAR_RATE_LIMIT_DELAY)
    resp = SESSION.get(doc_url, timeout=60)
    resp.raise_for_status()

    local_path.write_bytes(resp.content)
    print(f"  Saved -> {local_path}")
    return local_path
