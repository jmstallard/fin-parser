"""
tests/test_edgar.py
Unit tests for EDGAR ingestion helpers.
Network calls are mocked — no live HTTP in CI.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from fin_parser.ingestion.edgar import _quarter, get_filings, ticker_to_cik


# ── _quarter ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("date,expected", [
    ("2023-01-15", "QTR1"),
    ("2023-04-01", "QTR2"),
    ("2023-07-30", "QTR3"),
    ("2023-10-31", "QTR4"),
    ("2023-12-31", "QTR4"),
])
def test_quarter(date: str, expected: str) -> None:
    assert _quarter(date) == expected


# ── ticker_to_cik ──────────────────────────────────────────────────────────

FAKE_TICKERS = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp"},
}


@patch("fin_parser.ingestion.edgar._get", return_value=FAKE_TICKERS)
def test_ticker_to_cik_found(mock_get: MagicMock) -> None:
    cik = ticker_to_cik("AAPL")
    assert cik == "0000320193"


@patch("fin_parser.ingestion.edgar._get", return_value=FAKE_TICKERS)
def test_ticker_to_cik_case_insensitive(mock_get: MagicMock) -> None:
    cik = ticker_to_cik("msft")
    assert cik == "0000789019"


@patch("fin_parser.ingestion.edgar._get", return_value=FAKE_TICKERS)
def test_ticker_to_cik_not_found(mock_get: MagicMock) -> None:
    with pytest.raises(ValueError, match="not found"):
        ticker_to_cik("ZZZZ")


# ── get_filings ────────────────────────────────────────────────────────────

FAKE_SUBMISSIONS = {
    "name": "Apple Inc.",
    "filings": {
        "recent": {
            "form":            ["10-K", "10-Q", "10-K", "8-K"],
            "accessionNumber": ["0000320193-23-000077", "0000320193-23-000066",
                                "0000320193-22-000108", "0000320193-23-000050"],
            "filingDate":      ["2023-11-03", "2023-08-04", "2022-10-28", "2023-06-01"],
            "reportDate":      ["2023-09-30", "2023-07-01", "2022-09-24", ""],
            # get_filings now zips primaryDocument into each result. Without
            # it, zip() stops at the shortest iterable ([]) and yields zero
            # rows, which is what was causing the IndexError on filings[0].
            "primaryDocument": ["aapl-20230930.htm", "aapl-20230701.htm",
                                "aapl-20220924.htm", "aapl-8k-20230601.htm"],
        }
    }
}


@patch("fin_parser.ingestion.edgar._get", return_value=FAKE_SUBMISSIONS)
def test_get_filings_filters_by_form(mock_get: MagicMock) -> None:
    filings = get_filings("0000320193", form_type="10-K", limit=5)
    assert len(filings) == 2
    assert all(f["form_type"] == "10-K" for f in filings)


@patch("fin_parser.ingestion.edgar._get", return_value=FAKE_SUBMISSIONS)
def test_get_filings_respects_limit(mock_get: MagicMock) -> None:
    filings = get_filings("0000320193", form_type="10-K", limit=1)
    assert len(filings) == 1


@patch("fin_parser.ingestion.edgar._get", return_value=FAKE_SUBMISSIONS)
def test_get_filings_fields(mock_get: MagicMock) -> None:
    filings = get_filings("0000320193", form_type="10-K", limit=1)
    f = filings[0]
    assert f["company"] == "Apple Inc."
    assert f["period"] == "2023-09-30"
    assert f["accession"] == "0000320193-23-000077"
    assert "accession_clean" in f
