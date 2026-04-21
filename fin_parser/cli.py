"""
fin_parser/cli.py
CLI entry point.

Commands:
  fin-parser fetch AAPL --form 10-K --limit 3
  fin-parser download AAPL --form 10-K --limit 1
  fin-parser extract AAPL --form 10-K --limit 1
"""
from __future__ import annotations

import argparse
import sys


def cmd_fetch(args: argparse.Namespace) -> None:
    from fin_parser.ingestion.edgar import get_filings, ticker_to_cik
    from fin_parser.ingestion.repository import save_filing

    print(f"Resolving ticker '{args.ticker}' -> CIK...")
    cik = ticker_to_cik(args.ticker)
    print(f"CIK: {cik}")

    print(f"Fetching last {args.limit} {args.form} filings...")
    filings = get_filings(cik, form_type=args.form, limit=args.limit)

    for f in filings:
        filing_id = save_filing(f)
        print(f"  [{filing_id}] {f['filed_date']}  {f['form_type']}  period={f['period']}  accession={f['accession']}")

    print(f"Saved {len(filings)} filings to DB.")


def cmd_download(args: argparse.Namespace) -> None:
    from fin_parser.ingestion.edgar import download_filing, get_filings, ticker_to_cik
    from fin_parser.ingestion.repository import save_filing

    print(f"Resolving ticker '{args.ticker}' -> CIK...")
    cik = ticker_to_cik(args.ticker)

    print(f"Fetching last {args.limit} {args.form} filings...")
    filings = get_filings(cik, form_type=args.form, limit=args.limit)

    for f in filings:
        print(f"\nDownloading {f['form_type']} for period {f['period']}...")
        try:
            local_path = download_filing(f)
            filing_id = save_filing(f, raw_path=str(local_path))
            print(f"  Saved to DB [id={filing_id}] -> {local_path}")
        except Exception as e:
            print(f"  ERROR: {e}")

    print("\nDone.")


def cmd_extract(args: argparse.Namespace) -> None:
    from pathlib import Path

    from fin_parser.extraction.claude_extractor import extract_metrics
    from fin_parser.ingestion.edgar import download_filing, get_filings, ticker_to_cik
    from fin_parser.ingestion.repository import save_filing, save_metrics

    print(f"Resolving ticker '{args.ticker}' -> CIK...")
    cik = ticker_to_cik(args.ticker)

    print(f"Fetching last {args.limit} {args.form} filings...")
    filings = get_filings(cik, form_type=args.form, limit=args.limit)

    for f in filings:
        print(f"\nProcessing {f['form_type']} for period {f['period']}...")

        # Download if not already local
        local_path = download_filing(f)
        filing_id = save_filing(f, raw_path=str(local_path))

        # Extract metrics via Claude
        metrics = extract_metrics(Path(local_path), period=f["period"])

        # Save to DB
        save_metrics(filing_id, metrics, period=f["period"])

        # Print results
        print(f"\n  Extracted metrics for {f['company']} {f['period']}:")
        for metric, value in metrics.items():
            status = f"{value:>12,.2f}" if value is not None else "        None"
            print(f"    {metric:<25} {status}")

    print("\nDone.")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="fin-parser",
        description="AI-powered financial filing parser",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    fetch_p = sub.add_parser("fetch", help="Fetch and save filing metadata from EDGAR")
    fetch_p.add_argument("ticker")
    fetch_p.add_argument("--form", default="10-K")
    fetch_p.add_argument("--limit", type=int, default=3)
    fetch_p.set_defaults(func=cmd_fetch)

    download_p = sub.add_parser("download", help="Download filing documents to data/raw/")
    download_p.add_argument("ticker")
    download_p.add_argument("--form", default="10-K")
    download_p.add_argument("--limit", type=int, default=1)
    download_p.set_defaults(func=cmd_download)

    extract_p = sub.add_parser("extract", help="Extract metrics from filings using Claude")
    extract_p.add_argument("ticker")
    extract_p.add_argument("--form", default="10-K")
    extract_p.add_argument("--limit", type=int, default=1)
    extract_p.set_defaults(func=cmd_extract)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
