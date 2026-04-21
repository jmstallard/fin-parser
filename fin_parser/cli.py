"""
fin_parser/cli.py
CLI entry point.

Commands:
  fin-parser fetch AAPL --form 10-K --limit 3
  fin-parser download AAPL --form 10-K --limit 1
  fin-parser extract AAPL --form 10-K --limit 1
  fin-parser value AAPL --market-cap 3200000
  fin-parser redflag AAPL
"""
from __future__ import annotations

import argparse
import json
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
        print(f"  [{filing_id}] {f['filed_date']}  {f['form_type']}  period={f['period']}")
    print(f"Saved {len(filings)} filings to DB.")


def cmd_download(args: argparse.Namespace) -> None:
    from fin_parser.ingestion.edgar import download_filing, get_filings, ticker_to_cik
    from fin_parser.ingestion.repository import save_filing

    print(f"Resolving ticker '{args.ticker}' -> CIK...")
    cik = ticker_to_cik(args.ticker)
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
    filings = get_filings(cik, form_type=args.form, limit=args.limit)
    for f in filings:
        print(f"\nProcessing {f['form_type']} for period {f['period']}...")
        local_path = download_filing(f)
        filing_id = save_filing(f, raw_path=str(local_path))
        metrics = extract_metrics(Path(local_path), period=f["period"])
        save_metrics(filing_id, metrics, period=f["period"])
        print(f"\n  Extracted metrics for {f['company']} {f['period']}:")
        for metric, value in metrics.items():
            status = f"{value:>12,.2f}" if isinstance(value, float) else f"{'None':>12}"
            print(f"    {metric:<25} {status}")
    print("\nDone.")


def cmd_value(args: argparse.Namespace) -> None:
    from fin_parser.ingestion.repository import get_filing_by_accession, get_metrics
    from fin_parser.valuation.engine import DCFInputs, WACCInputs, run_valuation
    from fin_parser.db import get_connection

    # Load most recent filing metrics from DB
    row = _get_latest_filing_for_ticker(args.ticker)

    if not row:
        print(f"No filings found for '{args.ticker}' in DB. Run 'fin-parser extract {args.ticker}' first.")
        return

    filing_id, company, period = row["id"], row["company"], row["period"]
    raw_metrics = get_metrics(filing_id)

    metrics = {r["metric"]: r["value"] for r in raw_metrics}
    print(f"\nValuing {company} ({period})")
    print(f"Using market cap: ${args.market_cap:,.0f}M\n")

    wacc_inputs = WACCInputs(
        risk_free_rate=args.risk_free,
        equity_risk_premium=args.erp,
        beta=args.beta,
        total_debt=metrics.get("total_debt") or 0,
        interest_expense=args.interest_expense,
        tax_rate=args.tax_rate,
        market_cap=args.market_cap,
    )

    dcf_inputs = DCFInputs(
        free_cash_flow=metrics.get("free_cash_flow") or metrics.get("operating_cash_flow") or 0,
        revenue=metrics.get("revenue") or 0,
        growth_rate_stage1=args.growth1,
        growth_rate_stage2=args.growth2,
        terminal_growth_rate=args.terminal_growth,
        wacc=0,  # computed by engine
        shares_outstanding=metrics.get("shares_outstanding") or 1,
    )

    result = run_valuation(
        wacc_inputs=wacc_inputs,
        dcf_inputs=dcf_inputs,
        net_income=metrics.get("net_income"),
        market_cap=args.market_cap,
    )

    print("=" * 50)
    print("WACC COMPONENTS")
    print("=" * 50)
    print(f"  Cost of Equity:        {result.cost_of_equity:.2%}")
    print(f"  Cost of Debt (AT):     {result.cost_of_debt:.2%}")
    print(f"  Equity Weight:         {result.equity_weight:.2%}")
    print(f"  Debt Weight:           {result.debt_weight:.2%}")
    print(f"  WACC:                  {result.wacc:.2%}")

    print("\n" + "=" * 50)
    print("DCF VALUATION")
    print("=" * 50)
    print(f"  PV of FCFs:            ${result.pv_of_fcfs:>12,.0f}M")
    print(f"  Terminal Value (PV):   ${result.terminal_value:>12,.0f}M")
    print(f"  Enterprise Value:      ${result.enterprise_value:>12,.0f}M")
    print(f"  Intrinsic Value/Share: ${result.intrinsic_value_per_share:>12,.2f}")
    if result.irr:
        print(f"  IRR:                   {result.irr:.2%}")
    if result.pe_ratio:
        print(f"  P/E Ratio:             {result.pe_ratio:.1f}x")

    print("\n" + "=" * 50)
    print("SENSITIVITY TABLE (Intrinsic Value per Share)")
    print(f"Rows: Stage 1 Growth | Cols: WACC")
    print("=" * 50)
    wacc_keys = sorted(next(iter(result.sensitivity.values())).keys())
    header = f"{'Growth':>8} | " + " | ".join(f"{w:.1%}" for w in wacc_keys)
    print(header)
    print("-" * len(header))
    for g, row_data in sorted(result.sensitivity.items()):
        vals = " | ".join(
            f"${row_data[w]:>7,.0f}" if not (isinstance(row_data[w], float) and row_data[w] != row_data[w])
            else "    N/A"
            for w in wacc_keys
        )
        print(f"{g:>8.1%} | {vals}")

    # Save to DB
    from fin_parser.db import transaction
    import json as _json
    with transaction() as conn:
        conn.execute(
            "INSERT INTO valuations (filing_id, method, result, inputs_json) VALUES (?, ?, ?, ?)",
            (filing_id, "dcf_wacc", result.intrinsic_value_per_share,
             _json.dumps(result.inputs_summary)),
        )
    print(f"\nValuation saved to DB.")


def _get_latest_filing_for_ticker(ticker: str):
    """Look up the most recent filing for a ticker by matching company name or CIK."""
    from fin_parser.db import get_connection
    from fin_parser.ingestion.edgar import ticker_to_cik
    conn = get_connection()
    # Try by CIK first (most reliable)
    try:
        cik = ticker_to_cik(ticker)
        row = conn.execute(
            "SELECT id, company, period FROM filings WHERE cik = ? ORDER BY filed_date DESC LIMIT 1",
            (cik,)
        ).fetchone()
        if row:
            conn.close()
            return row
    except Exception:
        pass
    # Fallback: match company name
    row = conn.execute(
        "SELECT id, company, period FROM filings WHERE company LIKE ? ORDER BY filed_date DESC LIMIT 1",
        (f"%{ticker.upper()}%",)
    ).fetchone()
    conn.close()
    return row


def cmd_redflag(args: argparse.Namespace) -> None:
    from fin_parser.analysis.red_flags import analyze_red_flags
    from fin_parser.ingestion.repository import get_metrics

    row = _get_latest_filing_for_ticker(args.ticker)
    conn = None

    if not row:
        print("No filings found in DB. Run 'fin-parser extract' first.")
        return

    filing_id, company, period = row["id"], row["company"], row["period"]
    raw_metrics = get_metrics(filing_id)
    metrics = {r["metric"]: r["value"] for r in raw_metrics}

    print(f"\nAnalyzing red flags for {company} ({period})...\n")
    flags = analyze_red_flags(metrics, company, period)

    if not flags:
        print("No red flags identified.")
        return

    severity_order = {"high": 0, "medium": 1, "low": 2}
    flags.sort(key=lambda f: severity_order.get(f.get("severity", "low"), 3))

    icons = {"high": "🔴", "medium": "🟡", "low": "🟢"}
    for flag in flags:
        sev = flag.get("severity", "low")
        print(f"{icons.get(sev, '⚪')} [{sev.upper()}] {flag.get('flag_type', 'unknown')}")
        print(f"   {flag.get('detail', '')}\n")

    # Save to DB
    from fin_parser.db import transaction
    with transaction() as conn:
        for flag in flags:
            conn.execute(
                "INSERT INTO red_flags (filing_id, flag_type, severity, detail) VALUES (?, ?, ?, ?)",
                (filing_id, flag.get("flag_type"), flag.get("severity"), flag.get("detail")),
            )
    print(f"Saved {len(flags)} red flags to DB.")


def main() -> None:
    parser = argparse.ArgumentParser(prog="fin-parser")
    sub = parser.add_subparsers(dest="command", required=True)

    # fetch
    p = sub.add_parser("fetch")
    p.add_argument("ticker"); p.add_argument("--form", default="10-K"); p.add_argument("--limit", type=int, default=3)
    p.set_defaults(func=cmd_fetch)

    # download
    p = sub.add_parser("download")
    p.add_argument("ticker"); p.add_argument("--form", default="10-K"); p.add_argument("--limit", type=int, default=1)
    p.set_defaults(func=cmd_download)

    # extract
    p = sub.add_parser("extract")
    p.add_argument("ticker"); p.add_argument("--form", default="10-K"); p.add_argument("--limit", type=int, default=1)
    p.set_defaults(func=cmd_extract)

    # value
    p = sub.add_parser("value", help="Run DCF + WACC valuation using DB metrics")
    p.add_argument("ticker")
    p.add_argument("--market-cap", type=float, required=True, help="Market cap in millions USD")
    p.add_argument("--beta", type=float, default=1.24)
    p.add_argument("--risk-free", type=float, default=0.044)
    p.add_argument("--erp", type=float, default=0.055)
    p.add_argument("--interest-expense", type=float, default=3900.0)
    p.add_argument("--tax-rate", type=float, default=0.15)
    p.add_argument("--growth1", type=float, default=0.08)
    p.add_argument("--growth2", type=float, default=0.04)
    p.add_argument("--terminal-growth", type=float, default=0.025)
    p.set_defaults(func=cmd_value)

    # redflag
    p = sub.add_parser("redflag", help="Run red flag analysis using Claude")
    p.add_argument("ticker")
    p.set_defaults(func=cmd_redflag)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
