"""
fin_parser/cli.py
CLI entry point.

US (SEC EDGAR) commands:
  fin-parser fetch AAPL --form 10-K --limit 3
  fin-parser download AAPL --form 10-K --limit 1
  fin-parser extract AAPL --form 10-K --limit 1
  fin-parser value AAPL --market-cap 3200000
  fin-parser redflag AAPL

Canada (SEDAR+) commands — manual PDF upload for NI 43-101 / PEA / FS:
  fin-parser upload-ca --ticker ABX --company "Barrick Gold" \\
      --form "NI 43-101" --period 2025-03-31 --pdf ~/Downloads/report.pdf
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
        metrics = extract_metrics(
            Path(local_path),
            period=f["period"],
            reporting_units=getattr(args, "reporting_units", None),
        )
        save_metrics(filing_id, metrics, period=f["period"])
        print(f"\n  Extracted metrics for {f['company']} {f['period']}:")
        for metric, value in metrics.items():
            status = f"{value:>12,.2f}" if isinstance(value, float) else f"{'None':>12}"
            print(f"    {metric:<25} {status}")
    print("\nDone.")


def cmd_value(args: argparse.Namespace) -> None:
    from fin_parser.ingestion.repository import get_filing_by_accession, get_metrics
    from fin_parser.valuation.engine import DCFInputs, WACCInputs, run_valuation
    from fin_parser.db import get_connection, init_db

    # Make sure the outputs_json column exists on pre-existing DBs so the
    # snapshot write below doesn't fail on databases created before the
    # column was added.
    init_db()

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

    # Save to DB. We persist the full ValuationResult (WACC components,
    # DCF figures, IRR, P/E, sensitivity table) so the dashboard can
    # render the same breakdown the CLI just printed.
    from fin_parser.db import transaction
    import json as _json
    import math as _math

    def _nan_to_none(v):
        return None if (isinstance(v, float) and _math.isnan(v)) else v

    sensitivity_safe = {
        # JSON dict keys must be strings; the dashboard converts back to
        # float on read. NaN cells (when WACC <= terminal growth) become
        # None so the JSON stays standards-compliant.
        str(g): {str(w): _nan_to_none(v) for w, v in row.items()}
        for g, row in result.sensitivity.items()
    }

    outputs_payload = {
        "wacc":                       result.wacc,
        "cost_of_equity":             result.cost_of_equity,
        "cost_of_debt":               result.cost_of_debt,
        "equity_weight":              result.equity_weight,
        "debt_weight":                result.debt_weight,
        "intrinsic_value_per_share":  result.intrinsic_value_per_share,
        "enterprise_value":           result.enterprise_value,
        "terminal_value":             result.terminal_value,
        "pv_of_fcfs":                 result.pv_of_fcfs,
        "irr":                        result.irr,
        "pe_ratio":                   result.pe_ratio,
        "ev_ebitda":                  result.ev_ebitda,
        "sensitivity":                sensitivity_safe,
    }

    with transaction() as conn:
        conn.execute(
            "INSERT INTO valuations (filing_id, method, result, inputs_json, outputs_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                filing_id,
                "dcf_wacc",
                result.intrinsic_value_per_share,
                _json.dumps(result.inputs_summary),
                _json.dumps(outputs_payload),
            ),
        )
    print(f"\nValuation saved to DB.")


def _get_latest_filing_for_ticker(ticker: str):
    """Look up the most recent filing for a ticker by matching ticker, CIK, or name.
    Also matches Canadian pseudo-CIKs of the form 'CA-<TICKER>'."""
    from fin_parser.db import get_connection
    from fin_parser.ingestion.edgar import ticker_to_cik
    conn = get_connection()

    # Canadian uploads first (cheap, no network round-trip)
    row = conn.execute(
        """
        SELECT id, company, period, form_type, jurisdiction
        FROM filings
        WHERE cik = ? OR ticker = ?
        ORDER BY filed_date DESC LIMIT 1
        """,
        (f"CA-{ticker.upper()}", ticker.upper()),
    ).fetchone()
    if row:
        conn.close()
        return row

    # Try by EDGAR CIK next
    try:
        cik = ticker_to_cik(ticker)
        row = conn.execute(
            """
            SELECT id, company, period, form_type, jurisdiction
            FROM filings WHERE cik = ?
            ORDER BY filed_date DESC LIMIT 1
            """,
            (cik,),
        ).fetchone()
        if row:
            conn.close()
            return row
    except Exception:
        pass

    # Fallback: match company name
    row = conn.execute(
        """
        SELECT id, company, period, form_type, jurisdiction
        FROM filings WHERE company LIKE ?
        ORDER BY filed_date DESC LIMIT 1
        """,
        (f"%{ticker.upper()}%",),
    ).fetchone()
    conn.close()
    return row


def _resolve_redflag_filings(args: argparse.Namespace) -> list[dict]:
    """
    Resolve the set of filings the user wants analysed.

    Selection rules, in precedence order:
      1. --filing-id N        → that one row, regardless of ticker/form/project
      2. --all                → every filing matching ticker (+ optional --form/--project)
      3. --form / --project   → all matching filings for ticker
      4. (no selectors)       → single latest filing for ticker  (back-compat)

    Returns a list of sqlite3.Row-like dicts with id, company, period, form_type, project.
    """
    from fin_parser.db import get_connection
    from fin_parser.ingestion.edgar import ticker_to_cik

    conn = get_connection()

    # 1. Direct by id
    filing_id = getattr(args, "filing_id", None)
    if filing_id is not None:
        row = conn.execute(
            """
            SELECT id, company, period, form_type, project, jurisdiction
            FROM filings WHERE id = ?
            """,
            (filing_id,),
        ).fetchone()
        conn.close()
        return [dict(row)] if row else []

    # Work out which cik/ticker/name patterns might match this ticker
    ticker = (args.ticker or "").upper()
    cik_candidates = {f"CA-{ticker}"}
    try:
        cik_candidates.add(ticker_to_cik(args.ticker))
    except Exception:
        pass

    # Build WHERE clause
    where = ["(cik IN ({}) OR ticker = ? OR company LIKE ?)".format(
        ",".join("?" * len(cik_candidates))
    )]
    params: list = [*cik_candidates, ticker, f"%{ticker}%"]

    form = getattr(args, "form", None)
    if form:
        where.append("form_type = ?")
        params.append(form)

    project = getattr(args, "project", None)
    if project:
        where.append("project = ?")
        params.append(project)

    want_all = getattr(args, "all", False) or form or project

    sql = (
        "SELECT id, company, period, form_type, project, jurisdiction "
        "FROM filings WHERE " + " AND ".join(where) +
        " ORDER BY filed_date DESC"
    )
    if not want_all:
        sql += " LIMIT 1"

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def cmd_redflag(args: argparse.Namespace) -> None:
    from fin_parser.analysis.red_flags import analyze_red_flags
    from fin_parser.db import transaction
    from fin_parser.ingestion.repository import get_metrics, get_mining_metrics

    # Guard: ticker is optional in argparse (so --filing-id works standalone),
    # but without either selector we'd build a WHERE that matches every row.
    if not args.ticker and getattr(args, "filing_id", None) is None:
        print("Pass a ticker or --filing-id. Examples:")
        print("  fin-parser redflag AEM              # latest filing")
        print("  fin-parser redflag AEM --all        # every filing")
        print("  fin-parser redflag AEM --form 'NI 43-101'")
        print("  fin-parser redflag --filing-id 29")
        return

    filings = _resolve_redflag_filings(args)
    if not filings:
        print("No filings match that selector. Run 'fin-parser extract' / 'upload-ca' first,")
        print("or check --filing-id / --form / --project.")
        return

    severity_order = {"high": 0, "medium": 1, "low": 2}
    icons = {"high": "🔴", "medium": "🟡", "low": "🟢"}

    total_flags = 0
    for idx, row in enumerate(filings):
        filing_id = row["id"]
        company = row["company"]
        period = row["period"]
        form_type = row.get("form_type")
        project = row.get("project")

        # Per-filing header so it's obvious which report each set of flags belongs to.
        label = f"{company} · {form_type} · {period}"
        if project:
            label += f" · {project}"
        if idx > 0:
            print()
        print("=" * 72)
        print(f"Analyzing red flags for {label}  [filing_id={filing_id}]")
        print("=" * 72)

        raw_metrics = get_metrics(filing_id)
        metrics = {r["metric"]: r["value"] for r in raw_metrics}

        raw_mining = get_mining_metrics(filing_id)
        mining_metrics = {r["metric"]: r["value"] for r in raw_mining}
        commodity = next(
            (r.get("commodity") for r in raw_mining if r.get("commodity")), None
        )

        flags = analyze_red_flags(
            metrics, company, period,
            mining_metrics=mining_metrics or None,
            form_type=form_type,
            commodity=commodity,
        )

        # Upsert: wipe prior flags for this filing so re-runs replace instead of
        # piling up. Do this even when the new run returned zero flags.
        with transaction() as conn:
            conn.execute("DELETE FROM red_flags WHERE filing_id = ?", (filing_id,))
            for flag in flags:
                conn.execute(
                    "INSERT INTO red_flags (filing_id, flag_type, severity, detail) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        filing_id,
                        flag.get("flag_type"),
                        flag.get("severity"),
                        flag.get("detail"),
                    ),
                )

        if not flags:
            print("  No red flags identified.")
            continue

        flags.sort(key=lambda f: severity_order.get(f.get("severity", "low"), 3))
        print()
        for flag in flags:
            sev = flag.get("severity", "low")
            print(f"  {icons.get(sev, '⚪')} [{sev.upper()}] {flag.get('flag_type', 'unknown')}")
            print(f"     {flag.get('detail', '')}")
            print()
        total_flags += len(flags)

    print()
    print(f"Done. Analysed {len(filings)} filing(s), saved {total_flags} red flag(s) to DB.")


def cmd_upload_ca(args: argparse.Namespace) -> None:
    """
    Ingest a Canadian filing (NI 43-101, PEA, PFS, FS/DFS, Annual, MD&A, ...)
    from a local PDF. Runs the mining extractor when the form is a
    technical report; otherwise just registers the filing.
    """
    from pathlib import Path

    from fin_parser.db import init_db
    from fin_parser.ingestion.repository import (
        save_filing,
        save_metrics,
        save_mining_metrics,
    )
    from fin_parser.ingestion.sedar_plus import (
        is_mining_report,
        register_uploaded_filing,
    )

    init_db()  # make sure new columns exist on pre-existing DBs

    pdf = Path(args.pdf).expanduser().resolve()
    project = getattr(args, "project", None)
    header = f"Registering Canadian filing: {args.ticker} / {args.form} / {args.period}"
    if project:
        header += f" / project={project}"
    print(header)
    filing = register_uploaded_filing(
        pdf_path=pdf,
        ticker=args.ticker,
        company=args.company,
        form_type=args.form,
        period=args.period,
        filed_date=args.filed_date,
        project=project,
    )
    filing_id = save_filing(filing, raw_path=filing["raw_path"], project=project)
    tag = f" project={project}" if project else ""
    print(f"  Filing saved [id={filing_id}] jurisdiction=CA source=SEDAR+{tag}")

    if is_mining_report(args.form):
        from fin_parser.extraction.mining_extractor import extract_mining_report

        print("\n  Running mining extractor (NI 43-101 / PEA / PFS / FS)...")
        financial, mining, commodity = extract_mining_report(pdf, period=args.period)

        save_metrics(filing_id, financial, period=args.period)
        save_mining_metrics(filing_id, mining, period=args.period, commodity=commodity)

        print("\n  Financial metrics:")
        for metric, value in financial.items():
            status = (
                f"{value:>12,.2f}"
                if isinstance(value, (int, float)) and value is not None
                else f"{'None':>12}"
            )
            print(f"    {metric:<25} {status}")

        print("\n  Mining metrics:")
        for metric, value in mining.items():
            status = (
                f"{value:>14,.3f}"
                if isinstance(value, (int, float)) and value is not None
                else f"{'None':>14}"
            )
            print(f"    {metric:<30} {status}")
    else:
        # Quarterly MD&A, Interim Financial Statements, Annual Report,
        # AIF, etc. — run the standard financial extractor on the PDF.
        from fin_parser.extraction.claude_extractor import extract_metrics_from_pdf

        print(f"\n  Running financial extractor on '{args.form}' PDF...")
        financial = extract_metrics_from_pdf(
            pdf,
            period=args.period,
            reporting_units=getattr(args, "reporting_units", None),
        )
        save_metrics(filing_id, financial, period=args.period)

        print("\n  Financial metrics:")
        for metric, value in financial.items():
            status = (
                f"{value:>12,.2f}"
                if isinstance(value, (int, float)) and value is not None
                else f"{'None':>12}"
            )
            print(f"    {metric:<25} {status}")

    print("\nDone.")


def main() -> None:
    parser = argparse.ArgumentParser(prog="fin-parser")
    sub = parser.add_subparsers(dest="command", required=True)

    # fetch
    p = sub.add_parser("fetch", help="List recent EDGAR filings for a US ticker")
    p.add_argument("ticker"); p.add_argument("--form", default="10-K"); p.add_argument("--limit", type=int, default=3)
    p.add_argument("--jurisdiction", default="us", choices=["us"],
                   help="Only 'us' is supported for fetch — use 'upload-ca' for Canadian filings.")
    p.set_defaults(func=cmd_fetch)

    # download
    p = sub.add_parser("download", help="Download EDGAR filing HTM for a US ticker")
    p.add_argument("ticker"); p.add_argument("--form", default="10-K"); p.add_argument("--limit", type=int, default=1)
    p.add_argument("--jurisdiction", default="us", choices=["us"],
                   help="Only 'us' is supported for download — use 'upload-ca' for Canadian filings.")
    p.set_defaults(func=cmd_download)

    # extract
    p = sub.add_parser("extract", help="Download + extract metrics for a US ticker")
    p.add_argument("ticker"); p.add_argument("--form", default="10-K"); p.add_argument("--limit", type=int, default=1)
    p.add_argument("--jurisdiction", default="us", choices=["us"],
                   help="Only 'us' is supported for extract — use 'upload-ca' for Canadian filings.")
    p.add_argument(
        "--reporting-units", default=None, choices=["thousands", "millions"],
        help="Override the autodetected reporting scale for this filing. Use when "
             "the detector logs a non-obvious 'ambiguous' hint and you already "
             "know from the filing's cover page which unit is correct.",
    )
    p.set_defaults(func=cmd_extract)

    # upload-ca — manual PDF ingestion for Canadian filings (SEDAR+)
    p = sub.add_parser(
        "upload-ca",
        help="Register a Canadian filing from a local PDF (NI 43-101 / PEA / PFS / FS / Annual / MD&A)",
    )
    p.add_argument("--ticker", required=True, help="Issuer ticker (e.g. 'ABX', 'AEM', 'FNV')")
    p.add_argument("--company", required=True, help="Issuer legal name")
    p.add_argument(
        "--form", required=True,
        help="Report type: 'NI 43-101', 'PEA', 'PFS', 'FS', 'DFS', 'Annual Report', 'MD&A', ...",
    )
    p.add_argument("--period", required=True, help="Effective date of the report, YYYY-MM-DD")
    p.add_argument("--pdf", required=True, help="Path to the PDF on disk")
    p.add_argument("--filed-date", default=None, help="SEDAR+ filing date, YYYY-MM-DD (default: today)")
    p.add_argument(
        "--project", default=None,
        help="Mine / asset name for per-project NI 43-101 or PEA / PFS / FS "
             "reports (e.g. 'Detour Lake', 'Hope Bay', 'Fosterville'). "
             "Lets a single issuer carry multiple technical reports in the "
             "dashboard without collapsing. Leave unset for corporate filings.",
    )
    p.add_argument(
        "--reporting-units", default=None, choices=["thousands", "millions"],
        help="Override the autodetected reporting scale. Canadian IFRS interim "
             "statements almost always use 'thousands' of USD in their table "
             "headers even when the MD&A narrative quotes amounts inline in "
             "millions — use this flag to force the correct scale.",
    )
    p.set_defaults(func=cmd_upload_ca)

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
    p.add_argument(
        "ticker", nargs="?", default=None,
        help="Issuer ticker. Optional when --filing-id is used.",
    )
    p.add_argument(
        "--filing-id", type=int, default=None,
        help="Run red-flag analysis on this specific filings.id. Bypasses ticker "
             "resolution so you can target a single 43-101 precisely.",
    )
    p.add_argument(
        "--all", action="store_true",
        help="Analyse every filing on record for this ticker (optionally narrowed "
             "by --form / --project). Each filing gets its own set of flags in the DB.",
    )
    p.add_argument(
        "--form", default=None,
        help="Filter by form type (e.g. 'NI 43-101', '10-K', 'Interim Financial Statements').",
    )
    p.add_argument(
        "--project", default=None,
        help="Filter by project / mine name (e.g. 'Detour Lake'). Useful when one "
             "issuer has multiple technical reports and you only want to re-flag one.",
    )
    p.set_defaults(func=cmd_redflag)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
