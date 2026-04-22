"""
fin_parser/analysis/red_flags.py
Red flag agent: Claude reasons over extracted metrics and filing text
to identify financial risks, anomalies, and warning signs.
"""
from __future__ import annotations

import json
import re
from typing import Any

import anthropic

from fin_parser.config import ANTHROPIC_API_KEY, CLAUDE_MODEL

CLIENT = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

RED_FLAG_SYSTEM_PROMPT = """
You are a forensic financial analyst reviewing SEC filings for red flags and risks.
You will be given a set of extracted financial metrics and context from a filing.

Analyze the data and identify red flags across these categories:

1. DILUTION
   - Are shares outstanding growing faster than earnings?
   - Are stock-based compensation or option grants significant vs net income?

2. DEBT & LIQUIDITY
   - Is total debt growing faster than operating cash flow?
   - Is cash declining while debt is rising?
   - Is interest coverage (operating income / interest expense) below 3x?

3. EARNINGS QUALITY
   - Is net income significantly higher than operating cash flow? (accrual warning)
   - Is free cash flow declining while reported earnings grow?
   - Large goodwill relative to total assets can signal acquisition risk

4. GROWTH & MARGINS
   - Is revenue growth slowing or reversing?
   - Are margins compressing?

5. CAPITAL ALLOCATION
   - Is capex declining in a capital-intensive business? (underinvestment risk)
   - Are buybacks funded by debt rather than free cash flow?

For each red flag found, provide:
- flag_type: short snake_case identifier (e.g. "high_debt_growth", "accrual_warning")
- severity: "low", "medium", or "high"
- detail: 1-2 sentence explanation with specific numbers from the data

Return ONLY a valid JSON array of red flag objects. If no red flags found, return [].

Example output:
[
  {
    "flag_type": "accrual_warning",
    "severity": "medium",
    "detail": "Net income of $112B exceeds operating cash flow of $111B, suggesting some earnings may not be backed by cash."
  },
  {
    "flag_type": "goodwill_concentration",
    "severity": "low",
    "detail": "Goodwill of $72B represents a significant portion of the balance sheet, creating write-down risk if acquisitions underperform."
  }
]
""".strip()


# Extra categories layered on top of the base prompt when the filing is a
# Canadian mining technical report (NI 43-101 / PEA / PFS / FS). Kept as a
# suffix so standard 10-K analysis is unaffected.
MINING_RED_FLAG_ADDENDUM = """
ADDITIONAL CATEGORIES FOR MINING TECHNICAL REPORTS (NI 43-101 / PEA / PFS / FS):

6. RESERVE & RESOURCE QUALITY
   - Heavy reliance on inferred resources for mine plan viability
     (inferred cannot be used in economic analysis under NI 43-101;
     if the economics depend on them, flag high-severity).
   - Declining head grade over life of mine, or grade materially lower
     than peer projects for the same commodity.
   - Large gap between resources and reserves (low reserve conversion).
   - Reserves insufficient to justify the stated mine life at the
     planned throughput.

7. ECONOMIC ASSUMPTIONS
   - Commodity price assumption materially above the current spot price
     or consensus long-term price. Flag the gap explicitly.
   - Discount rate below 8% for development-stage projects, or below 5%
     for operating mines — often flatters the NPV.
   - Payback period > 50% of mine life.
   - After-tax IRR below the discount rate (project destroys value).

8. COST STRUCTURE
   - AISC in the 3rd or 4th industry cost quartile (compare to typical
     ranges: gold <$1,200/oz 1st quartile, >$1,600/oz 4th quartile;
     copper <$2/lb 1st, >$3/lb 4th).
   - Sustaining capex intensity — if LOM sustaining capex > 30% of
     initial capex, flag escalation risk.
   - Strip ratio rising sharply in later years (back-end loaded waste).

9. JURISDICTION & PERMITTING
   - Operating in a high-risk jurisdiction (look for political
     instability, expropriation history, changing royalty regimes).
   - Key permits still outstanding at FS stage.
   - Indigenous / community agreements not finalised.

10. TECHNICAL RISK
   - Novel or unproven metallurgy / recoveries assumed above what
     testwork demonstrates.
   - Infrastructure dependencies (roads, power, water) not yet
     financed or permitted.
   - Qualified person(s) disclosed conflicts of interest.

For all of the above, cite the specific numeric value from the metrics
context when possible (e.g. "IRR 6.2% is below the 8% discount rate").
""".strip()


MINING_METRIC_KEYS = {
    "npv_after_tax", "npv_pre_tax", "irr_after_tax", "irr_pre_tax",
    "payback_period", "discount_rate", "initial_capex", "sustaining_capex",
    "lom_revenue", "lom_free_cash_flow", "commodity_price_assumption",
    "mine_life", "annual_production", "aisc", "cash_cost", "opex_per_tonne",
    "strip_ratio", "throughput", "recovery_rate",
    "proven_reserves", "probable_reserves", "proven_probable_reserves",
    "measured_resources", "indicated_resources", "measured_indicated_resources",
    "inferred_resources", "head_grade", "contained_metal",
}


def build_metrics_context(
    metrics: dict[str, Any],
    company: str,
    period: str,
    mining_metrics: dict[str, Any] | None = None,
    form_type: str | None = None,
    commodity: str | None = None,
) -> str:
    """Format metrics dict(s) into a readable context string for Claude."""
    lines = [
        f"Company: {company}",
        f"Period: {period}",
    ]
    if form_type:
        lines.append(f"Filing type: {form_type}")
    if commodity:
        lines.append(f"Primary commodity: {commodity}")
    lines.append("")
    lines.append("EXTRACTED FINANCIAL METRICS (all monetary values in millions USD):")
    for metric, value in metrics.items():
        if value is not None:
            lines.append(
                f"  {metric}: {value:,.2f}" if isinstance(value, float)
                else f"  {metric}: {value}"
            )
        else:
            lines.append(f"  {metric}: not available")

    if mining_metrics:
        lines.append("")
        lines.append(
            "EXTRACTED MINING TECHNICAL METRICS "
            "(NPV/capex in USD M; IRR, discount rate, recovery as decimals; "
            "reserves & resources in Mt):"
        )
        for metric, value in mining_metrics.items():
            if value is not None:
                lines.append(
                    f"  {metric}: {value:,.4f}" if isinstance(value, float)
                    else f"  {metric}: {value}"
                )
            else:
                lines.append(f"  {metric}: not available")
    return "\n".join(lines)


def analyze_red_flags(
    metrics: dict[str, Any],
    company: str,
    period: str,
    mining_metrics: dict[str, Any] | None = None,
    form_type: str | None = None,
    commodity: str | None = None,
) -> list[dict[str, Any]]:
    """
    Send metrics to Claude and return a list of red flag dicts
    (flag_type / severity / detail).

    If `mining_metrics` is provided OR `form_type` looks like a mining
    technical report, the mining-specific addendum is appended to the
    system prompt so Claude also reasons about reserve quality,
    commodity-price assumptions, AISC positioning, and permitting risk.
    """
    is_mining = bool(mining_metrics) or (
        form_type is not None and form_type.strip().upper().replace("-", " ") in {
            "NI 43 101", "TECHNICAL REPORT", "PEA", "PFS", "FS", "DFS",
        }
    )

    system_prompt = RED_FLAG_SYSTEM_PROMPT
    if is_mining:
        system_prompt = f"{RED_FLAG_SYSTEM_PROMPT}\n\n{MINING_RED_FLAG_ADDENDUM}"

    context = build_metrics_context(
        metrics, company, period,
        mining_metrics=mining_metrics, form_type=form_type, commodity=commodity,
    )

    response = CLIENT.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=2500 if is_mining else 2000,
        system=system_prompt,
        messages=[{"role": "user", "content": context}],
    )

    raw = response.content[0].text.strip()
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    flags = json.loads(raw)
    return flags if isinstance(flags, list) else []
