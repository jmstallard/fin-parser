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


def build_metrics_context(metrics: dict[str, Any], company: str, period: str) -> str:
    """Format metrics dict into a readable context string for Claude."""
    lines = [
        f"Company: {company}",
        f"Period: {period}",
        "",
        "EXTRACTED FINANCIAL METRICS (all monetary values in millions USD):",
    ]
    for metric, value in metrics.items():
        if value is not None:
            lines.append(f"  {metric}: {value:,.2f}" if isinstance(value, float) else f"  {metric}: {value}")
        else:
            lines.append(f"  {metric}: not available")
    return "\n".join(lines)


def analyze_red_flags(
    metrics: dict[str, Any],
    company: str,
    period: str,
) -> list[dict[str, Any]]:
    """
    Send metrics to Claude and return a list of red flag dicts.
    Each dict has: flag_type, severity, detail.
    """
    context = build_metrics_context(metrics, company, period)

    response = CLIENT.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=2000,
        system=RED_FLAG_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": context}],
    )

    raw = response.content[0].text.strip()
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    flags = json.loads(raw)
    return flags if isinstance(flags, list) else []
