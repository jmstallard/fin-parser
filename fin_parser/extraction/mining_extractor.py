"""
fin_parser/extraction/mining_extractor.py

Extraction for Canadian mining technical reports (NI 43-101, PEA, PFS,
FS/DFS). Parses the PDF with pdfplumber, chunks the text, and sends each
chunk to Claude with a mining-specific prompt that also pulls any
conventional financial metrics it finds along the way.

Returns two dicts:
    financial_metrics — the same shape as claude_extractor.METRICS_TO_EXTRACT
    mining_metrics    — NI 43-101 / study-specific values (reserves, NPV,
                        IRR, AISC, mine life, grade, recovery, etc.)

The caller decides how to persist them (repository.save_metrics vs.
repository.save_mining_metrics).
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import anthropic

from fin_parser.config import ANTHROPIC_API_KEY, CLAUDE_MODEL
from fin_parser.extraction.claude_extractor import (
    METRICS_TO_EXTRACT as FINANCIAL_METRICS,
    extract_text_from_pdf,
)

CLIENT = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# Mining metrics we ask Claude to extract. These are paired with unit
# metadata in `fin_parser.ingestion.repository.MINING_METRIC_UNITS`.
MINING_METRICS: list[str] = [
    # Economics (PEA / PFS / FS)
    "npv_after_tax",              # NPV at the report's discount rate, USD millions
    "npv_pre_tax",
    "irr_after_tax",              # percent (e.g. 0.24 for 24%)
    "irr_pre_tax",
    "payback_period",             # years
    "discount_rate",              # percent (e.g. 0.08 for 8%)
    "initial_capex",              # USD millions
    "sustaining_capex",           # USD millions, life of mine total
    "lom_revenue",                # life-of-mine revenue, USD millions
    "lom_free_cash_flow",         # life-of-mine free cash flow, USD millions
    "commodity_price_assumption", # base-case price used in the model

    # Operating
    "mine_life",                  # years
    "annual_production",          # primary-commodity units per year (oz, lbs, t)
    "aisc",                       # all-in sustaining cost, USD per unit of commodity
    "cash_cost",                  # C1 cash cost, USD per unit of commodity
    "opex_per_tonne",             # total operating cost per tonne milled/processed, USD/t
    "strip_ratio",                # waste:ore (open pit only), ratio
    "throughput",                 # tonnes per day through the mill
    "recovery_rate",              # percent (e.g. 0.92 for 92%)

    # Reserves (Proven / Probable under CIM definitions)
    "proven_reserves",            # contained tonnage, Mt (millions of tonnes)
    "probable_reserves",
    "proven_probable_reserves",

    # Resources (Measured / Indicated / Inferred)
    "measured_resources",
    "indicated_resources",
    "measured_indicated_resources",
    "inferred_resources",

    # Geology
    "head_grade",                 # average grade of feed (g/t Au, % Cu, % Li2O, etc.)
    "contained_metal",            # total contained metal in reserves (oz, Mlb, t)

    # Derived — computed after extraction, not asked of Claude. Kept in the
    # schema so persistence + display flow through the normal pipeline.
    "inferred_to_mi_ratio",       # inferred / (measured + indicated), dimensionless
]


# Metrics that are *computed* rather than extracted. Kept separate so the
# LLM prompt doesn't ask for them (which would encourage hallucination) and
# so `compute_derived_mining_metrics` can enforce consistent math.
DERIVED_MINING_METRICS: set[str] = {"inferred_to_mi_ratio"}


def compute_derived_mining_metrics(mining: dict[str, Any]) -> dict[str, Any]:
    """
    Compute metrics derived from other mining values. Returns a NEW dict
    (doesn't mutate the input) so callers can decide whether to merge.

    Currently:
      inferred_to_mi_ratio = inferred_resources / (M&I resources)
        - prefers `measured_indicated_resources` if the report publishes it
        - falls back to measured + indicated if only the breakdown exists
        - returns None if the denominator is missing, zero, or negative

    Interpretation guide (from CIM definitions + industry convention):
      < 0.3   → mature, well-delineated resource base (low exploration upside)
      0.3–1.0 → balanced — some discovery potential still in the ground
      > 1.0   → heavy on Inferred, i.e. resource base is speculative and
                requires further drilling before it can be converted
      None    → report didn't quantify M&I or Inferred separately
    """
    out = dict(mining)
    inferred = mining.get("inferred_resources")
    mi_total = mining.get("measured_indicated_resources")

    # Fallback: derive M&I from the breakdown when the report omits the subtotal.
    if mi_total is None:
        m = mining.get("measured_resources")
        i = mining.get("indicated_resources")
        if m is not None or i is not None:
            mi_total = (m or 0.0) + (i or 0.0)

    if inferred is not None and mi_total and mi_total > 0:
        out["inferred_to_mi_ratio"] = float(inferred) / float(mi_total)
    else:
        out["inferred_to_mi_ratio"] = None
    return out


MINING_SYSTEM_PROMPT = """
You are a mining analyst extracting data from a Canadian technical report
(NI 43-101, PEA, PFS, FS, or DFS).

You will be given a chunk of the report. Extract BOTH:
  (a) the standard financial metrics listed in FINANCIAL_METRICS, and
  (b) the technical/economic metrics listed in MINING_METRICS.

Return ONE valid JSON object with ALL keys present; use null for anything
not stated in this chunk. No preamble, no markdown — just the JSON.

─── FINANCIAL_METRICS (same as 10-K extraction; use millions of USD) ───
revenue, net_income, eps_basic, eps_diluted,
free_cash_flow, operating_cash_flow, shares_outstanding, shares_diluted,
total_debt, cash_and_equivalents, goodwill, capex

─── MINING_METRICS ────────────────────────────────────────────────────
Economics (base case, AFTER tax unless pre-tax is explicitly requested):
- npv_after_tax:     After-tax NPV at the project's base-case discount
                     rate, in MILLIONS of USD. Convert from CAD if
                     reported in CAD (use the exchange rate stated in
                     the report; if none given, assume 1 CAD = 0.75 USD).
- npv_pre_tax:       Pre-tax NPV at the same discount rate, USD millions.
- irr_after_tax:     After-tax IRR as a decimal fraction (e.g. 0.24 for 24%).
- irr_pre_tax:       Pre-tax IRR as a decimal fraction.
- payback_period:    Years from first production to capital payback.
- discount_rate:     Discount rate used for the NPV, as a decimal (0.08 = 8%).
- initial_capex:     Initial project capital cost, USD millions.
- sustaining_capex:  Total life-of-mine sustaining capex, USD millions.
- lom_revenue:       Total life-of-mine revenue, USD millions.
- lom_free_cash_flow: Total life-of-mine after-tax free cash flow, USD millions.
- commodity_price_assumption: Base-case commodity price in the NPV model
                     (e.g. 1850 for $1,850/oz gold, 4.00 for $4.00/lb copper).

Operating:
- mine_life:         Life of mine, in years.
- annual_production: Average annual production of the PRIMARY commodity,
                     in its native unit (oz for Au/Ag, lbs for Cu/Mo/U,
                     tonnes for Fe/Ni/Li/Zn). Report the numeric value
                     only — the caller tracks the unit separately.
- aisc:              All-in sustaining cost, USD per unit of primary
                     commodity (USD/oz for Au/Ag, USD/lb for Cu).
- cash_cost:         C1 cash cost, same unit as AISC.
- opex_per_tonne:    Total operating cost per tonne milled/processed, USD/t.
- strip_ratio:       Waste:ore strip ratio for open-pit mines (e.g. 2.5
                     means 2.5 t waste per 1 t ore). null for underground.
- throughput:        Plant/mill throughput in tonnes per day.
- recovery_rate:     Overall metallurgical recovery as a decimal (0.92 = 92%).

Reserves (CIM / NI 43-101 definitions):
- proven_reserves:          Proven reserves tonnage, in MILLIONS of tonnes (Mt).
- probable_reserves:         Probable reserves tonnage, Mt.
- proven_probable_reserves:  P&P total tonnage, Mt.

Resources (reported EXCLUSIVE of reserves unless the report says otherwise):
- measured_resources:          Measured resources tonnage, Mt.
- indicated_resources:          Indicated resources tonnage, Mt.
- measured_indicated_resources: M&I total tonnage, Mt.
- inferred_resources:           Inferred resources tonnage, Mt.

Geology:
- head_grade:        Average head grade (grade of feed to the mill) for
                     the primary commodity. Use g/t for gold/silver,
                     percent (as a decimal fraction) for Cu/Ni/Zn/Li2O,
                     lbs/st (converted to % as decimal) for U3O8.
- contained_metal:   Total contained metal in Proven + Probable reserves,
                     in primary-commodity units (oz for Au, Mlbs for Cu,
                     t for Ni). Numeric value only.

─── RULES ────────────────────────────────────────────────────────────
- Use the report's BASE CASE, not sensitivity cases.
- If multiple commodities exist, report values for the PRIMARY metal
  (whichever contributes most revenue in the base case).
- Convert CAD to USD where needed (use the report's stated rate; else
  1 CAD ≈ 0.75 USD).
- Never invent numbers. If a metric is not stated in this chunk, use null.
- Percentages as decimals (24% → 0.24). Tonnage always in Mt. USD always
  in millions for NPV/capex/revenue; per-unit costs (AISC, cash_cost,
  opex_per_tonne) keep their native USD/unit form.

─── OUTPUT SCHEMA (all keys must be present, null allowed) ─────────────
{
  "revenue": null, "net_income": null, "eps_basic": null, "eps_diluted": null,
  "free_cash_flow": null, "operating_cash_flow": null,
  "shares_outstanding": null, "shares_diluted": null,
  "total_debt": null, "cash_and_equivalents": null, "goodwill": null, "capex": null,

  "npv_after_tax": null, "npv_pre_tax": null,
  "irr_after_tax": null, "irr_pre_tax": null,
  "payback_period": null, "discount_rate": null,
  "initial_capex": null, "sustaining_capex": null,
  "lom_revenue": null, "lom_free_cash_flow": null,
  "commodity_price_assumption": null,

  "mine_life": null, "annual_production": null,
  "aisc": null, "cash_cost": null, "opex_per_tonne": null,
  "strip_ratio": null, "throughput": null, "recovery_rate": null,

  "proven_reserves": null, "probable_reserves": null, "proven_probable_reserves": null,
  "measured_resources": null, "indicated_resources": null,
  "measured_indicated_resources": null, "inferred_resources": null,

  "head_grade": null, "contained_metal": null
}
""".strip()


# PDF text extraction is shared with `claude_extractor.extract_text_from_pdf`
# (imported above) so both pipelines produce identical input to Claude.

# ── Section heuristics ─────────────────────────────────────────────────────

# NI 43-101 reports follow a strict 27-section structure. The economic
# analysis is always section 22 (PEA / PFS / FS); reserves are section
# 15 and resources are section 14. We chase those keywords first.
HIGH_VALUE_MARKERS: list[str] = [
    "ITEM 22 ECONOMIC ANALYSIS",
    "22 ECONOMIC ANALYSIS",
    "ECONOMIC ANALYSIS",
    "AFTER-TAX NPV",
    "AFTER TAX NPV",
    "INTERNAL RATE OF RETURN",
    "NET PRESENT VALUE",
    "MINERAL RESERVE ESTIMATE",
    "MINERAL RESERVE STATEMENT",
    "MINERAL RESOURCE ESTIMATE",
    "MINERAL RESOURCE STATEMENT",
    "CAPITAL AND OPERATING COST",
    "ALL-IN SUSTAINING COST",
    "ALL IN SUSTAINING COST",
    "EXECUTIVE SUMMARY",
    "SUMMARY",
]


def find_study_sections(text: str) -> list[str]:
    """
    Return text windows around the highest-value sections of a technical
    report. Each window is ~40k chars, centred on the marker.

    We send multiple windows rather than chunking the whole 400-page
    report because mining reports have predictable high-density
    locations and we want to stay under context limits for cost.
    """
    text_upper = text.upper()
    seen: list[tuple[int, int]] = []  # (start, end)
    windows: list[str] = []
    WIN_HALF = 20_000  # 20k chars either side of the marker

    for marker in HIGH_VALUE_MARKERS:
        start = 0
        while True:
            pos = text_upper.find(marker, start)
            if pos == -1:
                break
            win_start = max(0, pos - 2_000)
            win_end = min(len(text), pos + WIN_HALF * 2)
            # Deduplicate overlapping windows — if this overlaps a
            # previously captured range by more than half, skip it.
            if not any(
                win_start < s_end and win_end > s_start and
                min(win_end, s_end) - max(win_start, s_start) > WIN_HALF
                for s_start, s_end in seen
            ):
                seen.append((win_start, win_end))
                windows.append(text[win_start:win_end])
            start = pos + len(marker)

    # Fallback: if we found nothing (unusual layout), just chunk the
    # whole document.
    if not windows:
        return [text]
    return windows


def chunk_text(text: str, chunk_size: int = 14_000, overlap: int = 500) -> list[str]:
    """Split a long section window into overlapping Claude-sized chunks."""
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start = end - overlap
    return chunks


# ── Claude round-trip ──────────────────────────────────────────────────────

ALL_METRICS: list[str] = FINANCIAL_METRICS + MINING_METRICS

# Metrics actually asked of Claude. Derived metrics are excluded so progress
# counts and the 85% early-exit threshold aren't diluted by values that
# nobody in the chunk loop can populate.
EXTRACTABLE_METRICS: list[str] = [m for m in ALL_METRICS if m not in DERIVED_MINING_METRICS]


def _parse_claude_json(raw: str) -> dict[str, Any]:
    raw = raw.strip()
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def extract_from_chunk(chunk: str) -> dict[str, Any]:
    response = CLIENT.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=2000,
        system=MINING_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": chunk}],
    )
    return _parse_claude_json(response.content[0].text)


def merge_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    """First-non-null wins, evaluated across all chunks."""
    merged: dict[str, Any] = {m: None for m in ALL_METRICS}
    for r in results:
        for m in ALL_METRICS:
            if merged[m] is None and r.get(m) is not None:
                merged[m] = r[m]
    return merged


def detect_commodity(text: str) -> str | None:
    """
    Light heuristic: the primary commodity almost always appears in the
    first 5k chars of a technical report (cover page / executive
    summary). Returns a normalized commodity name or None.
    """
    head = text[:5000].lower()
    candidates = [
        ("gold", ["gold", "au "]),
        ("silver", ["silver", "ag "]),
        ("copper", ["copper", "cu "]),
        ("nickel", ["nickel", "ni "]),
        ("zinc", ["zinc", "zn "]),
        ("lithium", ["lithium", "li2o", "spodumene"]),
        ("uranium", ["uranium", "u3o8"]),
        ("iron", ["iron ore", "magnetite"]),
    ]
    for name, keywords in candidates:
        if any(k in head for k in keywords):
            return name
    return None


# ── Public entry point ─────────────────────────────────────────────────────

def extract_mining_report(
    filing_path: Path,
    period: str,
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    """
    Full pipeline for a NI 43-101 / PEA / PFS / FS PDF.

    Returns (financial_metrics, mining_metrics, commodity).
    """
    filing_path = Path(filing_path)
    print(f"  Extracting text from {filing_path.name}...")
    full_text = extract_text_from_pdf(filing_path)
    print(f"  Full text length: {len(full_text):,} chars")

    commodity = detect_commodity(full_text)
    if commodity:
        print(f"  Detected primary commodity: {commodity}")

    sections = find_study_sections(full_text)
    print(f"  Targeted {len(sections)} high-value section window(s)")

    chunks: list[str] = []
    for section in sections:
        chunks.extend(chunk_text(section))
    # Safety cap: large FS reports can produce >50 chunks; trim the tail.
    MAX_CHUNKS = 30
    if len(chunks) > MAX_CHUNKS:
        print(f"  Truncating {len(chunks)} chunks -> {MAX_CHUNKS}")
        chunks = chunks[:MAX_CHUNKS]

    print(f"  Sending {len(chunks)} chunk(s) to Claude...")
    results: list[dict[str, Any]] = []
    for i, chunk in enumerate(chunks):
        print(f"    Chunk {i + 1}/{len(chunks)}...", end=" ", flush=True)
        try:
            results.append(extract_from_chunk(chunk))
            merged_so_far = merge_results(results)
            # Only count extractable metrics toward progress. Derived ones
            # get filled in after the loop.
            found = sum(1 for m in EXTRACTABLE_METRICS if merged_so_far.get(m) is not None)
            print(f"{found}/{len(EXTRACTABLE_METRICS)} metrics found")
            # Early exit if we've filled most of the schema
            if found >= int(len(EXTRACTABLE_METRICS) * 0.85):
                print("  Stopping early — ≥85% of metrics populated.")
                break
        except Exception as e:
            print(f"ERROR: {e}")
            results.append({})

    merged = merge_results(results)
    financial = {m: merged[m] for m in FINANCIAL_METRICS}
    # Claude is asked for EXTRACTED metrics only; derived ones are computed
    # locally from those values to keep the math consistent.
    extracted_mining_keys = [m for m in MINING_METRICS if m not in DERIVED_MINING_METRICS]
    mining = {m: merged.get(m) for m in extracted_mining_keys}
    mining = compute_derived_mining_metrics(mining)

    if mining.get("inferred_to_mi_ratio") is not None:
        print(f"  Derived inferred_to_mi_ratio = {mining['inferred_to_mi_ratio']:.3f}")
    return financial, mining, commodity
