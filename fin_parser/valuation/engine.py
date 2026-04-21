"""
fin_parser/valuation/engine.py
Valuation engine: DCF, WACC, IRR, and sensitivity analysis.
Uses numpy-financial for all time-value calculations.
"""
from __future__ import annotations

import numpy as np
import numpy_financial as npf
from dataclasses import dataclass, field
from typing import Any


@dataclass
class WACCInputs:
    # Equity inputs
    risk_free_rate: float       # e.g. 0.044 (4.4% 10yr treasury)
    equity_risk_premium: float  # e.g. 0.055 (Damodaran ERP)
    beta: float                 # e.g. 1.24 for AAPL
    # Debt inputs
    total_debt: float           # millions USD
    interest_expense: float     # millions USD (annual)
    tax_rate: float             # e.g. 0.15
    # Capital structure
    market_cap: float           # millions USD


@dataclass
class DCFInputs:
    # From extracted metrics
    free_cash_flow: float       # most recent FCF, millions USD
    revenue: float              # millions USD
    # Growth assumptions
    growth_rate_stage1: float   # e.g. 0.08 (8% for years 1-5)
    growth_rate_stage2: float   # e.g. 0.04 (4% for years 6-10)
    terminal_growth_rate: float # e.g. 0.025 (2.5% perpetuity)
    # Discount rate
    wacc: float                 # computed by WACCInputs
    # Share data
    shares_outstanding: float   # millions
    # Projection horizon
    stage1_years: int = 5
    stage2_years: int = 5


@dataclass
class ValuationResult:
    # WACC
    wacc: float
    cost_of_equity: float
    cost_of_debt: float
    equity_weight: float
    debt_weight: float
    # DCF
    intrinsic_value_per_share: float
    enterprise_value: float
    terminal_value: float
    pv_of_fcfs: float
    # IRR
    irr: float | None
    # Multiples
    pe_ratio: float | None
    ev_ebitda: float | None
    # Sensitivity: {growth_rate: {wacc: intrinsic_value}}
    sensitivity: dict[float, dict[float, float]] = field(default_factory=dict)
    # Raw inputs stored for DB
    inputs_summary: dict[str, Any] = field(default_factory=dict)


def compute_wacc(inputs: WACCInputs) -> tuple[float, float, float]:
    """
    Returns (wacc, cost_of_equity, after_tax_cost_of_debt).
    CAPM: cost_of_equity = risk_free + beta * equity_risk_premium
    After-tax cost of debt = (interest_expense / total_debt) * (1 - tax_rate)
    """
    cost_of_equity = inputs.risk_free_rate + inputs.beta * inputs.equity_risk_premium

    if inputs.total_debt > 0:
        pre_tax_cost_of_debt = inputs.interest_expense / inputs.total_debt
        after_tax_cost_of_debt = pre_tax_cost_of_debt * (1 - inputs.tax_rate)
    else:
        after_tax_cost_of_debt = 0.0

    total_capital = inputs.market_cap + inputs.total_debt
    equity_weight = inputs.market_cap / total_capital
    debt_weight = inputs.total_debt / total_capital

    wacc = equity_weight * cost_of_equity + debt_weight * after_tax_cost_of_debt
    return wacc, cost_of_equity, after_tax_cost_of_debt


def project_fcfs(dcf: DCFInputs) -> list[float]:
    """Project free cash flows over stage1 + stage2 years."""
    fcfs = []
    fcf = dcf.free_cash_flow
    for _ in range(dcf.stage1_years):
        fcf *= (1 + dcf.growth_rate_stage1)
        fcfs.append(fcf)
    for _ in range(dcf.stage2_years):
        fcf *= (1 + dcf.growth_rate_stage2)
        fcfs.append(fcf)
    return fcfs


def compute_dcf(dcf: DCFInputs) -> tuple[float, float, float]:
    """
    Returns (enterprise_value, pv_of_fcfs, terminal_value).
    Terminal value uses Gordon Growth Model on final year FCF.
    """
    fcfs = project_fcfs(dcf)
    discount_factors = [(1 / (1 + dcf.wacc) ** t) for t in range(1, len(fcfs) + 1)]
    pv_of_fcfs = sum(f * d for f, d in zip(fcfs, discount_factors))

    # Terminal value at end of projection
    terminal_fcf = fcfs[-1] * (1 + dcf.terminal_growth_rate)
    terminal_value_undiscounted = terminal_fcf / (dcf.wacc - dcf.terminal_growth_rate)
    terminal_value_pv = terminal_value_undiscounted * discount_factors[-1]

    enterprise_value = pv_of_fcfs + terminal_value_pv
    return enterprise_value, pv_of_fcfs, terminal_value_pv


def compute_irr(dcf: DCFInputs) -> float | None:
    """
    IRR treating current FCF as the 'investment' (negative) and
    projected FCFs as returns. Returns None if IRR can't be computed.
    """
    fcfs = project_fcfs(dcf)
    cash_flows = [-dcf.free_cash_flow] + fcfs
    try:
        irr = npf.irr(cash_flows)
        return float(irr) if np.isfinite(irr) else None
    except Exception:
        return None


def sensitivity_analysis(
    dcf: DCFInputs,
    wacc_range: list[float] | None = None,
    growth_range: list[float] | None = None,
) -> dict[float, dict[float, float]]:
    """
    2D sensitivity table: growth_rate_stage1 vs wacc.
    Returns {growth_rate: {wacc: intrinsic_value_per_share}}
    """
    if wacc_range is None:
        wacc_range = [dcf.wacc + delta for delta in (-0.02, -0.01, 0, 0.01, 0.02)]
    if growth_range is None:
        growth_range = [
            dcf.growth_rate_stage1 + delta for delta in (-0.04, -0.02, 0, 0.02, 0.04)
        ]

    table: dict[float, dict[float, float]] = {}
    for g in growth_range:
        table[round(g, 4)] = {}
        for w in wacc_range:
            if w <= dcf.terminal_growth_rate:
                table[round(g, 4)][round(w, 4)] = float("nan")
                continue
            variant = DCFInputs(
                free_cash_flow=dcf.free_cash_flow,
                revenue=dcf.revenue,
                growth_rate_stage1=g,
                growth_rate_stage2=dcf.growth_rate_stage2,
                terminal_growth_rate=dcf.terminal_growth_rate,
                wacc=w,
                shares_outstanding=dcf.shares_outstanding,
                stage1_years=dcf.stage1_years,
                stage2_years=dcf.stage2_years,
            )
            ev, _, _ = compute_dcf(variant)
            price = ev / variant.shares_outstanding if variant.shares_outstanding else 0
            table[round(g, 4)][round(w, 4)] = round(price, 2)

    return table


def run_valuation(
    wacc_inputs: WACCInputs,
    dcf_inputs: DCFInputs,
    net_income: float | None = None,
    market_cap: float | None = None,
) -> ValuationResult:
    """Full valuation pipeline — call this from the CLI."""
    wacc, cost_of_equity, cost_of_debt = compute_wacc(wacc_inputs)
    total_capital = wacc_inputs.market_cap + wacc_inputs.total_debt
    equity_weight = wacc_inputs.market_cap / total_capital
    debt_weight = wacc_inputs.total_debt / total_capital

    # Plug computed WACC into DCF
    dcf_inputs.wacc = wacc

    ev, pv_fcfs, tv = compute_dcf(dcf_inputs)
    intrinsic_per_share = ev / dcf_inputs.shares_outstanding if dcf_inputs.shares_outstanding else 0
    irr = compute_irr(dcf_inputs)
    sensitivity = sensitivity_analysis(dcf_inputs)

    # Simple multiples
    pe = market_cap / net_income if (net_income and market_cap) else None

    return ValuationResult(
        wacc=round(wacc, 4),
        cost_of_equity=round(cost_of_equity, 4),
        cost_of_debt=round(cost_of_debt, 4),
        equity_weight=round(equity_weight, 4),
        debt_weight=round(debt_weight, 4),
        intrinsic_value_per_share=round(intrinsic_per_share, 2),
        enterprise_value=round(ev, 2),
        terminal_value=round(tv, 2),
        pv_of_fcfs=round(pv_fcfs, 2),
        irr=round(irr, 4) if irr else None,
        pe_ratio=round(pe, 2) if pe else None,
        ev_ebitda=None,  # needs EBITDA — add in future
        sensitivity=sensitivity,
        inputs_summary={
            "wacc_inputs": wacc_inputs.__dict__,
            "dcf_inputs": {k: v for k, v in dcf_inputs.__dict__.items()
                           if k not in ("stage1_years", "stage2_years")},
        },
    )
