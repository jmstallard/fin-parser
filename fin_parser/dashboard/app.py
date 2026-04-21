"""
fin_parser/dashboard/app.py
Streamlit dashboard — compare companies side by side.

Run with: streamlit run fin_parser/dashboard/app.py
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="fin-parser",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Dark terminal aesthetic ────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

:root {
    --bg: #0a0e14;
    --surface: #0f1419;
    --border: #1e2730;
    --accent: #39d353;
    --accent2: #58a6ff;
    --warn: #f0883e;
    --danger: #f85149;
    --text: #cdd9e5;
    --muted: #768390;
}

html, body, [data-testid="stApp"] {
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'IBM Plex Sans', sans-serif;
}

[data-testid="stSidebar"] {
    background-color: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
}

h1, h2, h3 { font-family: 'IBM Plex Mono', monospace; color: var(--accent) !important; }
h4, h5, h6 { font-family: 'IBM Plex Mono', monospace; color: var(--accent2) !important; }

[data-testid="metric-container"] {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 12px;
}

.stDataFrame { background: var(--surface) !important; }
div[data-testid="stSelectbox"] label { color: var(--muted) !important; }

.flag-high   { color: #f85149; font-weight: 600; }
.flag-medium { color: #f0883e; font-weight: 600; }
.flag-low    { color: #39d353; font-weight: 600; }

.flag-card {
    background: var(--surface);
    border-left: 3px solid var(--border);
    padding: 10px 14px;
    margin: 6px 0;
    border-radius: 0 4px 4px 0;
    font-size: 0.9em;
}
.flag-card.high   { border-left-color: #f85149; }
.flag-card.medium { border-left-color: #f0883e; }
.flag-card.low    { border-left-color: #39d353; }
</style>
""", unsafe_allow_html=True)


# ── DB helpers ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_db() -> sqlite3.Connection:
    db_path = Path(__file__).parent.parent.parent / "data" / "fin_parser.db"
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def load_companies() -> pd.DataFrame:
    conn = get_db()
    return pd.read_sql(
        "SELECT id, company, cik, period, filed_date, form_type FROM filings ORDER BY company, filed_date DESC",
        conn
    )


def load_metrics(filing_id: int) -> dict[str, float]:
    conn = get_db()
    rows = conn.execute(
        "SELECT metric, value FROM metrics WHERE filing_id = ?", (filing_id,)
    ).fetchall()
    return {r["metric"]: r["value"] for r in rows}


def load_red_flags(filing_id: int) -> list[dict]:
    conn = get_db()
    rows = conn.execute(
        "SELECT flag_type, severity, detail FROM red_flags WHERE filing_id = ? ORDER BY severity",
        (filing_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def load_valuations(filing_id: int) -> list[dict]:
    conn = get_db()
    rows = conn.execute(
        "SELECT method, result, inputs_json FROM valuations WHERE filing_id = ? ORDER BY computed_at DESC",
        (filing_id,)
    ).fetchall()
    return [dict(r) for r in rows]


# ── Sidebar ────────────────────────────────────────────────────────────────
st.sidebar.markdown("## `fin-parser`")
st.sidebar.markdown("AI-powered SEC filing analysis")
st.sidebar.divider()

companies_df = load_companies()
if companies_df.empty:
    st.error("No filings in database. Run `fin-parser extract AAPL` first.")
    st.stop()

# Deduplicate — latest filing per company
latest = companies_df.groupby("company").first().reset_index()
company_options = latest["company"].tolist()

st.sidebar.markdown("#### Select Companies")
selected = st.sidebar.multiselect(
    "Compare up to 4",
    company_options,
    default=company_options[:min(2, len(company_options))],
    max_selections=4,
)

if not selected:
    st.info("Select at least one company from the sidebar.")
    st.stop()

# Load data for selected companies
filing_data = {}
for company in selected:
    row = latest[latest["company"] == company].iloc[0]
    metrics = load_metrics(int(row["id"]))
    flags = load_red_flags(int(row["id"]))
    valuations = load_valuations(int(row["id"]))
    filing_data[company] = {
        "id": int(row["id"]),
        "period": row["period"],
        "metrics": metrics,
        "flags": flags,
        "valuations": valuations,
    }

# ── Main ───────────────────────────────────────────────────────────────────
st.markdown("# `fin-parser` dashboard")
st.markdown(f"Analyzing **{len(selected)}** compan{'y' if len(selected)==1 else 'ies'} · "
            f"Periods: {', '.join(d['period'] for d in filing_data.values())}")
st.divider()

# ── Metrics comparison ─────────────────────────────────────────────────────
st.markdown("## Financial Metrics")

METRIC_LABELS = {
    "revenue": "Revenue ($M)",
    "net_income": "Net Income ($M)",
    "operating_cash_flow": "Operating CF ($M)",
    "free_cash_flow": "Free Cash Flow ($M)",
    "total_debt": "Total Debt ($M)",
    "cash_and_equivalents": "Cash ($M)",
    "eps_diluted": "EPS Diluted ($)",
    "capex": "Capex ($M)",
    "goodwill": "Goodwill ($M)",
}

# Summary table
table_rows = []
for company, data in filing_data.items():
    row = {"Company": company, "Period": data["period"]}
    for metric, label in METRIC_LABELS.items():
        v = data["metrics"].get(metric)
        row[label] = f"{v:,.1f}" if v is not None else "—"
    table_rows.append(row)

st.dataframe(
    pd.DataFrame(table_rows).set_index("Company"),
    use_container_width=True,
)

# ── Charts ─────────────────────────────────────────────────────────────────
st.markdown("## Charts")
chart_cols = st.columns(2)

CHART_METRICS = [
    ("revenue", "Revenue ($M)"),
    ("net_income", "Net Income ($M)"),
    ("free_cash_flow", "Free Cash Flow ($M)"),
    ("total_debt", "Total Debt ($M)"),
]

colors = ["#39d353", "#58a6ff", "#f0883e", "#f85149"]

for idx, (metric, label) in enumerate(CHART_METRICS):
    chart_data = []
    for company, data in filing_data.items():
        v = data["metrics"].get(metric)
        if v is not None:
            chart_data.append({"Company": company, "Value": v})

    if not chart_data:
        continue

    df = pd.DataFrame(chart_data)
    fig = px.bar(
        df, x="Company", y="Value", title=label,
        color="Company",
        color_discrete_sequence=colors,
    )
    fig.update_layout(
        paper_bgcolor="#0f1419",
        plot_bgcolor="#0a0e14",
        font=dict(color="#cdd9e5", family="IBM Plex Mono"),
        title_font=dict(color="#58a6ff"),
        showlegend=False,
        margin=dict(t=40, b=20, l=20, r=20),
    )
    fig.update_xaxes(gridcolor="#1e2730")
    fig.update_yaxes(gridcolor="#1e2730")

    with chart_cols[idx % 2]:
        st.plotly_chart(fig, use_container_width=True)

# ── Valuation ──────────────────────────────────────────────────────────────
st.markdown("## Valuations")
val_cols = st.columns(len(selected))
for i, (company, data) in enumerate(filing_data.items()):
    with val_cols[i]:
        st.markdown(f"#### {company.split()[0]}")
        if data["valuations"]:
            v = data["valuations"][0]
            st.metric("Intrinsic Value/Share", f"${v['result']:,.2f}")
            if v.get("inputs_json"):
                try:
                    inputs = json.loads(v["inputs_json"])
                    dcf = inputs.get("dcf_inputs", {})
                    wacc_in = inputs.get("wacc_inputs", {})
                    st.caption(f"WACC: {wacc_in.get('risk_free_rate', 0) + wacc_in.get('beta', 1) * wacc_in.get('equity_risk_premium', 0.055):.1%} · "
                               f"Growth: {dcf.get('growth_rate_stage1', 0):.0%} → {dcf.get('terminal_growth_rate', 0):.1%}")
                except Exception:
                    pass
        else:
            st.caption("No valuation — run `fin-parser value TICKER --market-cap N`")

# ── Red flags ──────────────────────────────────────────────────────────────
st.markdown("## Red Flag Analysis")
flag_cols = st.columns(len(selected))

severity_order = {"high": 0, "medium": 1, "low": 2}
icons = {"high": "🔴", "medium": "🟡", "low": "🟢"}

for i, (company, data) in enumerate(filing_data.items()):
    with flag_cols[i]:
        st.markdown(f"#### {company.split()[0]}")
        flags = sorted(data["flags"], key=lambda f: severity_order.get(f.get("severity", "low"), 3))
        if not flags:
            st.caption("No red flags — run `fin-parser redflag TICKER`")
        else:
            high = sum(1 for f in flags if f["severity"] == "high")
            med  = sum(1 for f in flags if f["severity"] == "medium")
            low  = sum(1 for f in flags if f["severity"] == "low")
            st.markdown(f"🔴 {high} high &nbsp; 🟡 {med} medium &nbsp; 🟢 {low} low",
                        unsafe_allow_html=True)
            for flag in flags:
                sev = flag.get("severity", "low")
                st.markdown(
                    f'<div class="flag-card {sev}">'
                    f'<span class="flag-{sev}">{icons[sev]} {flag["flag_type"]}</span><br>'
                    f'<small>{flag["detail"]}</small>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
