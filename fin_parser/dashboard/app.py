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


def _filings_columns() -> set[str]:
    """Robustly discover which columns the filings table actually has.
    Lets the dashboard keep working against older DBs that predate the
    jurisdiction / source columns."""
    conn = get_db()
    return {row["name"] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}


def load_companies() -> pd.DataFrame:
    conn = get_db()
    cols = _filings_columns()
    jurisdiction_expr = "jurisdiction" if "jurisdiction" in cols else "'US' AS jurisdiction"
    source_expr       = "source"       if "source"       in cols else "'EDGAR' AS source"
    project_expr      = "project"      if "project"      in cols else "NULL AS project"
    return pd.read_sql(
        f"""
        SELECT id, company, cik, period, filed_date, form_type,
               {jurisdiction_expr}, {source_expr}, {project_expr}
        FROM filings
        ORDER BY company, filed_date DESC
        """,
        conn,
    )


def load_metrics(filing_id: int) -> dict[str, float]:
    conn = get_db()
    rows = conn.execute(
        "SELECT metric, value FROM metrics WHERE filing_id = ?", (filing_id,)
    ).fetchall()
    return {r["metric"]: r["value"] for r in rows}


def load_mining_metrics(filing_id: int) -> list[dict]:
    """Return all NI 43-101 / PEA / FS technical metrics for a filing.
    Gracefully handles DBs that don't yet have the mining_metrics table."""
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT metric, value, unit, commodity, category
            FROM mining_metrics
            WHERE filing_id = ?
            """,
            (filing_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in rows]


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


# ── Jurisdiction helpers ───────────────────────────────────────────────────

FLAG_EMOJI = {"US": "🇺🇸", "CA": "🇨🇦"}

MINING_FORMS = {"NI 43-101", "Technical Report", "PEA", "PFS", "FS", "DFS"}


def flag_for(jurisdiction: str) -> str:
    return FLAG_EMOJI.get((jurisdiction or "US").upper(), "🏳️")


def _clean_project(value) -> str | None:
    """Return a real, non-empty project name — or None for any empty / NaN /
    'nan' / 'None' variants that SQLite NULL and pandas groupby tend to
    leave behind. Centralised here so every label site stays consistent."""
    if value is None:
        return None
    try:
        # Catches pandas NaN (float('nan')) and pd.NA without importing numpy.
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    return s


def is_mining(form_type: str) -> bool:
    if not form_type:
        return False
    return form_type.strip().upper().replace("-", " ") in {
        t.upper().replace("-", " ") for t in MINING_FORMS
    }


# ── Sidebar ────────────────────────────────────────────────────────────────
st.sidebar.markdown("## `fin-parser`")
st.sidebar.markdown("AI-powered filing analysis — SEC (US) + SEDAR+ (CA)")
st.sidebar.divider()

companies_df = load_companies()
if companies_df.empty:
    st.error(
        "No filings in database. Run `fin-parser extract AAPL` "
        "(US) or `fin-parser upload-ca --pdf ...` (Canada) first."
    )
    st.stop()

# Jurisdiction toggle — this is the US/Canada flag the user wanted
# when entering a ticker. Placed as the first sidebar control so it
# filters the company list below.
st.sidebar.markdown("#### Jurisdiction")
jurisdiction_choice = st.sidebar.radio(
    "Filter filings by filing jurisdiction",
    options=["All", "🇺🇸 US (EDGAR)", "🇨🇦 Canada (SEDAR+)"],
    index=0,
    label_visibility="collapsed",
)
_jcode = {
    "All": None,
    "🇺🇸 US (EDGAR)": "US",
    "🇨🇦 Canada (SEDAR+)": "CA",
}[jurisdiction_choice]

filtered_df = companies_df if _jcode is None else companies_df[companies_df["jurisdiction"] == _jcode]

if filtered_df.empty:
    st.warning(
        f"No {jurisdiction_choice} filings in the database yet. "
        f"Switch to 'All' or ingest filings for this jurisdiction first."
    )
    st.stop()

# Deduplicate — latest filing per (company, form_type, project). Including
# `project` in the key lets a single issuer carry multiple per-mine
# technical reports (e.g. Agnico Eagle's Detour Lake + Hope Bay NI 43-101s)
# as distinct dashboard entries. The groupby treats NaN as a valid group
# key when dropna=False is set, so corporate (project=NULL) filings
# still dedup correctly on (company, form_type) alone.
# Normalise nulls up-front so the groupby is well-defined across pandas
# versions (older pandas silently drops NaN groups even with dropna=False).
filtered_df = filtered_df.assign(project=filtered_df["project"].where(filtered_df["project"].notna(), None))
latest = (
    filtered_df.groupby(["company", "form_type", "project"], dropna=False)
    .first()
    .reset_index()
)
latest = latest.sort_values(["company", "form_type", "project"], na_position="first").reset_index(drop=True)

# Build display labels with a flag emoji + form type (+ project when set)
# so every entry is unambiguous in the multiselect.
def _label(row: pd.Series) -> str:
    base = f"{flag_for(row['jurisdiction'])} {row['company']} · {row['form_type']}"
    proj = _clean_project(row.get("project"))
    if proj:
        return f"{base} · {proj}"
    return base

latest["__label"] = latest.apply(_label, axis=1)
# (label, filing_id) tuples so we can distinguish same-company rows
# with different form types when the user makes a selection.
label_to_filing_id = dict(zip(latest["__label"], latest["id"].astype(int)))
option_labels = latest["__label"].tolist()

st.sidebar.markdown("#### Select Filings")
selected_labels = st.sidebar.multiselect(
    "Compare up to 4",
    option_labels,
    default=option_labels[: min(2, len(option_labels))],
    max_selections=4,
)
# Keep the downstream variable name (`selected`) for minimal blast
# radius, but the values are now "company · form_type" labels so each
# entry in filing_data is unique.
selected = selected_labels

st.sidebar.divider()
st.sidebar.caption(
    f"**Showing:** {len(latest)} companies · "
    f"{(filtered_df['jurisdiction'] == 'US').sum()} US · "
    f"{(filtered_df['jurisdiction'] == 'CA').sum()} CA"
)

if not selected:
    st.info("Select at least one company from the sidebar.")
    st.stop()

# Load data for each selected filing. `label` uniquely identifies a
# (company, form_type) pair, so two filings from the same issuer don't
# collide in the filing_data dict.
filing_data = {}
for label in selected:
    filing_id = label_to_filing_id[label]
    row = latest[latest["__label"] == label].iloc[0]
    metrics = load_metrics(filing_id)
    mining_metrics_rows = load_mining_metrics(filing_id)
    flags = load_red_flags(filing_id)
    valuations = load_valuations(filing_id)
    # `project` may not be present on rows from pre-migration DBs, and
    # SQLite NULLs arrive as NaN after a groupby — _clean_project folds
    # all those variants into a real None.
    proj = row["project"] if "project" in row.index else None
    filing_data[label] = {
        "id":            filing_id,
        "company":       row["company"],
        "period":        row["period"],
        "form_type":     row["form_type"],
        "jurisdiction":  row["jurisdiction"],
        "source":        row["source"],
        "project":       _clean_project(proj),
        "metrics":       metrics,
        "mining_metrics": mining_metrics_rows,
        "flags":         flags,
        "valuations":    valuations,
    }

# ── Main ───────────────────────────────────────────────────────────────────
st.markdown("# `fin-parser` dashboard")

_header_bits = [
    f"{flag_for(d['jurisdiction'])} **{c}** · {d['form_type']} · {d['period']}"
    for c, d in filing_data.items()
]
st.markdown(
    f"Analyzing **{len(selected)}** compan{'y' if len(selected)==1 else 'ies'} — "
    + " &nbsp;|&nbsp; ".join(_header_bits),
    unsafe_allow_html=True,
)
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

# Summary table. The index used to be `Company`, but with a single issuer
# potentially appearing under multiple form types (10-K + 10-Q, or NI 43-101
# + Interim FS) we key rows by the full label so duplicates are impossible.
table_rows = []
for label, data in filing_data.items():
    row = {
        "Filing":  label,
        "Form":    data["form_type"],
        "Project": data.get("project") or "—",
        "Period":  data["period"],
    }
    for metric, metric_label in METRIC_LABELS.items():
        v = data["metrics"].get(metric)
        row[metric_label] = f"{v:,.1f}" if v is not None else "—"
    table_rows.append(row)

st.dataframe(
    pd.DataFrame(table_rows).set_index("Filing"),
    use_container_width=True,
)

# ── Mining / Technical Report Metrics ──────────────────────────────────────
mining_companies = {
    c: d for c, d in filing_data.items()
    if is_mining(d["form_type"]) or d["mining_metrics"]
}

if mining_companies:
    st.markdown("## Technical Report Metrics")
    st.caption(
        "Extracted from NI 43-101 / PEA / PFS / FS reports. "
        "Reserves & resources in Mt, NPV & capex in USD M, IRR & recovery as %, "
        "AISC & cash cost in USD per unit of primary commodity."
    )

    # Render one row per metric category. We pivot the mining_metrics
    # list-of-rows into wide form so companies are columns.
    CATEGORY_ORDER = [
        ("economics", "Economics (base case)"),
        ("operating", "Operating"),
        ("reserves",  "Reserves (Proven & Probable)"),
        ("resources", "Resources (M&I + Inferred)"),
        ("geology",   "Geology"),
    ]

    # Friendly labels keep the UI skim-able without hiding the raw metric name.
    LABEL = {
        "npv_after_tax":              "NPV after-tax (USD M)",
        "npv_pre_tax":                "NPV pre-tax (USD M)",
        "irr_after_tax":              "IRR after-tax (%)",
        "irr_pre_tax":                "IRR pre-tax (%)",
        "payback_period":             "Payback (years)",
        "discount_rate":              "Discount rate (%)",
        "initial_capex":              "Initial capex (USD M)",
        "sustaining_capex":           "Sustaining capex LOM (USD M)",
        "lom_revenue":                "LOM revenue (USD M)",
        "lom_free_cash_flow":         "LOM free cash flow (USD M)",
        "commodity_price_assumption": "Price assumption (USD/unit)",
        "mine_life":                  "Mine life (years)",
        "annual_production":          "Annual production",
        "aisc":                       "AISC (USD/unit)",
        "cash_cost":                  "Cash cost C1 (USD/unit)",
        "opex_per_tonne":             "Opex per tonne (USD/t)",
        "strip_ratio":                "Strip ratio",
        "throughput":                 "Throughput (t/day)",
        "recovery_rate":              "Recovery (%)",
        "proven_reserves":            "Proven (Mt)",
        "probable_reserves":          "Probable (Mt)",
        "proven_probable_reserves":   "P&P total (Mt)",
        "measured_resources":         "Measured (Mt)",
        "indicated_resources":        "Indicated (Mt)",
        "measured_indicated_resources": "M&I total (Mt)",
        "inferred_resources":         "Inferred (Mt)",
        "inferred_to_mi_ratio":       "Inferred / M&I ratio",
        "head_grade":                 "Head grade",
        "contained_metal":            "Contained metal",
    }

    # Metrics expressed as decimal fractions should render as percentages.
    PCT_METRICS = {"irr_after_tax", "irr_pre_tax", "discount_rate", "recovery_rate"}

    def _fmt(metric: str, value: float | None) -> str:
        if value is None:
            return "—"
        if metric in PCT_METRICS:
            return f"{value * 100:,.1f}%"
        if metric == "inferred_to_mi_ratio":
            # A quick qualitative badge so the number is interpretable
            # at a glance. Thresholds match the docstring in
            # compute_derived_mining_metrics: <0.3 mature, 0.3-1.0
            # balanced, >1.0 speculative.
            if value < 0.3:
                tag = "mature"
            elif value <= 1.0:
                tag = "balanced"
            else:
                tag = "speculative"
            return f"{value:,.2f}× ({tag})"
        return f"{value:,.2f}"

    for cat_key, cat_label in CATEGORY_ORDER:
        # Collect metrics present in this category across any selected company
        rows_by_metric: dict[str, dict[str, float | None]] = {}
        commodity_by_company: dict[str, str | None] = {}

        for label, data in mining_companies.items():
            commodity_by_company[label] = None
            for row in data["mining_metrics"]:
                if row.get("category") != cat_key:
                    continue
                m = row["metric"]
                rows_by_metric.setdefault(m, {c: None for c in mining_companies})
                rows_by_metric[m][label] = row["value"]
                if row.get("commodity"):
                    commodity_by_company[label] = row["commodity"]

        if not rows_by_metric:
            continue

        st.markdown(f"### {cat_label}")
        table = []
        for metric, values in rows_by_metric.items():
            row = {"Metric": LABEL.get(metric, metric)}
            for lbl in mining_companies:
                # Columns are already labelled with flag + company + form,
                # so don't double-prefix the flag here.
                row[lbl] = _fmt(metric, values.get(lbl))
            table.append(row)

        st.dataframe(pd.DataFrame(table).set_index("Metric"), use_container_width=True)

        # Commodity pill — useful context for AISC / head grade rows.
        if cat_key in {"operating", "geology"}:
            cpill = " · ".join(
                f"{lbl}: {commodity_by_company[lbl] or 'primary commodity'}"
                for lbl in mining_companies
            )
            st.caption(f"Primary commodity — {cpill}")

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

def _short(data: dict) -> str:
    """Compact chart label: company first word + form type (+ project).
    'Agnico Eagle Mines Limited' + 'NI 43-101' + 'Detour Lake' →
    'Agnico · NI 43-101 · Detour Lake' — short enough for a bar-chart
    x-axis tick but still unique per filing."""
    first = (data["company"] or "").split()[0] or data["company"]
    base = f"{first} · {data['form_type']}"
    if data.get("project"):
        return f"{base} · {data['project']}"
    return base

for idx, (metric, label) in enumerate(CHART_METRICS):
    chart_data = []
    for lbl, data in filing_data.items():
        v = data["metrics"].get(metric)
        if v is not None:
            chart_data.append({"Company": _short(data), "Value": v})

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
for i, (lbl, data) in enumerate(filing_data.items()):
    with val_cols[i]:
        # Short company name + form type so same-issuer columns don't
        # look identical (e.g. 'Agnico · 10-Q' vs 'Agnico · NI 43-101').
        short = (data["company"] or "").split()[0] or data["company"]
        header = f"{short} · {data['form_type']}"
        if data.get("project"):
            header += f" · {data['project']}"
        st.markdown(f"#### {header}")
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

for i, (lbl, data) in enumerate(filing_data.items()):
    with flag_cols[i]:
        short = (data["company"] or "").split()[0] or data["company"]
        header = f"{short} · {data['form_type']}"
        if data.get("project"):
            header += f" · {data['project']}"
        st.markdown(f"#### {header}")
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
