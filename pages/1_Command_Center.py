"""Command Center - Overview + signals."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms, load_cogs_gaps, load_data_coverage, load_products, load_refund_summary, load_orders_enriched
from lib.metrics import calc_period_kpis, daily_summary, product_profitability, platform_summary
from lib.charts import area_chart, multi_line, bar_chart
from lib.signals import generate_signals
from lib.html_tables import render_table_css, render_alert_banner, render_cogs_gap_table

setup_page("Command Center")

# Inject shared HTML table CSS
st.markdown(render_table_css(), unsafe_allow_html=True)

# --- Header ---
st.markdown('<div class="section-header">COMMAND CENTER</div>', unsafe_allow_html=True)

# --- Sidebar controls ---
period_options = {7: "7D", 14: "14D", 30: "30D", 60: "60D", 90: "90D"}
days = st.sidebar.selectbox(
    "PERIOD", list(period_options.keys()), index=2,
    format_func=lambda x: period_options[x],
)

# Load data
platforms = load_platforms()
df = load_daily_metrics(days=days * 2)  # Load 2x for comparison

if df.empty:
    st.warning("No data available. Run ETL: python3.11 -m etl.run")
    st.stop()

# Map platform names
df["platform"] = df["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))

# Platform filter
all_platforms = sorted(df["platform"].unique())
selected_platforms = st.sidebar.multiselect("PLATFORMS", all_platforms, default=all_platforms)
df = df[df["platform"].isin(selected_platforms)]

if df.empty:
    st.warning("No data for selected filters.")
    st.stop()

# Data freshness
latest_date = df["date"].max()
st.markdown(
    f'<div class="freshness-badge">Data through: {latest_date}</div>',
    unsafe_allow_html=True,
)

# Load enriched order-level data for accurate profit/margin (net-revenue based)
_enriched_orders = pd.DataFrame()
try:
    _enriched_result = load_orders_enriched(days=days * 2)
    if isinstance(_enriched_result, tuple) and len(_enriched_result) == 2:
        _enriched_orders = _enriched_result[0]
except Exception:
    pass

# Compute net-revenue-based KPIs from enriched orders
_enriched_kpis = {}
if not _enriched_orders.empty and "profit_pln" in _enriched_orders.columns:
    _eo = _enriched_orders.copy()
    _eo["order_date"] = pd.to_datetime(_eo["order_date"], errors="coerce")
    _eo["_date"] = _eo["order_date"].dt.date

    _now = pd.Timestamp.now().date()
    _current_start = _now - timedelta(days=days)
    _prev_start = _current_start - timedelta(days=days)

    _cur = _eo[(_eo["_date"] >= _current_start) & (_eo["_date"] <= _now)]
    _prev = _eo[(_eo["_date"] >= _prev_start) & (_eo["_date"] < _current_start)]

    def _eo_agg(subset):
        return {
            "revenue_brutto": subset["revenue_pln"].sum() if "revenue_pln" in subset.columns else 0,
            "revenue_net": subset["revenue_net_pln"].sum() if "revenue_net_pln" in subset.columns else 0,
            "profit": subset["profit_pln"].sum() if "profit_pln" in subset.columns else 0,
            "total_costs": subset["total_costs_pln"].sum() if "total_costs_pln" in subset.columns else 0,
            "cogs": subset["cogs_pln"].sum() if "cogs_pln" in subset.columns else 0,
            "fees": subset["fees_pln"].sum() if "fees_pln" in subset.columns else 0,
            "shipping": subset["shipping_pln"].sum() if "shipping_pln" in subset.columns else 0,
        }

    _c = _eo_agg(_cur)
    _p = _eo_agg(_prev)
    _c["margin"] = (_c["profit"] / _c["revenue_net"] * 100) if _c["revenue_net"] > 0 else 0
    _p["margin"] = (_p["profit"] / _p["revenue_net"] * 100) if _p["revenue_net"] > 0 else 0
    _enriched_kpis = {
        "profit": _c["profit"],
        "profit_prev": _p["profit"],
        "profit_delta": ((_c["profit"] - _p["profit"]) / abs(_p["profit"]) * 100) if _p["profit"] != 0 else 0,
        "margin": _c["margin"],
        "margin_prev": _p["margin"],
        "revenue_net": _c["revenue_net"],
        "revenue_net_prev": _p["revenue_net"],
    }

# --- KPI Strip ---
refund_data = load_refund_summary(days=days)
kpis = calc_period_kpis(df, days, refund_summary=refund_data)

# Override profit/margin with enriched values if available
_use_enriched = bool(_enriched_kpis)

if kpis:
    k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
    k1.metric(
        "REVENUE",
        f"{kpis.get('revenue', 0):,.0f} PLN",
        delta=f"{kpis.get('revenue_delta', 0):+.1f}%" if kpis.get("revenue_prev", 0) > 0 else None,
    )

    _profit_val = _enriched_kpis.get("profit", kpis.get("profit", 0)) if _use_enriched else kpis.get("profit", 0)
    _profit_prev = _enriched_kpis.get("profit_prev", kpis.get("profit_prev", 0)) if _use_enriched else kpis.get("profit_prev", 0)
    _profit_delta = _enriched_kpis.get("profit_delta", kpis.get("profit_delta", 0)) if _use_enriched else kpis.get("profit_delta", 0)
    k2.metric(
        "PROFIT",
        f"{_profit_val:,.0f} PLN",
        delta=f"{_profit_delta:+.1f}%" if _profit_prev != 0 else None,
    )

    _margin_val = _enriched_kpis.get("margin", kpis.get("margin", 0)) if _use_enriched else kpis.get("margin", 0)
    _margin_prev = _enriched_kpis.get("margin_prev", kpis.get("margin_prev", 0)) if _use_enriched else kpis.get("margin_prev", 0)
    k3.metric(
        "MARGIN (NET)",
        f"{_margin_val:.1f}%",
        delta=f"{_margin_val - _margin_prev:+.1f}pp" if _margin_prev > 0 else None,
    )
    k4.metric(
        "ORDERS",
        f"{kpis.get('orders', 0):,}",
        delta=f"{kpis.get('orders_delta', 0):+.1f}%" if kpis.get("orders_prev", 0) > 0 else None,
    )
    k5.metric(
        "AOV",
        f"{kpis.get('aov', 0):,.0f} PLN",
        delta=f"{kpis.get('aov_delta', 0):+.1f}%" if kpis.get("aov_prev", 0) > 0 else None,
    )
    k6.metric(
        "UNITS",
        f"{kpis.get('units', 0):,}",
        delta=f"{kpis.get('units_delta', 0):+.1f}%" if kpis.get("units_prev", 0) > 0 else None,
    )
    refund_units = refund_data.get("total_units_returned", 0)
    refund_rate = refund_data.get("refund_rate_pct", 0)
    k7.metric(
        "REFUNDS",
        f"{refund_units} units",
        delta=f"{refund_rate:.1f}% rate" if refund_units > 0 else "0%",
        delta_color="inverse",
    )

# --- COGS Gap Alert ---
cogs_gaps = load_cogs_gaps(days=days)
if not cogs_gaps.empty:
    total_gap_rev = cogs_gaps["revenue_pln"].sum()
    total_gap_skus = len(cogs_gaps)
    total_rev = df["revenue_pln"].sum() if "revenue_pln" in df.columns else 1
    gap_pct = (total_gap_rev / total_rev * 100) if total_rev > 0 else 0

    st.markdown(render_alert_banner(
        title="COGS DATA GAP",
        body=f"{total_gap_skus} products missing costs &mdash; {total_gap_rev:,.0f} PLN unaccounted",
        detail=f"{gap_pct:.1f}% of revenue has no COGS. Margins are overstated. Add costs in Baselinker to fix.",
        variant="warning",
    ), unsafe_allow_html=True)

    # Merge image URLs from products
    products_for_images = load_products()
    if not products_for_images.empty and "image_url" in products_for_images.columns:
        cogs_gaps = cogs_gaps.merge(
            products_for_images[["sku", "image_url"]],
            on="sku", how="left",
        )

    # Top missing products table (show top 15)
    with st.expander(f"Top {min(15, total_gap_skus)} products missing COGS (by revenue impact)", expanded=total_gap_skus <= 20):
        st.markdown(render_cogs_gap_table(cogs_gaps, max_rows=15), unsafe_allow_html=True)

# --- Data Coverage Indicators ---
coverage = load_data_coverage(days=days)
cov1, cov2, cov3 = st.columns(3)
cov1.metric(
    "COGS COVERAGE",
    f"{coverage['cogs_coverage']:.0f}%",
    delta=f"{coverage.get('revenue_without_cogs', 0):,.0f} PLN uncovered" if coverage['cogs_coverage'] < 100 else "Full coverage",
    delta_color="inverse" if coverage['cogs_coverage'] < 90 else "normal",
)
cov2.metric(
    "FEE COVERAGE",
    f"{coverage['fee_coverage']:.0f}%",
)
cov3.metric(
    "DATA FRESHNESS",
    str(coverage['last_date']),
)

# --- Daily chart + Signals ---
daily = daily_summary(df)

# Build daily profit from enriched orders for the chart
_daily_profit_cc = pd.DataFrame()
if not _enriched_orders.empty and "profit_pln" in _enriched_orders.columns:
    _eo_cc = _enriched_orders.copy()
    _eo_cc["_date"] = pd.to_datetime(_eo_cc["order_date"], errors="coerce").dt.date.astype(str)
    _daily_profit_cc = _eo_cc.groupby("_date").agg(
        profit_net=("profit_pln", "sum"),
        revenue_net=("revenue_net_pln", "sum"),
    ).reset_index().rename(columns={"_date": "date"})
    _daily_profit_cc["date"] = pd.to_datetime(_daily_profit_cc["date"])
    _daily_profit_cc = _daily_profit_cc.sort_values("date")
    _daily_profit_cc["profit_net_7d"] = _daily_profit_cc["profit_net"].rolling(7, min_periods=1).mean()

col_chart, col_signals = st.columns([3, 1])

with col_chart:
    st.markdown('<div class="section-header">DAILY REVENUE & PROFIT</div>', unsafe_allow_html=True)
    if not daily.empty:
        daily["date"] = pd.to_datetime(daily["date"])
        # Filter to current period only for charts
        chart_cutoff = datetime.now() - timedelta(days=days)
        daily_chart = daily[daily["date"] >= chart_cutoff].copy()

        # Merge enriched profit into daily chart if available
        if not _daily_profit_cc.empty:
            daily_chart = daily_chart.merge(
                _daily_profit_cc[["date", "profit_net", "profit_net_7d"]],
                on="date", how="left",
            )
            for _c in ["profit_net", "profit_net_7d"]:
                if _c in daily_chart.columns:
                    daily_chart[_c] = daily_chart[_c].fillna(0)

        if not daily_chart.empty:
            y_cols = ["revenue_pln"]
            names = ["Revenue (brutto)"]
            colors = [COLORS["revenue"]]

            # Prefer enriched profit_net over old CM3
            if "profit_net" in daily_chart.columns and daily_chart["profit_net"].abs().sum() > 0:
                y_cols.append("profit_net")
                names.append("Profit (net)")
                colors.append(COLORS.get("cm3", COLORS["success"]))
            elif "cm3" in daily_chart.columns:
                y_cols.append("cm3")
                names.append("CM3")
                colors.append(COLORS["cm3"])

            if "profit_net_7d" in daily_chart.columns and daily_chart["profit_net_7d"].abs().sum() > 0:
                y_cols.append("profit_net_7d")
                names.append("Profit 7d MA")
                colors.append(COLORS["muted"])
            elif "revenue_pln_7d" in daily_chart.columns:
                y_cols.append("revenue_pln_7d")
                names.append("Revenue 7d MA")
                colors.append(COLORS["muted"])

            fig = multi_line(daily_chart, "date", y_cols, colors=colors, names=names, height=380)
            st.plotly_chart(fig, use_container_width=True)

with col_signals:
    st.markdown('<div class="section-header">SIGNALS</div>', unsafe_allow_html=True)
    product_df = product_profitability(df)
    signals = generate_signals(daily, product_df)
    if signals:
        for s in signals:
            st.markdown(
                f"""<div class="signal-card {s['type']}">
                    <div class="signal-title">{s['title']}</div>
                    <div class="signal-detail">{s['detail']}</div>
                </div>""",
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            '<div style="font-family: monospace; font-size: 0.8rem; color: #64748b; padding: 16px;">No active signals</div>',
            unsafe_allow_html=True,
        )

# --- Three columns: Platform breakdown, Top 5, Bottom 5 ---
st.markdown('<div class="section-header">BREAKDOWN</div>', unsafe_allow_html=True)

# Build platform and product profit from enriched orders if available
_plat_enriched = pd.DataFrame()
_prod_enriched = pd.DataFrame()
if not _enriched_orders.empty and "profit_pln" in _enriched_orders.columns:
    _eo_bd = _enriched_orders.copy()
    _eo_bd["order_date"] = pd.to_datetime(_eo_bd["order_date"], errors="coerce")
    _eo_bd["_date"] = _eo_bd["order_date"].dt.date
    _bd_cutoff = (pd.Timestamp.now() - timedelta(days=days)).date()
    _eo_bd = _eo_bd[_eo_bd["_date"] >= _bd_cutoff]

    if not _eo_bd.empty:
        # Platform summary from enriched data
        _plat_enriched = _eo_bd.groupby("platform_name").agg(
            revenue_pln=("revenue_pln", "sum"),
            revenue_net=("revenue_net_pln", "sum"),
            profit=("profit_pln", "sum"),
            total_costs=("total_costs_pln", "sum"),
            orders_count=("id", "count"),
            units=("unit_count", "sum"),
        ).reset_index().rename(columns={"platform_name": "platform"})
        _plat_enriched["margin_pct"] = np.where(
            _plat_enriched["revenue_net"] > 0,
            _plat_enriched["profit"] / _plat_enriched["revenue_net"] * 100,
            0,
        )
        _plat_enriched = _plat_enriched.sort_values("revenue_pln", ascending=False)

        # Product summary from enriched data
        _prod_enriched = _eo_bd.groupby("first_sku").agg(
            revenue_pln=("revenue_pln", "sum"),
            revenue_net=("revenue_net_pln", "sum"),
            profit=("profit_pln", "sum"),
            units=("unit_count", "sum"),
        ).reset_index().rename(columns={"first_sku": "sku"})
        _prod_enriched["margin_pct"] = np.where(
            _prod_enriched["revenue_net"] > 0,
            _prod_enriched["profit"] / _prod_enriched["revenue_net"] * 100,
            0,
        )
        _prod_enriched = _prod_enriched[_prod_enriched["sku"] != ""]
        _prod_enriched = _prod_enriched.sort_values("profit", ascending=False)

col_plat, col_top, col_bottom = st.columns(3)

with col_plat:
    st.markdown("**Platform Summary**")
    if not _plat_enriched.empty:
        display = _plat_enriched[["platform", "revenue_pln", "profit", "margin_pct", "orders_count", "units"]].copy()
        display.columns = ["Platform", "Revenue", "Profit", "Margin%", "Orders", "Units"]
        display["Revenue"] = display["Revenue"].map(lambda x: f"{x:,.0f}")
        display["Profit"] = display["Profit"].map(lambda x: f"{x:,.0f}")
        display["Margin%"] = display["Margin%"].map(lambda x: f"{x:.1f}%")
        st.dataframe(display, use_container_width=True, hide_index=True)
        _plat_csv = display.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", _plat_csv, "platform_summary.csv", "text/csv", key="dl_plat_summary")
    else:
        plat_df = platform_summary(df, platforms)
        if not plat_df.empty:
            display = plat_df[["platform", "revenue_pln", "cm3", "cm3_pct", "orders_count", "units"]].copy()
            display.columns = ["Platform", "Revenue", "CM3", "Margin%", "Orders", "Units"]
            display["Revenue"] = display["Revenue"].map(lambda x: f"{x:,.0f}")
            display["CM3"] = display["CM3"].map(lambda x: f"{x:,.0f}")
            display["Margin%"] = display["Margin%"].map(lambda x: f"{x:.1f}%")
            st.dataframe(display, use_container_width=True, hide_index=True)
            _plat_csv = display.to_csv(index=False).encode("utf-8")
            st.download_button("Download CSV", _plat_csv, "platform_summary.csv", "text/csv", key="dl_plat_summary")

with col_top:
    st.markdown("**Top 5 by Profit**")
    if not _prod_enriched.empty:
        top5 = _prod_enriched.head(5)[["sku", "profit", "margin_pct", "units"]].copy()
        top5.columns = ["SKU", "Profit", "Margin%", "Units"]
        top5["Profit"] = top5["Profit"].map(lambda x: f"{x:,.0f}")
        top5["Margin%"] = top5["Margin%"].map(lambda x: f"{x:.1f}%")
        st.dataframe(top5, use_container_width=True, hide_index=True)
    elif not product_df.empty:
        top5 = product_df.head(5)[["sku", "cm3", "cm3_pct", "units"]].copy()
        top5.columns = ["SKU", "CM3", "Margin%", "Units"]
        top5["CM3"] = top5["CM3"].map(lambda x: f"{x:,.0f}")
        top5["Margin%"] = top5["Margin%"].map(lambda x: f"{x:.1f}%")
        st.dataframe(top5, use_container_width=True, hide_index=True)

with col_bottom:
    st.markdown("**Bottom 5 by Profit**")
    if not _prod_enriched.empty:
        bottom5 = _prod_enriched.tail(5)[["sku", "profit", "margin_pct", "units"]].copy()
        bottom5.columns = ["SKU", "Profit", "Margin%", "Units"]
        bottom5["Profit"] = bottom5["Profit"].map(lambda x: f"{x:,.0f}")
        bottom5["Margin%"] = bottom5["Margin%"].map(lambda x: f"{x:.1f}%")
        st.dataframe(bottom5, use_container_width=True, hide_index=True)
    elif not product_df.empty:
        bottom5 = product_df.tail(5)[["sku", "cm3", "cm3_pct", "units"]].copy()
        bottom5.columns = ["SKU", "CM3", "Margin%", "Units"]
        bottom5["CM3"] = bottom5["CM3"].map(lambda x: f"{x:,.0f}")
        bottom5["Margin%"] = bottom5["Margin%"].map(lambda x: f"{x:.1f}%")
        st.dataframe(bottom5, use_container_width=True, hide_index=True)

# --- Revenue by platform stacked area ---
st.markdown('<div class="section-header">REVENUE BY PLATFORM</div>', unsafe_allow_html=True)
if not df.empty:
    chart_cutoff = datetime.now() - timedelta(days=days)
    plat_daily = df[pd.to_datetime(df["date"]) >= chart_cutoff].copy()
    plat_daily["date"] = pd.to_datetime(plat_daily["date"])
    plat_pivot = plat_daily.groupby(["date", "platform"])["revenue_pln"].sum().reset_index()
    plat_pivot = plat_pivot.pivot(index="date", columns="platform", values="revenue_pln").fillna(0)
    plat_pivot = plat_pivot.rolling(7, min_periods=1).mean()

    import plotly.graph_objects as go
    fig_plat = go.Figure()
    palette = [COLORS["primary"], COLORS["success"], COLORS["warning"], COLORS["danger"], COLORS["info"], COLORS["cm2"]]
    for i, col in enumerate(plat_pivot.columns):
        fig_plat.add_trace(go.Scatter(
            x=plat_pivot.index, y=plat_pivot[col], name=col,
            mode="lines", stackgroup="one",
            line=dict(width=0.5, color=palette[i % len(palette)]),
        ))
    fig_plat.update_layout(height=350, showlegend=True)
    st.plotly_chart(fig_plat, use_container_width=True)
