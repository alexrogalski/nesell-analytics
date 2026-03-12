"""Command Center - Overview + signals."""
import streamlit as st
import pandas as pd
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms
from lib.metrics import calc_period_kpis, daily_summary, product_profitability, platform_summary
from lib.charts import area_chart, multi_line, bar_chart
from lib.signals import generate_signals

setup_page("Command Center")

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

# --- KPI Strip ---
kpis = calc_period_kpis(df, days)
if kpis:
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric(
        "REVENUE",
        f"{kpis.get('revenue', 0):,.0f} PLN",
        delta=f"{kpis.get('revenue_delta', 0):+.1f}%" if kpis.get("revenue_prev", 0) > 0 else None,
    )
    k2.metric(
        "CM3 (PROFIT)",
        f"{kpis.get('profit', 0):,.0f} PLN",
        delta=f"{kpis.get('profit_delta', 0):+.1f}%" if kpis.get("profit_prev", 0) > 0 else None,
    )
    k3.metric(
        "MARGIN",
        f"{kpis.get('margin', 0):.1f}%",
        delta=f"{kpis.get('margin', 0) - kpis.get('margin_prev', 0):+.1f}pp" if kpis.get("margin_prev", 0) > 0 else None,
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

# --- Daily chart + Signals ---
daily = daily_summary(df)

col_chart, col_signals = st.columns([3, 1])

with col_chart:
    st.markdown('<div class="section-header">DAILY REVENUE & CM3</div>', unsafe_allow_html=True)
    if not daily.empty:
        daily["date"] = pd.to_datetime(daily["date"])
        # Filter to current period only for charts
        from datetime import datetime, timedelta
        chart_cutoff = datetime.now() - timedelta(days=days)
        daily_chart = daily[daily["date"] >= chart_cutoff].copy()

        if not daily_chart.empty:
            y_cols = ["revenue_pln"]
            names = ["Revenue"]
            colors = [COLORS["revenue"]]
            if "cm3" in daily_chart.columns:
                y_cols.append("cm3")
                names.append("CM3")
                colors.append(COLORS["cm3"])
            if "revenue_pln_7d" in daily_chart.columns:
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
col_plat, col_top, col_bottom = st.columns(3)

with col_plat:
    st.markdown("**Platform Summary**")
    plat_df = platform_summary(df, platforms)
    if not plat_df.empty:
        display = plat_df[["platform", "revenue_pln", "cm3", "cm3_pct", "orders_count", "units"]].copy()
        display.columns = ["Platform", "Revenue", "CM3", "Margin%", "Orders", "Units"]
        display["Revenue"] = display["Revenue"].map(lambda x: f"{x:,.0f}")
        display["CM3"] = display["CM3"].map(lambda x: f"{x:,.0f}")
        display["Margin%"] = display["Margin%"].map(lambda x: f"{x:.1f}%")
        st.dataframe(display, use_container_width=True, hide_index=True)

with col_top:
    st.markdown("**Top 5 by CM3**")
    if not product_df.empty:
        top5 = product_df.head(5)[["sku", "cm3", "cm3_pct", "units"]].copy()
        top5.columns = ["SKU", "CM3", "Margin%", "Units"]
        top5["CM3"] = top5["CM3"].map(lambda x: f"{x:,.0f}")
        top5["Margin%"] = top5["Margin%"].map(lambda x: f"{x:.1f}%")
        st.dataframe(top5, use_container_width=True, hide_index=True)

with col_bottom:
    st.markdown("**Bottom 5 by CM3**")
    if not product_df.empty:
        bottom5 = product_df.tail(5)[["sku", "cm3", "cm3_pct", "units"]].copy()
        bottom5.columns = ["SKU", "CM3", "Margin%", "Units"]
        bottom5["CM3"] = bottom5["CM3"].map(lambda x: f"{x:,.0f}")
        bottom5["Margin%"] = bottom5["Margin%"].map(lambda x: f"{x:.1f}%")
        st.dataframe(bottom5, use_container_width=True, hide_index=True)

# --- Revenue by platform stacked area ---
st.markdown('<div class="section-header">REVENUE BY PLATFORM</div>', unsafe_allow_html=True)
if not df.empty:
    from datetime import datetime, timedelta
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
