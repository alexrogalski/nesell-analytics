"""P&L - Waterfall, contribution margins, fee decomposition."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms, load_products
from lib.metrics import calc_period_kpis, daily_summary, calc_contribution_margins, platform_summary
from lib.charts import waterfall_chart, multi_line

setup_page("P&L")

st.markdown('<div class="section-header">PROFIT & LOSS</div>', unsafe_allow_html=True)

# Sidebar
period_options = {7: "7D", 14: "14D", 30: "30D", 60: "60D", 90: "90D"}
days = st.sidebar.selectbox(
    "PERIOD", list(period_options.keys()), index=2,
    format_func=lambda x: period_options[x], key="pnl_period",
)

platforms = load_platforms()
df = load_daily_metrics(days=days * 2)

if df.empty:
    st.warning("No data available. Run ETL: python3.11 -m etl.run")
    st.stop()

df["platform"] = df["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))

# Platform filter
all_platforms = sorted(df["platform"].unique())
selected_platforms = st.sidebar.multiselect("PLATFORMS", all_platforms, default=all_platforms, key="pnl_plats")
df = df[df["platform"].isin(selected_platforms)]

if df.empty:
    st.warning("No data for selected filters.")
    st.stop()

# Current period data
now = datetime.now().date()
current_start = now - timedelta(days=days)
df["date_parsed"] = pd.to_datetime(df["date"]).dt.date
df_current = df[df["date_parsed"] >= current_start]

# --- 1. Waterfall Chart ---
st.markdown('<div class="section-header">P&L WATERFALL</div>', unsafe_allow_html=True)

total_revenue = df_current["revenue_pln"].sum()
total_cogs = df_current["cogs"].sum()
total_fees = df_current["fees"].sum()
total_shipping = df_current["shipping_cost"].sum() if "shipping_cost" in df_current.columns else 0
total_profit = df_current["profit"].sum()

cm1 = total_revenue - total_cogs
cm2 = cm1 - total_fees

labels = ["Revenue", "COGS", "CM1", "Fees", "CM2", "Shipping", "CM3"]
values = [total_revenue, -total_cogs, cm1, -total_fees, cm2, -total_shipping, total_profit]

# Adjust waterfall measures: CM1 and CM2 are intermediate totals
import plotly.graph_objects as go
fig_wf = go.Figure(go.Waterfall(
    x=labels,
    y=[total_revenue, -total_cogs, 0, -total_fees, 0, -total_shipping, 0],
    measure=["absolute", "relative", "total", "relative", "total", "relative", "total"],
    connector=dict(line=dict(color=COLORS["border"])),
    increasing=dict(marker=dict(color=COLORS["success"])),
    decreasing=dict(marker=dict(color=COLORS["danger"])),
    totals=dict(marker=dict(color=COLORS["primary"])),
    textposition="outside",
    text=[f"{total_revenue:,.0f}", f"-{total_cogs:,.0f}", f"{cm1:,.0f}",
          f"-{total_fees:,.0f}", f"{cm2:,.0f}", f"-{total_shipping:,.0f}", f"{total_profit:,.0f}"],
    textfont=dict(size=10),
))
fig_wf.update_layout(title="", height=420, showlegend=False)
st.plotly_chart(fig_wf, use_container_width=True)

# KPI summary under waterfall
w1, w2, w3, w4 = st.columns(4)
w1.metric("CM1 (Revenue - COGS)", f"{cm1:,.0f} PLN", delta=f"{cm1/total_revenue*100:.1f}% of rev" if total_revenue > 0 else None)
w2.metric("CM2 (CM1 - Fees)", f"{cm2:,.0f} PLN", delta=f"{cm2/total_revenue*100:.1f}% of rev" if total_revenue > 0 else None)
w3.metric("CM3 (Net Profit)", f"{total_profit:,.0f} PLN", delta=f"{total_profit/total_revenue*100:.1f}% of rev" if total_revenue > 0 else None)
w4.metric("COGS Coverage", f"{df_current[df_current['cogs']>0]['revenue_pln'].sum()/total_revenue*100:.0f}%" if total_revenue > 0 else "N/A")

# --- 2. Daily CM1/CM2/CM3 trend ---
st.markdown('<div class="section-header">DAILY CONTRIBUTION MARGINS</div>', unsafe_allow_html=True)

daily = daily_summary(df)
if not daily.empty:
    daily["date"] = pd.to_datetime(daily["date"])
    chart_cutoff = datetime.now() - timedelta(days=days)
    daily_chart = daily[daily["date"] >= chart_cutoff].copy()

    if not daily_chart.empty and all(c in daily_chart.columns for c in ["cm1", "cm2", "cm3"]):
        y_cols = ["cm1", "cm2", "cm3"]
        names = ["CM1 (Rev-COGS)", "CM2 (CM1-Fees)", "CM3 (Net)"]
        colors = [COLORS["cm1"], COLORS["cm2"], COLORS["cm3"]]

        # Add 7d MA for CM3
        if "cm3_7d" not in daily_chart.columns:
            daily_chart["cm3_7d"] = daily_chart["cm3"].rolling(7, min_periods=1).mean()
        y_cols.append("cm3_7d")
        names.append("CM3 7d MA")
        colors.append(COLORS["muted"])

        fig_cm = multi_line(daily_chart, "date", y_cols, colors=colors, names=names, height=380)
        st.plotly_chart(fig_cm, use_container_width=True)

# --- 3. Source split (Printful vs Resell) ---
st.markdown('<div class="section-header">SOURCE SPLIT: PRINTFUL vs RESELL</div>', unsafe_allow_html=True)

products_df = load_products()
if not products_df.empty and not df_current.empty:
    source_map = dict(zip(products_df["sku"], products_df["source"].fillna("unknown")))
    df_with_source = df_current.copy()
    df_with_source["source"] = df_with_source["sku"].map(source_map).fillna("unknown")

    # Classify: printful vs everything else
    df_with_source["source_group"] = df_with_source["source"].apply(
        lambda x: "Printful" if str(x).lower() in ["printful", "pft"] else "Resell/Other"
    )

    source_agg = df_with_source.groupby("source_group").agg({
        "revenue_pln": "sum", "cogs": "sum", "fees": "sum", "profit": "sum",
        "orders_count": "sum", "units": "sum",
    }).reset_index()
    source_agg = calc_contribution_margins(source_agg)

    s1, s2 = st.columns(2)
    for i, (_, row) in enumerate(source_agg.iterrows()):
        col = s1 if i == 0 else s2
        with col:
            st.markdown(f"**{row['source_group']}**")
            st.metric("Revenue", f"{row['revenue_pln']:,.0f} PLN")
            st.metric("CM1", f"{row['cm1']:,.0f} PLN ({row['cm1_pct']:.1f}%)")
            st.metric("CM3", f"{row['cm3']:,.0f} PLN ({row['cm3_pct']:.1f}%)")
            st.metric("Units", f"{int(row['units']):,}")

# --- 4. Fee decomposition by platform ---
st.markdown('<div class="section-header">FEE DECOMPOSITION BY PLATFORM</div>', unsafe_allow_html=True)

plat_summary = platform_summary(df_current, platforms)
if not plat_summary.empty:
    plat_fees = plat_summary[["platform", "fees", "cogs", "revenue_pln"]].copy()
    plat_fees["fees_pct"] = np.where(plat_fees["revenue_pln"] > 0, plat_fees["fees"] / plat_fees["revenue_pln"] * 100, 0)
    plat_fees = plat_fees.sort_values("fees", ascending=True)

    import plotly.graph_objects as go
    fig_fees = go.Figure()
    fig_fees.add_trace(go.Bar(
        y=plat_fees["platform"], x=plat_fees["fees"],
        name="Platform Fees", orientation="h", marker_color=COLORS["warning"],
        text=plat_fees["fees_pct"].map(lambda x: f"{x:.1f}%"), textposition="outside",
    ))
    fig_fees.update_layout(height=max(250, len(plat_fees) * 40), showlegend=False, title="")
    st.plotly_chart(fig_fees, use_container_width=True)

# --- 5. Period comparison table ---
st.markdown('<div class="section-header">PERIOD COMPARISON</div>', unsafe_allow_html=True)

kpis = calc_period_kpis(df, days)
if kpis:
    comp_data = {
        "Metric": ["Revenue", "COGS", "Fees", "Shipping", "Profit (CM3)", "Margin %", "ROI %", "Orders", "Units", "AOV"],
        f"Current {days}d": [
            f"{kpis['revenue']:,.0f}", f"{kpis['cogs']:,.0f}", f"{kpis['fees']:,.0f}",
            f"{kpis.get('shipping', 0):,.0f}",
            f"{kpis['profit']:,.0f}", f"{kpis['margin']:.1f}%", f"{kpis.get('roi', 0):.1f}%",
            f"{kpis['orders']:,}", f"{kpis['units']:,}", f"{kpis['aov']:,.0f}",
        ],
        f"Previous {days}d": [
            f"{kpis['revenue_prev']:,.0f}", f"{kpis['cogs_prev']:,.0f}", f"{kpis['fees_prev']:,.0f}",
            f"{kpis.get('shipping_prev', 0):,.0f}",
            f"{kpis['profit_prev']:,.0f}", f"{kpis['margin_prev']:.1f}%", f"{kpis.get('roi_prev', 0):.1f}%",
            f"{kpis['orders_prev']:,}", f"{kpis['units_prev']:,}", f"{kpis['aov_prev']:,.0f}",
        ],
        "Change %": [
            f"{kpis['revenue_delta']:+.1f}%", f"{kpis['cogs_delta']:+.1f}%", f"{kpis['fees_delta']:+.1f}%",
            f"{kpis.get('shipping_delta', 0):+.1f}%",
            f"{kpis['profit_delta']:+.1f}%",
            f"{kpis['margin'] - kpis['margin_prev']:+.1f}pp",
            f"{kpis.get('roi', 0) - kpis.get('roi_prev', 0):+.1f}pp",
            f"{kpis['orders_delta']:+.1f}%", f"{kpis['units_delta']:+.1f}%", f"{kpis['aov_delta']:+.1f}%",
        ],
    }
    st.dataframe(pd.DataFrame(comp_data), use_container_width=True, hide_index=True)
