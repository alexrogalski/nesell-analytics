"""Products - SKU profitability, portfolio analysis."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms, load_products
from lib.metrics import product_profitability, calc_contribution_margins
from lib.charts import scatter_quadrant, bar_chart

setup_page("Products")

st.markdown('<div class="section-header">PRODUCT INTELLIGENCE</div>', unsafe_allow_html=True)

# Sidebar
period_options = {7: "7D", 14: "14D", 30: "30D", 60: "60D", 90: "90D"}
days = st.sidebar.selectbox(
    "PERIOD", list(period_options.keys()), index=2,
    format_func=lambda x: period_options[x], key="prod_period",
)

platforms = load_platforms()
df = load_daily_metrics(days=days)
products_df = load_products()

if df.empty:
    st.warning("No data available. Run ETL: python3.11 -m etl.run")
    st.stop()

df["platform"] = df["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))

# Platform filter
all_platforms = sorted(df["platform"].unique())
selected_platforms = st.sidebar.multiselect("PLATFORMS", all_platforms, default=all_platforms, key="prod_plats")
df = df[df["platform"].isin(selected_platforms)]

if df.empty:
    st.warning("No data for selected filters.")
    st.stop()

# --- Product profitability ---
prod_df = product_profitability(df)

# Merge product names and source
if not products_df.empty and not prod_df.empty:
    prod_df = prod_df.merge(
        products_df[["sku", "name", "source", "cost_pln"]],
        on="sku", how="left",
    )
    prod_df["source"] = prod_df["source"].fillna("unknown")
    prod_df["name"] = prod_df["name"].fillna("")
else:
    prod_df["name"] = ""
    prod_df["source"] = "unknown"
    prod_df["cost_pln"] = 0

# --- 1. Top products table ---
st.markdown('<div class="section-header">SKU PROFITABILITY TABLE</div>', unsafe_allow_html=True)

sort_options = {"cm3": "CM3 (Profit)", "revenue_pln": "Revenue", "units": "Units", "cm3_pct": "Margin %"}
sort_by = st.selectbox("Sort by", list(sort_options.keys()), format_func=lambda x: sort_options[x])
prod_sorted = prod_df.sort_values(sort_by, ascending=False).head(50)

display_cols = prod_sorted[["sku", "name", "source", "units", "revenue_per_unit", "cost_per_unit",
                             "revenue_pln", "cogs", "cm1", "fees", "cm2", "cm3", "cm3_pct"]].copy()
display_cols.columns = ["SKU", "Name", "Source", "Units", "Avg Price", "Unit Cost",
                        "Revenue", "COGS", "CM1", "Fees", "CM2", "CM3", "Margin%"]

# Format numbers
for col in ["Avg Price", "Unit Cost", "Revenue", "COGS", "CM1", "Fees", "CM2", "CM3"]:
    display_cols[col] = display_cols[col].map(lambda x: f"{x:,.0f}")
display_cols["Margin%"] = display_cols["Margin%"].map(lambda x: f"{x:.1f}%")

st.dataframe(display_cols, use_container_width=True, hide_index=True, height=450)

# --- 2. Quadrant scatter ---
st.markdown('<div class="section-header">PORTFOLIO QUADRANT</div>', unsafe_allow_html=True)

scatter_data = prod_df[prod_df["units"] > 0].copy()
if len(scatter_data) > 3:
    fig_quad = scatter_quadrant(
        scatter_data, x="revenue_pln", y="cm3_pct", size="units",
        color="source", hover_name="sku",
        title="Revenue vs Margin (size = units sold)",
        height=500,
    )
    st.plotly_chart(fig_quad, use_container_width=True)

# --- 3. Pareto analysis ---
st.markdown('<div class="section-header">PARETO ANALYSIS (80/20 RULE)</div>', unsafe_allow_html=True)

if not prod_df.empty:
    pareto = prod_df.sort_values("revenue_pln", ascending=False).copy()
    pareto["cumulative_pct"] = pareto["revenue_pln"].cumsum() / pareto["revenue_pln"].sum() * 100
    pareto["product_rank"] = range(1, len(pareto) + 1)
    pareto["rank_pct"] = pareto["product_rank"] / len(pareto) * 100

    # Find 80% revenue point
    products_for_80 = pareto[pareto["cumulative_pct"] <= 80].shape[0]
    pct_products_80 = products_for_80 / len(pareto) * 100

    import plotly.graph_objects as go
    fig_pareto = go.Figure()
    fig_pareto.add_trace(go.Scatter(
        x=pareto["rank_pct"], y=pareto["cumulative_pct"],
        mode="lines", line=dict(color=COLORS["primary"], width=2),
        fill="tozeroy",
        fillcolor=f"rgba(59,130,246,0.1)",
        name="Cumulative Revenue %",
    ))
    # 80% line
    fig_pareto.add_hline(y=80, line_dash="dot", line_color=COLORS["warning"], opacity=0.7,
                         annotation_text="80% of revenue")
    fig_pareto.add_vline(x=pct_products_80, line_dash="dot", line_color=COLORS["warning"], opacity=0.7)
    fig_pareto.update_layout(
        height=350,
        xaxis_title="% of Products (ranked by revenue)",
        yaxis_title="Cumulative Revenue %",
    )
    st.plotly_chart(fig_pareto, use_container_width=True)
    st.markdown(
        f'<div style="font-family: monospace; font-size: 0.85rem; color: {COLORS["info"]};">'
        f'{products_for_80} products ({pct_products_80:.1f}%) generate 80% of revenue. '
        f'Total products: {len(pareto)}'
        f'</div>',
        unsafe_allow_html=True,
    )

# --- 4. Source breakdown ---
st.markdown('<div class="section-header">SOURCE BREAKDOWN</div>', unsafe_allow_html=True)

if not prod_df.empty:
    # Classify sources
    prod_df["source_group"] = prod_df["source"].apply(
        lambda x: "Printful" if str(x).lower() in ["printful", "pft"] else (
            "Wholesale" if str(x).lower() in ["wholesale", "hurtownia"] else "Other/Arbitrage"
        )
    )

    source_agg = prod_df.groupby("source_group").agg({
        "revenue_pln": "sum", "cogs": "sum", "fees": "sum", "cm3": "sum",
        "units": "sum", "sku": "nunique",
    }).reset_index()
    source_agg["margin"] = np.where(source_agg["revenue_pln"] > 0, source_agg["cm3"] / source_agg["revenue_pln"] * 100, 0)
    source_agg = source_agg.sort_values("revenue_pln", ascending=False)

    src_display = source_agg.copy()
    src_display.columns = ["Source", "Revenue", "COGS", "Fees", "CM3", "Units", "SKUs", "Margin%"]
    for col in ["Revenue", "COGS", "Fees", "CM3"]:
        src_display[col] = src_display[col].map(lambda x: f"{x:,.0f}")
    src_display["Margin%"] = src_display["Margin%"].map(lambda x: f"{x:.1f}%")

    st.dataframe(src_display, use_container_width=True, hide_index=True)

# --- 5. COGS coverage ---
st.markdown('<div class="section-header">COGS COVERAGE</div>', unsafe_allow_html=True)

if not prod_df.empty:
    with_cogs = prod_df[prod_df["cogs"] > 0]
    without_cogs = prod_df[prod_df["cogs"] == 0]

    cov_rev = with_cogs["revenue_pln"].sum()
    total_rev = prod_df["revenue_pln"].sum()
    coverage_pct = (cov_rev / total_rev * 100) if total_rev > 0 else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Products with COGS", f"{len(with_cogs)} / {len(prod_df)}")
    c2.metric("Revenue Coverage", f"{coverage_pct:.1f}%")
    c3.metric("Revenue without COGS", f"{without_cogs['revenue_pln'].sum():,.0f} PLN")

    if coverage_pct < 90:
        st.warning(
            f"COGS coverage is {coverage_pct:.0f}% of revenue. "
            f"Products without costs inflate margins. "
            f"Real margin (with COGS only): "
            f"{with_cogs['cm3'].sum() / cov_rev * 100:.1f}%" if cov_rev > 0 else ""
        )
    else:
        st.success(f"COGS coverage is good: {coverage_pct:.1f}% of revenue")
