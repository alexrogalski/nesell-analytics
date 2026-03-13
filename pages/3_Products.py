"""Products - SKU profitability, portfolio analysis."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms, load_products, load_cogs_gaps
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

# --- 5. COGS GAPS ---
st.markdown('<div class="section-header">COGS GAPS</div>', unsafe_allow_html=True)

if not prod_df.empty:
    with_cogs = prod_df[prod_df["cogs"] > 0]
    without_cogs = prod_df[(prod_df["cogs"] == 0) & (prod_df["revenue_pln"] > 0)]

    cov_rev = with_cogs["revenue_pln"].sum()
    total_rev = prod_df["revenue_pln"].sum()
    coverage_pct = (cov_rev / total_rev * 100) if total_rev > 0 else 0
    uncovered_rev = total_rev - cov_rev

    # Coverage KPIs
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Products with COGS", f"{len(with_cogs)} / {len(prod_df)}")
    c2.metric("Revenue Coverage", f"{coverage_pct:.1f}%")
    c3.metric("Revenue without COGS", f"{uncovered_rev:,.0f} PLN")
    if cov_rev > 0:
        real_margin = with_cogs["cm3"].sum() / cov_rev * 100
        c4.metric("Real Margin (COGS only)", f"{real_margin:.1f}%")
    else:
        c4.metric("Real Margin (COGS only)", "N/A")

    # Coverage status banner
    if coverage_pct < 90:
        st.markdown(f"""
        <div style="background: #1c1208; border: 1px solid #92400e; border-left: 4px solid #f59e0b;
                    border-radius: 6px; padding: 14px 18px; margin: 12px 0;">
            <div style="font-family: var(--font-mono); font-size: 0.65rem; text-transform: uppercase;
                        letter-spacing: 0.1em; color: #f59e0b; margin-bottom: 4px;">
                LOW COGS COVERAGE
            </div>
            <div style="font-family: var(--font-mono); font-size: 0.85rem; color: #fbbf24;">
                {coverage_pct:.0f}% of revenue has COGS data. Reported margins are overstated by ~{100 - coverage_pct:.0f}pp.
                Add product costs in Baselinker to get accurate P&L.
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="background: #0b1a12; border: 1px solid #065f46; border-left: 4px solid #10b981;
                    border-radius: 6px; padding: 14px 18px; margin: 12px 0;">
            <div style="font-family: var(--font-mono); font-size: 0.85rem; color: #34d399;">
                COGS coverage is good: {coverage_pct:.1f}% of revenue covered.
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Full COGS gaps table
    if not without_cogs.empty:
        # Classify source
        gap_df = without_cogs[["sku", "name", "source", "revenue_pln", "units", "orders_count"]].copy()
        gap_df["source_label"] = gap_df["source"].apply(
            lambda x: "Printful" if str(x).lower() in ["printful", "pft"]
            else ("Wholesale" if str(x).lower() in ["wholesale", "hurtownia"]
                  else "Unknown")
        )
        gap_df = gap_df.sort_values("revenue_pln", ascending=False)

        st.markdown(f"""
        <div style="font-family: var(--font-mono); font-size: 0.75rem; color: #94a3b8; margin: 8px 0 16px 0;">
            {len(gap_df)} products with revenue but no COGS. Total impact: {uncovered_rev:,.0f} PLN.
        </div>
        """, unsafe_allow_html=True)

        # Build HTML table with Baselinker links
        rows_html = ""
        for i, (_, row) in enumerate(gap_df.iterrows()):
            sku = row.get("sku", "unknown")
            name = str(row.get("name", ""))[:40]
            source = row.get("source_label", "Unknown")
            rev = row.get("revenue_pln", 0)
            units = int(row.get("units", 0))
            orders = int(row.get("orders_count", 0))
            bl_url = f"https://panel-f.baselinker.com/products.html?search={sku}"
            row_bg = "#111827" if i % 2 == 0 else "#0f1729"

            # Source badge color
            source_color = "#10b981" if source == "Printful" else ("#3b82f6" if source == "Wholesale" else "#64748b")

            rows_html += f"""
            <tr style="background: {row_bg}; border-bottom: 1px solid #1e293b;">
                <td style="padding: 7px 10px; font-family: monospace; font-size: 0.78rem; color: #e2e8f0;
                           white-space: nowrap; max-width: 220px; overflow: hidden; text-overflow: ellipsis;">{sku}</td>
                <td style="padding: 7px 10px; font-family: monospace; font-size: 0.75rem; color: #94a3b8;
                           max-width: 200px; overflow: hidden; text-overflow: ellipsis;">{name}</td>
                <td style="padding: 7px 10px; text-align: center;">
                    <span style="font-family: monospace; font-size: 0.65rem; color: {source_color};
                                 background: rgba({','.join(str(int(source_color.lstrip('#')[i:i+2], 16)) for i in (0,2,4))},0.1);
                                 padding: 2px 6px; border-radius: 3px;">{source}</span>
                </td>
                <td style="padding: 7px 10px; font-family: monospace; font-size: 0.78rem; text-align: right; color: #fbbf24;">{rev:,.0f}</td>
                <td style="padding: 7px 10px; font-family: monospace; font-size: 0.78rem; text-align: right; color: #94a3b8;">{units}</td>
                <td style="padding: 7px 10px; font-family: monospace; font-size: 0.78rem; text-align: right; color: #94a3b8;">{orders}</td>
                <td style="padding: 7px 10px; text-align: center;">
                    <a href="{bl_url}" target="_blank"
                       style="color: #3b82f6; font-family: monospace; font-size: 0.7rem; text-decoration: none;
                              background: rgba(59,130,246,0.08); padding: 3px 8px; border-radius: 3px;
                              border: 1px solid rgba(59,130,246,0.2);">
                        Edit &#8594;
                    </a>
                </td>
            </tr>
            """

        st.markdown(f"""
        <div style="overflow-x: auto; border-radius: 6px; border: 1px solid #1e293b; max-height: 500px; overflow-y: auto;">
            <table style="width: 100%; border-collapse: collapse; background: #111827;">
                <thead>
                    <tr style="border-bottom: 2px solid #1e293b; background: #0d1117; position: sticky; top: 0; z-index: 1;">
                        <th style="padding: 10px; text-align: left; font-family: monospace; font-size: 0.6rem;
                                   text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">SKU</th>
                        <th style="padding: 10px; text-align: left; font-family: monospace; font-size: 0.6rem;
                                   text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">Name</th>
                        <th style="padding: 10px; text-align: center; font-family: monospace; font-size: 0.6rem;
                                   text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">Source</th>
                        <th style="padding: 10px; text-align: right; font-family: monospace; font-size: 0.6rem;
                                   text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">Revenue (PLN)</th>
                        <th style="padding: 10px; text-align: right; font-family: monospace; font-size: 0.6rem;
                                   text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">Units</th>
                        <th style="padding: 10px; text-align: right; font-family: monospace; font-size: 0.6rem;
                                   text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">Orders</th>
                        <th style="padding: 10px; text-align: center; font-family: monospace; font-size: 0.6rem;
                                   text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">Action</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
        """, unsafe_allow_html=True)

        # Copyable SKU list for bulk operations
        with st.expander("Export SKU list (copy-paste)"):
            sku_list = "\n".join(gap_df["sku"].tolist())
            st.code(sku_list, language=None)
    else:
        st.markdown("""
        <div style="font-family: var(--font-mono); font-size: 0.85rem; color: #34d399; padding: 16px 0;">
            All products with revenue have COGS assigned.
        </div>
        """, unsafe_allow_html=True)
