"""Products - SKU profitability, portfolio analysis."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms, load_products, load_cogs_gaps, load_orders_enriched
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

# Merge product names, source, and image_url
if not products_df.empty and not prod_df.empty:
    merge_cols = ["sku", "name", "source", "cost_pln"]
    if "image_url" in products_df.columns:
        merge_cols.append("image_url")
    prod_df = prod_df.merge(
        products_df[merge_cols],
        on="sku", how="left",
    )
    prod_df["source"] = prod_df["source"].fillna("unknown")
    prod_df["name"] = prod_df["name"].fillna("")
    if "image_url" not in prod_df.columns:
        prod_df["image_url"] = None
else:
    prod_df["name"] = ""
    prod_df["source"] = "unknown"
    prod_df["cost_pln"] = 0
    prod_df["image_url"] = None

# --- 1. Top products table ---
st.markdown('<div class="section-header">SKU PROFITABILITY TABLE</div>', unsafe_allow_html=True)

sort_options = {"cm3": "CM3 (Profit)", "revenue_pln": "Revenue", "units": "Units", "cm3_pct": "Margin %"}
sort_by = st.selectbox("Sort by", list(sort_options.keys()), format_func=lambda x: sort_options[x])
prod_sorted = prod_df.sort_values(sort_by, ascending=False).head(50)

# Build HTML table with product thumbnails
prof_rows_html = ""
for i, (_, row) in enumerate(prod_sorted.iterrows()):
    sku = row.get("sku", "")
    name = str(row.get("name", ""))[:40]
    source = row.get("source", "unknown")
    units = int(row.get("units", 0))
    avg_price = row.get("revenue_per_unit", 0)
    unit_cost = row.get("cost_per_unit", 0)
    revenue = row.get("revenue_pln", 0)
    cogs_val = row.get("cogs", 0)
    cm1 = row.get("cm1", 0)
    fees_val = row.get("fees", 0)
    cm3_val = row.get("cm3", 0)
    margin = row.get("cm3_pct", 0)
    img_url = row.get("image_url", None)
    row_bg = "#111827" if i % 2 == 0 else "#0f1729"

    # Thumbnail or placeholder
    if img_url and str(img_url) != "None" and str(img_url).startswith("http"):
        img_html = f'<img src="{img_url}" style="width:40px;height:40px;object-fit:cover;border-radius:4px;border:1px solid #1e293b;" loading="lazy" />'
    else:
        img_html = '<div style="width:40px;height:40px;background:#1e293b;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:0.5rem;color:#475569;">N/A</div>'

    # Source badge color
    source_color = "#10b981" if source == "printful" else ("#3b82f6" if source == "wholesale" else "#64748b")

    # Margin color
    margin_color = "#10b981" if margin > 30 else ("#fbbf24" if margin > 10 else "#ef4444")

    source_rgba = ','.join(str(int(source_color.lstrip('#')[j:j+2], 16)) for j in (0, 2, 4))
    prof_rows_html += (
        f'<tr style="background: {row_bg}; border-bottom: 1px solid #1e293b;">'
        f'<td style="padding: 5px 8px; vertical-align: middle;">{img_html}</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.75rem; color: #e2e8f0; white-space: nowrap; max-width: 180px; overflow: hidden; text-overflow: ellipsis; vertical-align: middle;">{sku}</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.72rem; color: #94a3b8; max-width: 160px; overflow: hidden; text-overflow: ellipsis; vertical-align: middle;">{name}</td>'
        f'<td style="padding: 5px 8px; text-align: center; vertical-align: middle;">'
        f'<span style="font-family: monospace; font-size: 0.6rem; color: {source_color}; background: rgba({source_rgba},0.1); padding: 2px 5px; border-radius: 3px;">{source}</span>'
        f'</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.75rem; text-align: right; color: #94a3b8; vertical-align: middle;">{units}</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.75rem; text-align: right; color: #94a3b8; vertical-align: middle;">{avg_price:,.0f}</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.75rem; text-align: right; color: #94a3b8; vertical-align: middle;">{unit_cost:,.0f}</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.75rem; text-align: right; color: #e2e8f0; vertical-align: middle;">{revenue:,.0f}</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.75rem; text-align: right; color: #ef4444; vertical-align: middle;">{cogs_val:,.0f}</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.75rem; text-align: right; color: #fbbf24; vertical-align: middle;">{fees_val:,.0f}</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.75rem; text-align: right; color: #10b981; font-weight: 600; vertical-align: middle;">{cm3_val:,.0f}</td>'
        f'<td style="padding: 5px 8px; font-family: monospace; font-size: 0.75rem; text-align: right; color: {margin_color}; font-weight: 600; vertical-align: middle;">{margin:.1f}%</td>'
        f'</tr>'
    )

th_style = 'padding: 8px; text-align: left; font-family: monospace; font-size: 0.58rem; text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;'
th_r = th_style.replace("text-align: left", "text-align: right")
th_c = th_style.replace("text-align: left", "text-align: center")

prof_table_inner = (
    '<table style="width: 100%; border-collapse: collapse; background: #111827;">'
    '<thead><tr style="border-bottom: 2px solid #1e293b; background: #0d1117; position: sticky; top: 0; z-index: 1;">'
    f'<th style="{th_style} width: 50px;"></th>'
    f'<th style="{th_style}">SKU</th>'
    f'<th style="{th_style}">Name</th>'
    f'<th style="{th_c}">Source</th>'
    f'<th style="{th_r}">Units</th>'
    f'<th style="{th_r}">Avg Price</th>'
    f'<th style="{th_r}">Unit Cost</th>'
    f'<th style="{th_r}">Revenue</th>'
    f'<th style="{th_r}">COGS</th>'
    f'<th style="{th_r}">Fees</th>'
    f'<th style="{th_r}">CM3</th>'
    f'<th style="{th_r}">Margin%</th>'
    '</tr></thead>'
    f'<tbody>{prof_rows_html}</tbody>'
    '</table>'
)

_prof_row_count = len(prod_sorted)
_prof_table_height = min(550, 42 + _prof_row_count * 45)
st.html(f'''<style>
html, body {{ margin: 0; padding: 0; background: transparent; overflow: hidden; }}
</style>
<div style="overflow-x: auto; border-radius: 6px; border: 1px solid #1e293b; max-height: {_prof_table_height}px; overflow-y: auto;">
{prof_table_inner}
</div>''')

# --- 1b. SKU Order Drill-Down ---
st.markdown('<div class="section-header">SKU ORDER DRILL-DOWN</div>', unsafe_allow_html=True)

# Build SKU list from the profitability table (sorted by revenue)
_sku_list = prod_df.sort_values("revenue_pln", ascending=False)["sku"].tolist()
_sku_labels = []
for _s in _sku_list:
    _row = prod_df[prod_df["sku"] == _s].iloc[0]
    _label_name = str(_row.get("name", ""))[:30]
    _label_rev = _row.get("revenue_pln", 0)
    _sku_labels.append(f"{_s}  |  {_label_name}  |  {_label_rev:,.0f} PLN")

selected_sku_idx = st.selectbox(
    "Select SKU to view orders",
    range(len(_sku_list)),
    format_func=lambda i: _sku_labels[i],
    index=None,
    placeholder="Choose a SKU to drill down into its orders...",
    key="sku_drilldown",
)

if selected_sku_idx is not None:
    selected_sku = _sku_list[selected_sku_idx]

    with st.spinner(f"Loading orders for {selected_sku}..."):
        _orders_result = load_orders_enriched(days=days)

    if isinstance(_orders_result, tuple) and len(_orders_result) == 2:
        _ord_df, _items_df = _orders_result
    else:
        _ord_df, _items_df = pd.DataFrame(), pd.DataFrame()

    if not _items_df.empty and not _ord_df.empty:
        # Filter items for the selected SKU
        _sku_items = _items_df[_items_df["sku"] == selected_sku].copy()

        if not _sku_items.empty:
            # Get order IDs that contain this SKU
            _sku_order_ids = _sku_items["order_id"].unique()
            _sku_orders = _ord_df[_ord_df["id"].isin(_sku_order_ids)].copy()

            # Merge item-level data (quantity, unit_price, cost for THIS sku) into orders
            _item_agg = _sku_items.groupby("order_id").agg(
                sku_qty=("quantity", "sum"),
                sku_unit_price_pln=("unit_price_pln", "first"),
                sku_unit_cost_pln=("unit_cost_pln", "first"),
                sku_revenue_pln=("line_revenue_pln", "sum"),
                sku_cogs_pln=("line_cost_pln", "sum"),
            ).reset_index()

            _sku_orders = _sku_orders.merge(_item_agg, left_on="id", right_on="order_id", how="left", suffixes=("", "_sku"))

            # Calculate per-SKU profit and margin within each order
            # Allocate fees proportionally: (sku_revenue / order_revenue) * order_fees
            _sku_orders["fee_share"] = np.where(
                _sku_orders["revenue_pln"] > 0,
                (_sku_orders["sku_revenue_pln"] / _sku_orders["revenue_pln"]) * _sku_orders["fees_pln"],
                0,
            )
            _sku_orders["sku_profit"] = _sku_orders["sku_revenue_pln"] - _sku_orders["sku_cogs_pln"] - _sku_orders["fee_share"]
            _sku_orders["sku_margin"] = np.where(
                _sku_orders["sku_revenue_pln"] > 0,
                _sku_orders["sku_profit"] / _sku_orders["sku_revenue_pln"] * 100,
                0,
            )

            # Summary stats
            _total_orders = len(_sku_orders)
            _total_units = int(_sku_orders["sku_qty"].sum())
            _total_revenue = _sku_orders["sku_revenue_pln"].sum()
            _total_cogs = _sku_orders["sku_cogs_pln"].sum()
            _total_fees = _sku_orders["fee_share"].sum()
            _total_profit = _sku_orders["sku_profit"].sum()
            _avg_margin = (_total_profit / _total_revenue * 100) if _total_revenue > 0 else 0

            # Display summary KPIs
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Orders", f"{_total_orders}")
            k2.metric("Units Sold", f"{_total_units}")
            k3.metric("Revenue", f"{_total_revenue:,.0f} PLN")
            k4.metric("Profit", f"{_total_profit:,.0f} PLN")
            k5.metric("Avg Margin", f"{_avg_margin:.1f}%")

            # Build display table
            _display = _sku_orders[["order_date", "platform_order_id", "platform_name", "fulfillment",
                                     "sku_qty", "sku_unit_price_pln", "sku_revenue_pln",
                                     "sku_cogs_pln", "fee_share", "sku_profit", "sku_margin"]].copy()
            _display.columns = ["Date", "Order ID", "Marketplace", "Channel",
                                "Qty", "Unit Price", "Revenue",
                                "COGS", "Fees", "Profit", "Margin %"]
            _display["Date"] = pd.to_datetime(_display["Date"]).dt.strftime("%Y-%m-%d")
            _display = _display.sort_values("Date", ascending=False).reset_index(drop=True)

            # Format numeric columns
            for _col in ["Unit Price", "Revenue", "COGS", "Fees", "Profit"]:
                _display[_col] = _display[_col].map(lambda x: f"{x:,.2f}")
            _display["Margin %"] = _display["Margin %"].map(lambda x: f"{x:.1f}%")

            with st.expander(f"All {_total_orders} orders for {selected_sku}", expanded=True):
                st.dataframe(_display, use_container_width=True, hide_index=True, height=min(400, 38 + _total_orders * 35))
        else:
            st.info(f"No order items found for SKU: {selected_sku} in the selected {days}-day period.")
    else:
        st.warning("Could not load enriched order data. Make sure ETL has been run.")

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
        low_cogs_html = (
            '<div style="background: #1c1208; border: 1px solid #92400e; border-left: 4px solid #f59e0b; border-radius: 6px; padding: 14px 18px; margin: 12px 0;">'
            '<div style="font-family: var(--font-mono); font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #f59e0b; margin-bottom: 4px;">LOW COGS COVERAGE</div>'
            f'<div style="font-family: var(--font-mono); font-size: 0.85rem; color: #fbbf24;">{coverage_pct:.0f}% of revenue has COGS data. Reported margins are overstated by ~{100 - coverage_pct:.0f}pp. Add product costs in Baselinker to get accurate P&L.</div>'
            '</div>'
        )
        st.markdown(low_cogs_html, unsafe_allow_html=True)
    else:
        good_cogs_html = (
            '<div style="background: #0b1a12; border: 1px solid #065f46; border-left: 4px solid #10b981; border-radius: 6px; padding: 14px 18px; margin: 12px 0;">'
            f'<div style="font-family: var(--font-mono); font-size: 0.85rem; color: #34d399;">COGS coverage is good: {coverage_pct:.1f}% of revenue covered.</div>'
            '</div>'
        )
        st.markdown(good_cogs_html, unsafe_allow_html=True)

    # Full COGS gaps table
    if not without_cogs.empty:
        # Classify source
        gap_cols = ["sku", "name", "source", "revenue_pln", "units", "orders_count"]
        if "image_url" in without_cogs.columns:
            gap_cols.append("image_url")
        gap_df = without_cogs[gap_cols].copy()
        gap_df["source_label"] = gap_df["source"].apply(
            lambda x: "Printful" if str(x).lower() in ["printful", "pft"]
            else ("Wholesale" if str(x).lower() in ["wholesale", "hurtownia"]
                  else "Unknown")
        )
        gap_df = gap_df.sort_values("revenue_pln", ascending=False)

        gap_count_html = f'<div style="font-family: var(--font-mono); font-size: 0.75rem; color: #94a3b8; margin: 8px 0 16px 0;">{len(gap_df)} products with revenue but no COGS. Total impact: {uncovered_rev:,.0f} PLN.</div>'
        st.markdown(gap_count_html, unsafe_allow_html=True)

        # Build HTML table with Baselinker links and thumbnails
        rows_html = ""
        for i, (_, row) in enumerate(gap_df.iterrows()):
            sku = row.get("sku", "unknown")
            name = str(row.get("name", ""))[:40]
            source = row.get("source_label", "Unknown")
            rev = row.get("revenue_pln", 0)
            units = int(row.get("units", 0))
            orders = int(row.get("orders_count", 0))
            img_url = row.get("image_url", None)
            bl_url = f"https://panel-f.baselinker.com/products.html?search={sku}"
            row_bg = "#111827" if i % 2 == 0 else "#0f1729"

            # Thumbnail or placeholder
            if img_url and str(img_url) != "None" and str(img_url).startswith("http"):
                img_html = f'<img src="{img_url}" style="width:36px;height:36px;object-fit:cover;border-radius:4px;border:1px solid #1e293b;" loading="lazy" />'
            else:
                img_html = '<div style="width:36px;height:36px;background:#1e293b;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:0.45rem;color:#475569;">N/A</div>'

            # Source badge color
            source_color = "#10b981" if source == "Printful" else ("#3b82f6" if source == "Wholesale" else "#64748b")

            source_rgba = ','.join(str(int(source_color.lstrip('#')[j:j+2], 16)) for j in (0, 2, 4))
            rows_html += (
                f'<tr style="background: {row_bg}; border-bottom: 1px solid #1e293b;">'
                f'<td style="padding: 5px 8px; vertical-align: middle;">{img_html}</td>'
                f'<td style="padding: 7px 10px; font-family: monospace; font-size: 0.78rem; color: #e2e8f0; white-space: nowrap; max-width: 220px; overflow: hidden; text-overflow: ellipsis; vertical-align: middle;">{sku}</td>'
                f'<td style="padding: 7px 10px; font-family: monospace; font-size: 0.75rem; color: #94a3b8; max-width: 200px; overflow: hidden; text-overflow: ellipsis; vertical-align: middle;">{name}</td>'
                f'<td style="padding: 7px 10px; text-align: center; vertical-align: middle;">'
                f'<span style="font-family: monospace; font-size: 0.65rem; color: {source_color}; background: rgba({source_rgba},0.1); padding: 2px 6px; border-radius: 3px;">{source}</span>'
                f'</td>'
                f'<td style="padding: 7px 10px; font-family: monospace; font-size: 0.78rem; text-align: right; color: #fbbf24; vertical-align: middle;">{rev:,.0f}</td>'
                f'<td style="padding: 7px 10px; font-family: monospace; font-size: 0.78rem; text-align: right; color: #94a3b8; vertical-align: middle;">{units}</td>'
                f'<td style="padding: 7px 10px; font-family: monospace; font-size: 0.78rem; text-align: right; color: #94a3b8; vertical-align: middle;">{orders}</td>'
                f'<td style="padding: 7px 10px; text-align: center; vertical-align: middle;">'
                f'<a href="{bl_url}" target="_blank" style="color: #3b82f6; font-family: monospace; font-size: 0.7rem; text-decoration: none; background: rgba(59,130,246,0.08); padding: 3px 8px; border-radius: 3px; border: 1px solid rgba(59,130,246,0.2);">Edit &#8594;</a>'
                f'</td></tr>'
            )

        gap_th = 'padding: 10px; font-family: monospace; font-size: 0.6rem; text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;'
        gap_table_inner = (
            '<table style="width: 100%; border-collapse: collapse; background: #111827;">'
            '<thead><tr style="border-bottom: 2px solid #1e293b; background: #0d1117; position: sticky; top: 0; z-index: 1;">'
            f'<th style="{gap_th} text-align: left; width: 46px;"></th>'
            f'<th style="{gap_th} text-align: left;">SKU</th>'
            f'<th style="{gap_th} text-align: left;">Name</th>'
            f'<th style="{gap_th} text-align: center;">Source</th>'
            f'<th style="{gap_th} text-align: right;">Revenue (PLN)</th>'
            f'<th style="{gap_th} text-align: right;">Units</th>'
            f'<th style="{gap_th} text-align: right;">Orders</th>'
            f'<th style="{gap_th} text-align: center;">Action</th>'
            '</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            '</table>'
        )

        _gap_row_count = len(gap_df)
        _gap_table_height = min(500, 46 + _gap_row_count * 44)
        st.html(f'''<style>
html, body {{ margin: 0; padding: 0; background: transparent; overflow: hidden; }}
a {{ color: #3b82f6; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
</style>
<div style="overflow-x: auto; border-radius: 6px; border: 1px solid #1e293b; max-height: {_gap_table_height}px; overflow-y: auto;">
{gap_table_inner}
</div>''')

        # Copyable SKU list for bulk operations
        with st.expander("Export SKU list (copy-paste)"):
            sku_list = "\n".join(gap_df["sku"].tolist())
            st.code(sku_list, language=None)
    else:
        st.markdown('<div style="font-family: var(--font-mono); font-size: 0.85rem; color: #34d399; padding: 16px 0;">All products with revenue have COGS assigned.</div>', unsafe_allow_html=True)
