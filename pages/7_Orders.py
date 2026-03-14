"""Orders - Per-order profit breakdown inspired by Sellerboard."""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from lib.theme import setup_page, COLORS
from lib.data import load_orders_enriched

setup_page("Orders")

st.markdown('<div class="section-header">ORDERS</div>', unsafe_allow_html=True)

# --- Country flag mapping ---
COUNTRY_FLAGS = {
    "DE": "DE", "FR": "FR", "IT": "IT", "ES": "ES",
    "NL": "NL", "SE": "SE", "PL": "PL", "BE": "BE",
    "GB": "GB", "UK": "GB", "US": "US", "AT": "AT",
    "CZ": "CZ", "DK": "DK", "FI": "FI", "IE": "IE",
    "PT": "PT", "NO": "NO", "CH": "CH", "LU": "LU",
    "HU": "HU", "RO": "RO", "SK": "SK", "SI": "SI",
    "HR": "HR", "BG": "BG", "EE": "EE", "LT": "LT",
    "LV": "LV", "GR": "GR", "CY": "CY", "MT": "MT",
}

# Platform badge colors
PLATFORM_COLORS = {
    "amazon_de": "#ff9900", "amazon_fr": "#ff9900", "amazon_it": "#ff9900",
    "amazon_es": "#ff9900", "amazon_nl": "#ff9900", "amazon_se": "#ff9900",
    "amazon_pl": "#ff9900", "amazon_be": "#ff9900", "amazon_gb": "#ff9900",
    "allegro": "#ff5a00", "temu": "#f74c00", "empik": "#00a2e8",
    "baselinker_other": "#64748b",
}

# --- Sidebar filters ---
st.sidebar.markdown('<div class="section-header">FILTERS</div>', unsafe_allow_html=True)

period_map = {7: "7D", 14: "14D", 30: "30D", 90: "90D"}
days = st.sidebar.selectbox(
    "PERIOD", list(period_map.keys()), index=2,
    format_func=lambda x: period_map[x], key="orders_period",
)

# Load data
result = load_orders_enriched(days=days)
if isinstance(result, tuple) and len(result) == 2:
    orders_df, items_df = result
else:
    orders_df, items_df = pd.DataFrame(), pd.DataFrame()

if orders_df.empty:
    st.warning("No order data available. Run ETL: python3.11 -m etl.run --orders --fba")
    st.stop()

# Platform filter
all_platforms = sorted(orders_df["platform_name"].unique())
selected_platforms = st.sidebar.multiselect(
    "PLATFORMS", all_platforms, default=all_platforms, key="orders_plats",
)

# Status filter
status_options = ["all"] + sorted([s for s in orders_df["status"].dropna().unique() if s])
selected_status = st.sidebar.selectbox("STATUS", status_options, key="orders_status")

# Profit filter
profit_filter = st.sidebar.selectbox(
    "PROFIT", ["all", "profitable", "unprofitable"], key="orders_profit",
)

# Search
search_query = st.sidebar.text_input("SEARCH (Order ID / SKU)", key="orders_search")

# Sort
sort_options = {"order_date": "Date", "profit_pln": "Profit", "revenue_pln": "Revenue", "margin_pct": "Margin %"}
sort_by = st.sidebar.selectbox(
    "SORT BY", list(sort_options.keys()),
    format_func=lambda x: sort_options[x], key="orders_sort",
)

# --- Apply filters ---
filtered = orders_df.copy()

# Platform
filtered = filtered[filtered["platform_name"].isin(selected_platforms)]

# Status
if selected_status != "all":
    filtered = filtered[filtered["status"] == selected_status]

# Profit
if profit_filter == "profitable":
    filtered = filtered[filtered["profit_pln"] > 0]
elif profit_filter == "unprofitable":
    filtered = filtered[filtered["profit_pln"] <= 0]

# Search
if search_query:
    q = search_query.lower()
    mask = (
        filtered["external_id"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["first_sku"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["first_name"].astype(str).str.lower().str.contains(q, na=False)
    )
    # Also search in items
    if not items_df.empty:
        matching_order_ids = items_df[
            items_df["sku"].astype(str).str.lower().str.contains(q, na=False)
            | items_df["name"].astype(str).str.lower().str.contains(q, na=False)
        ]["order_id"].unique()
        mask = mask | filtered["id"].isin(matching_order_ids)
    filtered = filtered[mask]

# Sort
sort_asc = sort_by == "order_date"
if sort_by == "order_date":
    sort_asc = False
filtered = filtered.sort_values(sort_by, ascending=sort_asc).reset_index(drop=True)

# Data freshness
if not filtered.empty:
    latest_date = pd.to_datetime(filtered["order_date"]).max().strftime("%Y-%m-%d")
    st.markdown(
        '<div class="freshness-badge">Data through: ' + latest_date + '</div>',
        unsafe_allow_html=True,
    )

# --- KPI Strip ---
total_orders = len(filtered)
total_revenue = filtered["revenue_pln"].sum()
total_cogs = filtered["cogs_pln"].sum()
total_fees = filtered["fees_pln"].sum()
total_profit = filtered["profit_pln"].sum()
avg_profit = total_profit / total_orders if total_orders > 0 else 0
avg_margin = filtered["margin_pct"].mean() if total_orders > 0 else 0
cancelled_count = len(filtered[filtered["status"].isin(["cancelled", "returned"])])
profitable_count = len(filtered[filtered["profit_pln"] > 0])
unprofitable_count = len(filtered[filtered["profit_pln"] <= 0])
cogs_coverage = len(filtered[filtered["has_cogs"]]) / total_orders * 100 if total_orders > 0 else 0

def _fmt_pln(val):
    """Format PLN values: use K suffix for large numbers to prevent truncation."""
    if abs(val) >= 10000:
        return f"{val/1000:,.1f}K PLN"
    return f"{val:,.0f} PLN"

# Use custom HTML KPI strip for reliable layout
kpi_data = [
    ("ORDERS", f"{total_orders:,}", "", "accent-blue"),
    ("REVENUE", _fmt_pln(total_revenue), "", "accent-blue"),
    ("TOTAL PROFIT", _fmt_pln(total_profit), f"margin {avg_margin:.1f}%", "accent-green" if total_profit > 0 else "accent-red"),
    ("AVG PROFIT/ORDER", _fmt_pln(avg_profit), f"COGS coverage {cogs_coverage:.0f}%", "accent-green" if avg_profit > 0 else "accent-red"),
    ("PROFITABLE", f"{profitable_count:,}", f"{profitable_count / total_orders * 100:.0f}% of orders" if total_orders > 0 else "", "accent-green"),
    ("UNPROFITABLE", f"{unprofitable_count:,}", f"{unprofitable_count / total_orders * 100:.0f}% of orders" if total_orders > 0 else "", "accent-red"),
]

kpi_html = '<div class="orders-kpi-strip">'
for label, value, sub, accent in kpi_data:
    profit_cls = ""
    if "PROFIT" in label and "UN" not in label:
        profit_cls = " profit-positive" if total_profit > 0 else " profit-negative"
    kpi_html += (
        f'<div class="orders-kpi-card {accent}">'
        f'<div class="orders-kpi-label">{label}</div>'
        f'<div class="orders-kpi-value{profit_cls}">{value}</div>'
    )
    if sub:
        kpi_html += f'<div class="orders-kpi-sub">{sub}</div>'
    kpi_html += '</div>'
kpi_html += '</div>'
st.markdown(kpi_html, unsafe_allow_html=True)

# --- Orders Table (HTML) ---
st.markdown('<div class="section-header">ORDER LIST</div>', unsafe_allow_html=True)

# Pagination
PAGE_SIZE = 50
if "orders_page_count" not in st.session_state:
    st.session_state.orders_page_count = 1
visible_count = st.session_state.orders_page_count * PAGE_SIZE
visible = filtered.head(visible_count)

# Build table header
th_style = 'padding: 8px 6px; text-align: left; font-family: monospace; font-size: 0.58rem; text-transform: uppercase; letter-spacing: 0.08em; color: #64748b; white-space: nowrap;'
th_r = th_style.replace("text-align: left", "text-align: right")
th_c = th_style.replace("text-align: left", "text-align: center")

header_html = (
    '<thead><tr style="border-bottom: 2px solid #1e293b; background: #0d1117; position: sticky; top: 0; z-index: 1;">'
    + '<th style="' + th_style + '">Date</th>'
    + '<th style="' + th_style + '">Order ID</th>'
    + '<th style="' + th_c + '">Platform</th>'
    + '<th style="' + th_c + '">Country</th>'
    + '<th style="' + th_style + '">Items</th>'
    + '<th style="' + th_r + '">Revenue</th>'
    + '<th style="' + th_r + '">COGS</th>'
    + '<th style="' + th_r + '">Fees</th>'
    + '<th style="' + th_r + '">Profit</th>'
    + '<th style="' + th_r + '">Margin</th>'
    + '</tr></thead>'
)

# Build table rows
rows_html = ""
for i, (_, row) in enumerate(visible.iterrows()):
    row_bg = "#111827" if i % 2 == 0 else "#0f1729"
    order_date = str(row["order_date"])[:10]
    ext_id = str(row.get("external_id", ""))
    ext_id_short = ext_id[:20] + "..." if len(ext_id) > 20 else ext_id
    platform = str(row.get("platform_name", ""))
    country = str(row.get("shipping_country", "")).upper()[:2]
    country_label = COUNTRY_FLAGS.get(country, country) if country else "--"
    item_count = int(row.get("item_count", 0))
    unit_count = int(row.get("unit_count", 0))
    first_name = str(row.get("first_name", ""))[:30]
    revenue = float(row.get("revenue_pln", 0))
    cogs = float(row.get("cogs_pln", 0))
    fees = float(row.get("fees_pln", 0))
    profit = float(row.get("profit_pln", 0))
    margin = float(row.get("margin_pct", 0))
    has_cogs = bool(row.get("has_cogs", False))

    # Colors
    profit_color = COLORS["success"] if profit > 0 else COLORS["danger"]
    margin_color = COLORS["success"] if margin > 30 else (COLORS["warning"] if margin > 10 else COLORS["danger"])
    cogs_display = f"{cogs:,.0f}" if has_cogs else "?"
    cogs_color = "#94a3b8" if has_cogs else COLORS["muted"]

    # Platform badge
    plat_color = PLATFORM_COLORS.get(platform, "#64748b")
    plat_label = platform.replace("amazon_", "AMZ ").upper() if "amazon" in platform else platform.upper()
    plat_rgb = ",".join(str(int(plat_color.lstrip("#")[j:j+2], 16)) for j in (0, 2, 4))

    # Items display
    items_text = f"{unit_count}x" if unit_count > 0 else "0"
    if first_name:
        items_text = items_text + " " + first_name

    rows_html += (
        '<tr style="background: ' + row_bg + '; border-bottom: 1px solid #1e293b;">'
        + '<td style="padding: 6px 6px; font-family: monospace; font-size: 0.72rem; color: #94a3b8; white-space: nowrap; vertical-align: middle;">' + order_date + '</td>'
        + '<td style="padding: 6px 6px; font-family: monospace; font-size: 0.72rem; color: #e2e8f0; white-space: nowrap; vertical-align: middle;" title="' + ext_id + '">' + ext_id_short + '</td>'
        + '<td style="padding: 6px 6px; text-align: center; vertical-align: middle;">'
        + '<span style="font-family: monospace; font-size: 0.58rem; color: ' + plat_color + '; background: rgba(' + plat_rgb + ',0.12); padding: 2px 6px; border-radius: 3px; white-space: nowrap;">' + plat_label + '</span>'
        + '</td>'
        + '<td style="padding: 6px 6px; font-family: monospace; font-size: 0.72rem; color: #94a3b8; text-align: center; vertical-align: middle;">' + country_label + '</td>'
        + '<td style="padding: 6px 6px; font-family: monospace; font-size: 0.68rem; color: #94a3b8; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: middle;">' + items_text + '</td>'
        + '<td style="padding: 6px 6px; font-family: monospace; font-size: 0.75rem; text-align: right; color: #e2e8f0; vertical-align: middle;">' + f"{revenue:,.0f}" + '</td>'
        + '<td style="padding: 6px 6px; font-family: monospace; font-size: 0.75rem; text-align: right; color: ' + cogs_color + '; vertical-align: middle;">' + cogs_display + '</td>'
        + '<td style="padding: 6px 6px; font-family: monospace; font-size: 0.75rem; text-align: right; color: ' + COLORS["warning"] + '; vertical-align: middle;">' + f"{fees:,.0f}" + '</td>'
        + '<td style="padding: 6px 6px; font-family: monospace; font-size: 0.75rem; text-align: right; color: ' + profit_color + '; font-weight: 600; vertical-align: middle;">' + f"{profit:,.0f}" + '</td>'
        + '<td style="padding: 6px 6px; font-family: monospace; font-size: 0.75rem; text-align: right; color: ' + margin_color + '; font-weight: 600; vertical-align: middle;">' + f"{margin:.1f}%" + '</td>'
        + '</tr>'
    )

table_html = (
    '<div style="overflow-x: auto; border-radius: 6px; border: 1px solid #1e293b; max-height: 650px; overflow-y: auto;">'
    + '<table style="width: 100%; border-collapse: collapse; background: #111827;">'
    + header_html
    + '<tbody>' + rows_html + '</tbody>'
    + '</table></div>'
)
st.markdown(table_html, unsafe_allow_html=True)

# Show more button
if visible_count < len(filtered):
    remaining = len(filtered) - visible_count
    show_label = f"Show more ({remaining:,} remaining)"
    if st.button(show_label, key="orders_show_more"):
        st.session_state.orders_page_count += 1
        st.rerun()

st.markdown(
    '<div style="font-family: monospace; font-size: 0.65rem; color: #475569; padding: 6px 0;">'
    + f"Showing {min(visible_count, len(filtered)):,} of {len(filtered):,} orders"
    + '</div>',
    unsafe_allow_html=True,
)

# --- Expandable Order Details ---
st.markdown('<div class="section-header">ORDER DETAILS</div>', unsafe_allow_html=True)
st.markdown(
    '<div style="font-family: monospace; font-size: 0.72rem; color: #64748b; margin-bottom: 12px;">'
    + 'Expand an order to see line items, fee breakdown, and shipping details.'
    + '</div>',
    unsafe_allow_html=True,
)

# Show details for first 30 visible orders (expanders)
detail_limit = min(30, len(visible))
for i in range(detail_limit):
    row = visible.iloc[i]
    order_id = int(row["id"])
    ext_id = str(row.get("external_id", ""))
    order_date = str(row["order_date"])[:10]
    platform = str(row.get("platform_display", row.get("platform_name", "")))
    profit = float(row.get("profit_pln", 0))
    revenue = float(row.get("revenue_pln", 0))
    profit_icon = "+" if profit > 0 else ""
    profit_color_label = "profit" if profit > 0 else "loss"

    expander_label = f"{order_date}  |  {ext_id[:25]}  |  {platform}  |  {profit_icon}{profit:,.0f} PLN"
    with st.expander(expander_label, expanded=False):
        # Order summary
        d1, d2, d3, d4 = st.columns(4)
        d1.markdown(
            '<div style="font-family: monospace; font-size: 0.65rem; color: #64748b; text-transform: uppercase;">Revenue</div>'
            + '<div style="font-family: monospace; font-size: 1rem; color: #e2e8f0;">' + f"{revenue:,.0f} PLN" + '</div>',
            unsafe_allow_html=True,
        )
        d2.markdown(
            '<div style="font-family: monospace; font-size: 0.65rem; color: #64748b; text-transform: uppercase;">COGS</div>'
            + '<div style="font-family: monospace; font-size: 1rem; color: ' + COLORS["danger"] + ';">' + f"{float(row.get('cogs_pln', 0)):,.0f} PLN" + '</div>',
            unsafe_allow_html=True,
        )
        d3.markdown(
            '<div style="font-family: monospace; font-size: 0.65rem; color: #64748b; text-transform: uppercase;">Fees</div>'
            + '<div style="font-family: monospace; font-size: 1rem; color: ' + COLORS["warning"] + ';">' + f"{float(row.get('fees_pln', 0)):,.0f} PLN" + '</div>',
            unsafe_allow_html=True,
        )
        profit_val = float(row.get("profit_pln", 0))
        p_color = COLORS["success"] if profit_val > 0 else COLORS["danger"]
        d4.markdown(
            '<div style="font-family: monospace; font-size: 0.65rem; color: #64748b; text-transform: uppercase;">Profit</div>'
            + '<div style="font-family: monospace; font-size: 1rem; color: ' + p_color + '; font-weight: 700;">' + f"{profit_val:,.0f} PLN" + '</div>',
            unsafe_allow_html=True,
        )

        # Shipping info
        country = str(row.get("shipping_country", "--")).upper()
        shipping_cost = float(row.get("shipping_pln", 0))
        status = str(row.get("status", "--"))
        notes = str(row.get("notes", "")) if row.get("notes") else ""
        buyer = str(row.get("buyer_email", "")) if row.get("buyer_email") else ""

        info_html = (
            '<div style="display: flex; gap: 24px; margin: 8px 0; flex-wrap: wrap;">'
            + '<div style="font-family: monospace; font-size: 0.72rem; color: #94a3b8;">'
            + '<span style="color: #64748b;">Country:</span> ' + country
            + '</div>'
            + '<div style="font-family: monospace; font-size: 0.72rem; color: #94a3b8;">'
            + '<span style="color: #64748b;">Shipping:</span> ' + f"{shipping_cost:,.0f} PLN"
            + '</div>'
            + '<div style="font-family: monospace; font-size: 0.72rem; color: #94a3b8;">'
            + '<span style="color: #64748b;">Status:</span> ' + status
            + '</div>'
            + '<div style="font-family: monospace; font-size: 0.72rem; color: #94a3b8;">'
            + '<span style="color: #64748b;">Currency:</span> ' + str(row.get("currency", ""))
            + '</div>'
        )
        if buyer:
            info_html += (
                '<div style="font-family: monospace; font-size: 0.72rem; color: #94a3b8;">'
                + '<span style="color: #64748b;">Buyer:</span> ' + buyer
                + '</div>'
            )
        info_html += '</div>'
        if notes:
            info_html += (
                '<div style="font-family: monospace; font-size: 0.68rem; color: #64748b; margin-top: 4px;">'
                + '<span style="color: #475569;">Notes:</span> ' + notes
                + '</div>'
            )
        st.markdown(info_html, unsafe_allow_html=True)

        # Line items table
        order_items = items_df[items_df["order_id"] == order_id] if not items_df.empty else pd.DataFrame()
        if not order_items.empty:
            li_header = (
                '<table style="width: 100%; border-collapse: collapse; background: #0d1117; margin-top: 8px; border-radius: 4px;">'
                + '<thead><tr style="border-bottom: 1px solid #1e293b;">'
                + '<th style="padding: 6px 8px; font-family: monospace; font-size: 0.55rem; text-transform: uppercase; color: #64748b; text-align: left;">SKU</th>'
                + '<th style="padding: 6px 8px; font-family: monospace; font-size: 0.55rem; text-transform: uppercase; color: #64748b; text-align: left;">Name</th>'
                + '<th style="padding: 6px 8px; font-family: monospace; font-size: 0.55rem; text-transform: uppercase; color: #64748b; text-align: right;">Qty</th>'
                + '<th style="padding: 6px 8px; font-family: monospace; font-size: 0.55rem; text-transform: uppercase; color: #64748b; text-align: right;">Unit Price</th>'
                + '<th style="padding: 6px 8px; font-family: monospace; font-size: 0.55rem; text-transform: uppercase; color: #64748b; text-align: right;">Unit Cost</th>'
                + '<th style="padding: 6px 8px; font-family: monospace; font-size: 0.55rem; text-transform: uppercase; color: #64748b; text-align: right;">Line Total</th>'
                + '</tr></thead><tbody>'
            )
            li_rows = ""
            for j, (_, item) in enumerate(order_items.iterrows()):
                item_bg = "#0d1117" if j % 2 == 0 else "#0a0e1a"
                sku = str(item.get("sku", ""))
                name = str(item.get("name", ""))[:45]
                qty = int(item.get("quantity", 1))
                u_price = float(item.get("unit_price_pln", 0))
                u_cost = float(item.get("unit_cost_pln", 0))
                line_total = u_price * qty
                cost_str = f"{u_cost:,.0f}" if u_cost > 0 else "?"
                cost_color = "#94a3b8" if u_cost > 0 else COLORS["muted"]
                li_rows += (
                    '<tr style="background: ' + item_bg + ';">'
                    + '<td style="padding: 5px 8px; font-family: monospace; font-size: 0.7rem; color: #e2e8f0; white-space: nowrap; max-width: 160px; overflow: hidden; text-overflow: ellipsis;">' + sku + '</td>'
                    + '<td style="padding: 5px 8px; font-family: monospace; font-size: 0.68rem; color: #94a3b8; max-width: 180px; overflow: hidden; text-overflow: ellipsis;">' + name + '</td>'
                    + '<td style="padding: 5px 8px; font-family: monospace; font-size: 0.7rem; color: #94a3b8; text-align: right;">' + str(qty) + '</td>'
                    + '<td style="padding: 5px 8px; font-family: monospace; font-size: 0.7rem; color: #94a3b8; text-align: right;">' + f"{u_price:,.0f}" + '</td>'
                    + '<td style="padding: 5px 8px; font-family: monospace; font-size: 0.7rem; color: ' + cost_color + '; text-align: right;">' + cost_str + '</td>'
                    + '<td style="padding: 5px 8px; font-family: monospace; font-size: 0.7rem; color: #e2e8f0; text-align: right;">' + f"{line_total:,.0f}" + '</td>'
                    + '</tr>'
                )
            li_table = li_header + li_rows + '</tbody></table>'
            st.markdown(li_table, unsafe_allow_html=True)
        else:
            st.markdown(
                '<div style="font-family: monospace; font-size: 0.72rem; color: #475569; padding: 8px 0;">No line items found for this order.</div>',
                unsafe_allow_html=True,
            )

# --- Profit Distribution Chart ---
st.markdown('<div class="section-header">PROFIT DISTRIBUTION</div>', unsafe_allow_html=True)

if len(filtered) > 5:
    profits = filtered["profit_pln"].dropna()
    profits = profits[profits.between(profits.quantile(0.02), profits.quantile(0.98))]

    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=profits,
        nbinsx=40,
        marker=dict(
            color=[COLORS["success"] if x >= 0 else COLORS["danger"] for x in np.linspace(profits.min(), profits.max(), 40)],
            line=dict(color=COLORS["border"], width=0.5),
        ),
        opacity=0.85,
        name="Orders",
    ))
    # Simpler approach: use two traces for pos/neg
    fig.data = []
    pos_profits = profits[profits >= 0]
    neg_profits = profits[profits < 0]
    if len(neg_profits) > 0:
        fig.add_trace(go.Histogram(
            x=neg_profits, name="Loss",
            marker_color=COLORS["danger"],
            opacity=0.8,
            nbinsx=max(5, int(len(neg_profits) ** 0.5)),
        ))
    if len(pos_profits) > 0:
        fig.add_trace(go.Histogram(
            x=pos_profits, name="Profit",
            marker_color=COLORS["success"],
            opacity=0.8,
            nbinsx=max(5, int(len(pos_profits) ** 0.5)),
        ))
    fig.add_vline(x=0, line_dash="dot", line_color=COLORS["muted"], opacity=0.6)
    median_profit = profits.median()
    fig.add_vline(
        x=median_profit, line_dash="dash", line_color=COLORS["primary"], opacity=0.7,
        annotation_text=f"Median: {median_profit:,.0f} PLN",
        annotation_font=dict(size=10, color=COLORS["primary"]),
    )
    fig.update_layout(
        height=350,
        xaxis_title="Profit per order (PLN)",
        yaxis_title="Number of orders",
        barmode="overlay",
        showlegend=True,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Stats below chart
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("MEDIAN PROFIT", f"{median_profit:,.0f} PLN")
    s2.metric("AVG PROFIT", f"{profits.mean():,.0f} PLN")
    s3.metric("MIN", f"{profits.min():,.0f} PLN")
    s4.metric("MAX", f"{profits.max():,.0f} PLN")
else:
    st.markdown(
        '<div style="font-family: monospace; font-size: 0.8rem; color: #64748b; padding: 16px 0;">Not enough orders for distribution chart (need at least 6).</div>',
        unsafe_allow_html=True,
    )
