"""Orders - Per-order profit breakdown inspired by Sellerboard."""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from lib.theme import setup_page, COLORS
from lib.data import load_orders_enriched

setup_page("Orders")

st.markdown(
    '<div class="section-header">ORDERS</div>',
    unsafe_allow_html=True,
)

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
st.sidebar.markdown(
    '<div class="section-header">FILTERS</div>',
    unsafe_allow_html=True,
)

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
status_options = (
    ["all"]
    + sorted([s for s in orders_df["status"].dropna().unique() if s])
)
selected_status = st.sidebar.selectbox(
    "STATUS", status_options, key="orders_status",
)

# Profit filter
profit_filter = st.sidebar.selectbox(
    "PROFIT", ["all", "profitable", "unprofitable"], key="orders_profit",
)

# Search
search_query = st.sidebar.text_input(
    "SEARCH (Order ID / SKU)", key="orders_search",
)

# Sort
sort_options = {
    "order_date": "Date",
    "profit_pln": "Profit",
    "revenue_pln": "Revenue",
    "margin_pct": "Margin %",
}
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
        '<div class="freshness-badge">Data through: '
        + latest_date
        + '</div>',
        unsafe_allow_html=True,
    )


# ============================================================
# HELPERS
# ============================================================
def _fmt(val, decimals=0):
    """Format number: 20982 -> '20,982'. Handles None/NaN."""
    if pd.isna(val):
        return "0"
    if decimals == 0:
        return f"{val:,.0f}"
    return f"{val:,.{decimals}f}"


def _fmt_pln(val):
    """Format PLN values: use K suffix for large numbers to prevent truncation."""
    if abs(val) >= 10000:
        return f"{val/1000:,.1f}K PLN"
    return f"{val:,.0f} PLN"


def _plat_badge_html(platform):
    """Return HTML span for a platform badge."""
    plat_color = PLATFORM_COLORS.get(platform, "#64748b")
    plat_label = (
        platform.replace("amazon_", "AMZ ").upper()
        if "amazon" in platform
        else platform.upper()
    )
    r, g, b = (int(plat_color.lstrip("#")[i:i+2], 16) for i in (0, 2, 4))
    return (
        '<span class="plat-badge" style="color: '
        + plat_color
        + "; background: rgba("
        + str(r) + "," + str(g) + "," + str(b)
        + ',0.12);">'
        + plat_label
        + "</span>"
    )


def _margin_pill_html(margin_val):
    """Return margin as a colored pill."""
    tier = "high" if margin_val > 30 else ("mid" if margin_val > 10 else "low")
    return (
        '<span class="margin-pill '
        + tier
        + '">'
        + f"{margin_val:.1f}%"
        + "</span>"
    )


# ============================================================
# KPI STRIP
# ============================================================
total_orders = len(filtered)
total_revenue = filtered["revenue_pln"].sum()
total_cogs = filtered["cogs_pln"].sum()
total_fees = filtered["fees_pln"].sum()
total_shipping = filtered["shipping_pln"].sum() if "shipping_pln" in filtered.columns else 0
total_profit = filtered["profit_pln"].sum()
avg_profit = total_profit / total_orders if total_orders > 0 else 0
# Revenue-weighted margin (Sellerboard-style): total profit / total revenue
# NOT simple mean of per-order margins, which would weight all orders equally
avg_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
# ROI = Net Profit / COGS (Sellerboard-style)
total_roi = (total_profit / total_cogs * 100) if total_cogs > 0 else 0
cancelled_count = len(
    filtered[filtered["status"].isin(["cancelled", "returned"])]
)
profitable_count = len(filtered[filtered["profit_pln"] > 0])
unprofitable_count = len(filtered[filtered["profit_pln"] <= 0])
cogs_coverage = (
    len(filtered[filtered["has_cogs"]]) / total_orders * 100
    if total_orders > 0 else 0
)

profit_cls = "profit-positive" if total_profit >= 0 else "profit-negative"
avg_profit_cls = "profit-positive" if avg_profit >= 0 else "profit-negative"

kpi_html = (
    '<div class="orders-kpi-strip">'
    # Orders
    + '<div class="orders-kpi-card accent-blue">'
    + '<div class="orders-kpi-label">Orders</div>'
    + '<div class="orders-kpi-value">' + _fmt(total_orders) + '</div>'
    + '<div class="orders-kpi-sub">'
    + str(period_map.get(days, "")) + ' period'
    + '</div>'
    + '</div>'
    # Revenue
    + '<div class="orders-kpi-card accent-blue">'
    + '<div class="orders-kpi-label">Revenue</div>'
    + '<div class="orders-kpi-value">' + _fmt_pln(total_revenue) + '</div>'
    + '<div class="orders-kpi-sub">avg '
    + _fmt(total_revenue / total_orders if total_orders else 0)
    + ' PLN/order</div>'
    + '</div>'
    # Total Profit
    + '<div class="orders-kpi-card accent-green">'
    + '<div class="orders-kpi-label">Total Profit</div>'
    + '<div class="orders-kpi-value ' + profit_cls + '">'
    + _fmt_pln(total_profit) + '</div>'
    + '<div class="orders-kpi-sub">'
    + 'COGS ' + _fmt(total_cogs) + ' + Fees ' + _fmt(total_fees) + ' + Ship ' + _fmt(total_shipping)
    + '</div>'
    + '</div>'
    # Avg Profit/Order
    + '<div class="orders-kpi-card accent-cyan">'
    + '<div class="orders-kpi-label">Avg Profit / Order</div>'
    + '<div class="orders-kpi-value ' + avg_profit_cls + '">'
    + _fmt_pln(avg_profit) + '</div>'
    + '<div class="orders-kpi-sub">COGS coverage '
    + f"{cogs_coverage:.0f}%"
    + '</div>'
    + '</div>'
    # Net Margin (revenue-weighted, Sellerboard-style)
    + '<div class="orders-kpi-card accent-purple">'
    + '<div class="orders-kpi-label">Net Margin</div>'
    + '<div class="orders-kpi-value">' + f"{avg_margin:.1f}%" + '</div>'
    + '<div class="orders-kpi-sub">ROI ' + f"{total_roi:.0f}%" + '</div>'
    + '</div>'
    # Cancelled
    + '<div class="orders-kpi-card accent-red">'
    + '<div class="orders-kpi-label">Cancelled / Returned</div>'
    + '<div class="orders-kpi-value">' + _fmt(cancelled_count) + '</div>'
    + '<div class="orders-kpi-sub">'
    + (f"{cancelled_count / total_orders * 100:.1f}%"
       if total_orders > 0 else "0%")
    + ' of total</div>'
    + '</div>'
    + '</div>'
)
st.markdown(kpi_html, unsafe_allow_html=True)

# --- Profitability summary strip ---
prof_pct = (
    f"{profitable_count / total_orders * 100:.0f}%"
    if total_orders > 0 else "0%"
)
unprof_pct = (
    f"{unprofitable_count / total_orders * 100:.0f}%"
    if total_orders > 0 else "0%"
)

summary_html = (
    '<div class="orders-profit-strip">'
    # Total Profit
    + '<div class="profit-summary-card">'
    + '<div class="icon green">&#9650;</div>'
    + '<div class="content">'
    + '<div class="label">Total Profit</div>'
    + '<div class="value" style="color: '
    + (COLORS["success"] if total_profit >= 0 else COLORS["danger"])
    + ';">' + _fmt(total_profit) + ' PLN</div>'
    + '</div></div>'
    # Profitable
    + '<div class="profit-summary-card">'
    + '<div class="icon blue">&#10003;</div>'
    + '<div class="content">'
    + '<div class="label">Profitable Orders</div>'
    + '<div class="value">' + _fmt(profitable_count) + '</div>'
    + '<div class="sub">' + prof_pct + ' of total</div>'
    + '</div></div>'
    # Unprofitable
    + '<div class="profit-summary-card">'
    + '<div class="icon red">&#10007;</div>'
    + '<div class="content">'
    + '<div class="label">Unprofitable Orders</div>'
    + '<div class="value">' + _fmt(unprofitable_count) + '</div>'
    + '<div class="sub">' + unprof_pct + ' of total</div>'
    + '</div></div>'
    + '</div>'
)
st.markdown(summary_html, unsafe_allow_html=True)


# ============================================================
# ORDERS TABLE (HTML)
# ============================================================
st.markdown(
    '<div class="section-header">ORDER LIST</div>',
    unsafe_allow_html=True,
)

# Pagination
PAGE_SIZE = 50
if "orders_page_count" not in st.session_state:
    st.session_state.orders_page_count = 1
visible_count = st.session_state.orders_page_count * PAGE_SIZE
visible = filtered.head(visible_count)

# Colgroup for fixed column widths
colgroup = (
    '<colgroup>'
    + '<col class="col-date"/>'
    + '<col class="col-orderid"/>'
    + '<col class="col-platform"/>'
    + '<col class="col-country"/>'
    + '<col class="col-items"/>'
    + '<col class="col-revenue"/>'
    + '<col class="col-cogs"/>'
    + '<col class="col-fees"/>'
    + '<col class="col-profit"/>'
    + '<col class="col-margin"/>'
    + '</colgroup>'
)

header_html = (
    '<thead><tr>'
    + '<th>Date</th>'
    + '<th>Order ID</th>'
    + '<th class="c">Platform</th>'
    + '<th class="c">Country</th>'
    + '<th>Items</th>'
    + '<th class="r">Revenue</th>'
    + '<th class="r">COGS</th>'
    + '<th class="r">Fees</th>'
    + '<th class="r">Profit</th>'
    + '<th class="r">Margin</th>'
    + '</tr></thead>'
)

# Build table rows
rows_html = ""
for _, row in visible.iterrows():
    order_date = str(row["order_date"])[:10]
    ext_id = str(row.get("external_id", ""))
    ext_id_short = ext_id[:22] + ".." if len(ext_id) > 22 else ext_id
    platform = str(row.get("platform_name", ""))
    country = str(row.get("shipping_country", "")).upper()[:2]
    country_label = COUNTRY_FLAGS.get(country, country) if country else "--"
    item_count = int(row.get("item_count", 0))
    unit_count = int(row.get("unit_count", 0))
    first_name = str(row.get("first_name", ""))[:35]
    revenue = float(row.get("revenue_pln", 0))
    cogs = float(row.get("cogs_pln", 0))
    fees = float(row.get("fees_pln", 0))
    profit = float(row.get("profit_pln", 0))
    margin = float(row.get("margin_pct", 0))
    has_cogs = bool(row.get("has_cogs", False))

    # COGS display
    if has_cogs:
        cogs_td = '<td class="r">' + _fmt(cogs) + '</td>'
    else:
        cogs_td = '<td class="r cogs-missing">n/a</td>'

    # Profit class
    profit_cls = "positive" if profit > 0 else "negative"

    # Items display
    items_text = (
        '<span class="items-count">'
        + str(unit_count)
        + 'x</span> '
        + first_name
    ) if unit_count > 0 else "0"

    rows_html += (
        '<tr>'
        + '<td>' + order_date + '</td>'
        + '<td title="' + ext_id + '">'
        + '<span style="color: #e2e8f0;">' + ext_id_short + '</span>'
        + '</td>'
        + '<td class="c">' + _plat_badge_html(platform) + '</td>'
        + '<td class="c"><span class="country-badge">'
        + country_label + '</span></td>'
        + '<td><span class="items-text">' + items_text + '</span></td>'
        + '<td class="r rev-cell">' + _fmt(revenue) + '</td>'
        + cogs_td
        + '<td class="r fees-cell">' + _fmt(fees) + '</td>'
        + '<td class="r profit-cell ' + profit_cls + '">'
        + _fmt(profit) + '</td>'
        + '<td class="r">' + _margin_pill_html(margin) + '</td>'
        + '</tr>'
    )

table_html = (
    '<div class="orders-table-wrap">'
    + '<table class="orders-table">'
    + colgroup
    + header_html
    + '<tbody>' + rows_html + '</tbody>'
    + '</table></div>'
)
st.markdown(table_html, unsafe_allow_html=True)

# Show more button + count
if visible_count < len(filtered):
    remaining = len(filtered) - visible_count
    show_label = f"Show more ({remaining:,} remaining)"
    if st.button(show_label, key="orders_show_more"):
        st.session_state.orders_page_count += 1
        st.rerun()

footer_html = (
    '<div class="orders-table-footer">'
    + '<span>Showing '
    + f"{min(visible_count, len(filtered)):,}"
    + ' of '
    + f"{len(filtered):,}"
    + ' orders</span>'
    + '<span>' + str(period_map.get(days, "")) + ' period</span>'
    + '</div>'
)
st.markdown(footer_html, unsafe_allow_html=True)


# ============================================================
# ORDER DETAILS (expandable)
# ============================================================
st.markdown(
    '<div class="section-header">ORDER DETAILS</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div style="font-family: var(--font-mono); font-size: 0.72rem;'
    ' color: #64748b; margin-bottom: 12px;">'
    'Expand an order to see line items, fee breakdown, and shipping details.'
    '</div>',
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
    cogs_val = float(row.get("cogs_pln", 0))
    fees_val = float(row.get("fees_pln", 0))
    profit_icon = "+" if profit > 0 else ""

    expander_label = (
        order_date
        + "  |  "
        + ext_id[:25]
        + "  |  "
        + platform
        + "  |  "
        + profit_icon
        + _fmt(profit)
        + " PLN"
    )
    with st.expander(expander_label, expanded=False):
        # Order summary - custom HTML grid
        p_color = COLORS["success"] if profit > 0 else COLORS["danger"]
        detail_html = (
            '<div class="order-detail-grid">'
            + '<div class="order-detail-cell">'
            + '<div class="label">Revenue</div>'
            + '<div class="value">' + _fmt(revenue) + ' PLN</div>'
            + '</div>'
            + '<div class="order-detail-cell">'
            + '<div class="label">COGS</div>'
            + '<div class="value" style="color: '
            + COLORS["danger"] + ';">'
            + _fmt(cogs_val) + ' PLN</div>'
            + '</div>'
            + '<div class="order-detail-cell">'
            + '<div class="label">Fees</div>'
            + '<div class="value" style="color: '
            + COLORS["warning"] + ';">'
            + _fmt(fees_val) + ' PLN</div>'
            + '</div>'
            + '<div class="order-detail-cell">'
            + '<div class="label">Profit</div>'
            + '<div class="value" style="color: '
            + p_color + '; font-weight: 700;">'
            + _fmt(profit) + ' PLN</div>'
            + '</div>'
            + '</div>'
        )
        st.markdown(detail_html, unsafe_allow_html=True)

        # Shipping info
        country = str(row.get("shipping_country", "--")).upper()
        shipping_cost = float(row.get("shipping_pln", 0))
        status = str(row.get("status", "--"))
        notes = str(row.get("notes", "")) if row.get("notes") else ""
        buyer = (
            str(row.get("buyer_email", ""))
            if row.get("buyer_email") else ""
        )

        meta_html = (
            '<div class="order-meta-row">'
            + '<div class="order-meta-item">'
            + '<span class="key">Country:</span>' + country
            + '</div>'
            + '<div class="order-meta-item">'
            + '<span class="key">Shipping:</span>'
            + _fmt(shipping_cost) + ' PLN'
            + '</div>'
            + '<div class="order-meta-item">'
            + '<span class="key">Status:</span>' + status
            + '</div>'
            + '<div class="order-meta-item">'
            + '<span class="key">Currency:</span>'
            + str(row.get("currency", ""))
            + '</div>'
        )
        if buyer:
            meta_html += (
                '<div class="order-meta-item">'
                + '<span class="key">Buyer:</span>' + buyer
                + '</div>'
            )
        meta_html += '</div>'
        if notes:
            meta_html += (
                '<div style="font-family: var(--font-mono);'
                ' font-size: 0.68rem; color: #64748b; margin-top: 4px;">'
                '<span style="color: #475569;">Notes:</span> '
                + notes
                + '</div>'
            )
        st.markdown(meta_html, unsafe_allow_html=True)

        # Line items table
        order_items = (
            items_df[items_df["order_id"] == order_id]
            if not items_df.empty
            else pd.DataFrame()
        )
        if not order_items.empty:
            li_header = (
                '<table class="line-items-table">'
                + '<thead><tr>'
                + '<th>SKU</th>'
                + '<th>Name</th>'
                + '<th class="r">Qty</th>'
                + '<th class="r">Unit Price</th>'
                + '<th class="r">Unit Cost</th>'
                + '<th class="r">Line Total</th>'
                + '</tr></thead><tbody>'
            )
            li_rows = ""
            for _, item in order_items.iterrows():
                sku = str(item.get("sku", ""))
                name = str(item.get("name", ""))[:45]
                qty = int(item.get("quantity", 1))
                u_price = float(item.get("unit_price_pln", 0))
                u_cost = float(item.get("unit_cost_pln", 0))
                line_total = u_price * qty
                cost_str = _fmt(u_cost) if u_cost > 0 else "?"
                cost_cls = "" if u_cost > 0 else ' class="cogs-missing"'
                li_rows += (
                    '<tr>'
                    + '<td class="primary" style="max-width: 160px;'
                    + ' overflow: hidden; text-overflow: ellipsis;'
                    + ' white-space: nowrap;">'
                    + sku + '</td>'
                    + '<td style="max-width: 200px; overflow: hidden;'
                    + ' text-overflow: ellipsis; white-space: nowrap;">'
                    + name + '</td>'
                    + '<td class="r">' + str(qty) + '</td>'
                    + '<td class="r">' + _fmt(u_price) + '</td>'
                    + '<td class="r"' + cost_cls + '>'
                    + cost_str + '</td>'
                    + '<td class="r primary">'
                    + _fmt(line_total) + '</td>'
                    + '</tr>'
                )
            li_table = li_header + li_rows + '</tbody></table>'
            st.markdown(li_table, unsafe_allow_html=True)
        else:
            st.markdown(
                '<div style="font-family: var(--font-mono);'
                ' font-size: 0.72rem; color: #475569;'
                ' padding: 8px 0;">'
                'No line items found for this order.</div>',
                unsafe_allow_html=True,
            )


# ============================================================
# PROFIT DISTRIBUTION CHART
# ============================================================
st.markdown(
    '<div class="section-header">PROFIT DISTRIBUTION</div>',
    unsafe_allow_html=True,
)

if len(filtered) > 5:
    profits = filtered["profit_pln"].dropna()
    profits = profits[
        profits.between(profits.quantile(0.02), profits.quantile(0.98))
    ]

    fig = go.Figure()
    pos_profits = profits[profits >= 0]
    neg_profits = profits[profits < 0]

    if len(neg_profits) > 0:
        fig.add_trace(go.Histogram(
            x=neg_profits, name="Loss",
            marker_color=COLORS["danger"],
            opacity=0.85,
            nbinsx=max(5, int(len(neg_profits) ** 0.5)),
        ))
    if len(pos_profits) > 0:
        fig.add_trace(go.Histogram(
            x=pos_profits, name="Profit",
            marker_color=COLORS["success"],
            opacity=0.85,
            nbinsx=max(5, int(len(pos_profits) ** 0.5)),
        ))

    fig.add_vline(
        x=0, line_dash="dot",
        line_color=COLORS["muted"], opacity=0.5,
    )
    median_profit = profits.median()
    fig.add_vline(
        x=median_profit, line_dash="dash",
        line_color=COLORS["primary"], opacity=0.7,
        annotation_text="Median: " + _fmt(median_profit) + " PLN",
        annotation_font=dict(size=10, color=COLORS["primary"]),
    )
    fig.update_layout(
        height=350,
        xaxis_title="Profit per order (PLN)",
        yaxis_title="Number of orders",
        barmode="overlay",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Stats below chart - custom HTML
    stats_html = (
        '<div class="dist-stats-row">'
        + '<div class="dist-stat">'
        + '<div class="label">Median Profit</div>'
        + '<div class="value">' + _fmt(median_profit) + ' PLN</div>'
        + '</div>'
        + '<div class="dist-stat">'
        + '<div class="label">Avg Profit</div>'
        + '<div class="value">' + _fmt(profits.mean()) + ' PLN</div>'
        + '</div>'
        + '<div class="dist-stat">'
        + '<div class="label">Min</div>'
        + '<div class="value" style="color: '
        + COLORS["danger"] + ';">'
        + _fmt(profits.min()) + ' PLN</div>'
        + '</div>'
        + '<div class="dist-stat">'
        + '<div class="label">Max</div>'
        + '<div class="value" style="color: '
        + COLORS["success"] + ';">'
        + _fmt(profits.max()) + ' PLN</div>'
        + '</div>'
        + '</div>'
    )
    st.markdown(stats_html, unsafe_allow_html=True)
else:
    st.markdown(
        '<div style="font-family: var(--font-mono);'
        ' font-size: 0.8rem; color: #64748b; padding: 16px 0;">'
        'Not enough orders for distribution chart (need at least 6).'
        '</div>',
        unsafe_allow_html=True,
    )
