"""Orders - Per-order profit breakdown inspired by Sellerboard."""
import streamlit as st
import pandas as pd
from lib.theme import setup_page, COLORS
from lib.data import load_orders_enriched, load_amazon_returns
from lib.order_table import render_table_html, _fmt, _fmt_pln
from lib.order_detail import render_order_details
from lib.profit_distribution import render_profit_distribution

setup_page("Orders")

st.markdown(
    '<div class="section-header">ORDERS</div>',
    unsafe_allow_html=True,
)

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

# Load returns data and match to orders
returns_df = load_amazon_returns(days=days)
returned_order_ids = set()
if not returns_df.empty:
    returned_order_ids = set(returns_df["order_id"].dropna().unique())
    # Add refund flag to orders
    orders_df["has_return"] = orders_df["platform_order_id"].isin(returned_order_ids)
else:
    orders_df["has_return"] = False

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
# Helper: safe column sum (returns 0 if column missing)
# ============================================================
def _col_sum(df, col):
    """Sum a DataFrame column, returning 0 if it does not exist or is all NaN."""
    if col not in df.columns:
        return 0.0
    return pd.to_numeric(df[col], errors="coerce").fillna(0).sum()


# ============================================================
# KPI STRIP
# ============================================================
total_orders = len(filtered)
total_revenue_brutto = _col_sum(filtered, "revenue_pln")
total_vat = _col_sum(filtered, "vat_amount_pln")
total_revenue_net = _col_sum(filtered, "revenue_net_pln")
# Fallback: if revenue_net_pln column not yet populated, approximate from brutto
if total_revenue_net == 0 and total_revenue_brutto > 0:
    total_revenue_net = total_revenue_brutto - total_vat

total_cogs = _col_sum(filtered, "cogs_pln")
total_fees = _col_sum(filtered, "fees_pln")
total_shipping = _col_sum(filtered, "shipping_pln")
total_fulfillment = _col_sum(filtered, "fulfillment_cost_pln")
total_ppc = _col_sum(filtered, "ppc_cost_pln")
total_storage = _col_sum(filtered, "storage_fee_pln")
total_fx_spread = _col_sum(filtered, "fx_spread_pln")
total_all_costs = total_cogs + total_fees + total_shipping + total_fulfillment + total_ppc + total_storage + total_fx_spread
total_profit = _col_sum(filtered, "profit_pln")

avg_profit = total_profit / total_orders if total_orders > 0 else 0
# Net margin: profit / revenue netto (Sellerboard-style)
net_margin = (total_profit / total_revenue_net * 100) if total_revenue_net > 0 else 0
# ROI = Net Profit / COGS
total_roi = (total_profit / total_cogs * 100) if total_cogs > 0 else 0
cancelled_count = len(
    filtered[filtered["status"].isin(["cancelled", "returned"])]
)
refund_count = int(filtered["has_return"].sum()) if "has_return" in filtered.columns else 0
profitable_count = len(filtered[filtered["profit_pln"] > 0])
unprofitable_count = len(filtered[filtered["profit_pln"] <= 0])
cogs_coverage = (
    len(filtered[filtered["has_cogs"]]) / total_orders * 100
    if total_orders > 0 else 0
)

profit_cls = "profit-positive" if total_profit >= 0 else "profit-negative"
margin_cls = "profit-positive" if net_margin >= 0 else "profit-negative"

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
    # Revenue Netto
    + '<div class="orders-kpi-card accent-blue">'
    + '<div class="orders-kpi-label">Revenue Netto</div>'
    + '<div class="orders-kpi-value">' + _fmt_pln(total_revenue_net) + '</div>'
    + '<div class="orders-kpi-sub">VAT '
    + _fmt(total_vat) + ' PLN</div>'
    + '</div>'
    # Total Costs
    + '<div class="orders-kpi-card accent-yellow">'
    + '<div class="orders-kpi-label">Total Costs</div>'
    + '<div class="orders-kpi-value">' + _fmt_pln(total_all_costs) + '</div>'
    + '<div class="orders-kpi-sub">'
    + 'COGS ' + _fmt(total_cogs) + ' + Fees ' + _fmt(total_fees) + ' + Ship ' + _fmt(total_shipping)
    + '</div>'
    + '</div>'
    # Net Profit
    + '<div class="orders-kpi-card accent-green">'
    + '<div class="orders-kpi-label">Net Profit</div>'
    + '<div class="orders-kpi-value ' + profit_cls + '">'
    + _fmt_pln(total_profit) + '</div>'
    + '<div class="orders-kpi-sub">avg '
    + _fmt(avg_profit) + ' PLN/order</div>'
    + '</div>'
    # Net Margin
    + '<div class="orders-kpi-card accent-purple">'
    + '<div class="orders-kpi-label">Net Margin</div>'
    + '<div class="orders-kpi-value ' + margin_cls + '">'
    + f"{net_margin:.1f}%" + '</div>'
    + '<div class="orders-kpi-sub">ROI ' + f"{total_roi:.0f}%"
    + ' | COGS cov. ' + f"{cogs_coverage:.0f}%"
    + '</div>'
    + '</div>'
    # Cancelled / Refunded
    + '<div class="orders-kpi-card accent-red">'
    + '<div class="orders-kpi-label">Returns / Refunds</div>'
    + '<div class="orders-kpi-value">'
    + _fmt(cancelled_count) + ' / ' + _fmt(refund_count)
    + '</div>'
    + '<div class="orders-kpi-sub">'
    + (f"{(cancelled_count + refund_count) / total_orders * 100:.1f}%"
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
    + '<div class="label">Net Profit</div>'
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
# COST BREAKDOWN WATERFALL (Sellerboard-style)
# ============================================================
st.markdown(
    '<div class="section-header">COST BREAKDOWN</div>',
    unsafe_allow_html=True,
)


def _wf_row(label, amount, pct, row_class=""):
    """Build one waterfall table row."""
    cls = f' class="{row_class}"' if row_class else ''
    sign = "+" if amount > 0 else ""
    amt_str = f"{sign}{amount:,.0f} PLN" if row_class not in ("wf-subtotal", "wf-result") else f"{amount:,.0f} PLN"
    if row_class in ("wf-subtotal", "wf-result") and amount >= 0:
        amt_str = f"{amount:,.0f} PLN"
    elif row_class not in ("wf-subtotal", "wf-result"):
        amt_str = f"-{abs(amount):,.0f} PLN" if amount != 0 else "- PLN"
    pct_str = f"{pct:+.1f}%" if pct != 0 else "-"
    return (
        '<tr' + cls + '>'
        + '<td class="wf-label">' + label + '</td>'
        + '<td class="wf-amount">' + amt_str + '</td>'
        + '<td class="wf-pct">' + pct_str + '</td>'
        + '</tr>'
    )


# Build waterfall rows
# Use revenue netto as the 100% base for percentage calculations
_pct_base = total_revenue_net if total_revenue_net > 0 else 1  # avoid div by zero
_profit_row_cls = "wf-result wf-profit-pos" if total_profit >= 0 else "wf-result wf-profit-neg"

waterfall_rows = ""
# Revenue (brutto)
waterfall_rows += (
    '<tr class="wf-subtotal">'
    + '<td class="wf-label">Revenue (brutto)</td>'
    + '<td class="wf-amount">' + f"{total_revenue_brutto:,.0f} PLN" + '</td>'
    + '<td class="wf-pct">-</td>'
    + '</tr>'
)
# VAT
waterfall_rows += (
    '<tr class="wf-vat">'
    + '<td class="wf-label">- VAT</td>'
    + '<td class="wf-amount">' + f"-{abs(total_vat):,.0f} PLN" + '</td>'
    + '<td class="wf-pct">'
    + (f"-{abs(total_vat) / total_revenue_brutto * 100:.1f}%" if total_revenue_brutto > 0 else "-")
    + '</td>'
    + '</tr>'
)
# Revenue netto subtotal
waterfall_rows += (
    '<tr class="wf-subtotal">'
    + '<td class="wf-label">= Revenue (netto)</td>'
    + '<td class="wf-amount">' + f"{total_revenue_net:,.0f} PLN" + '</td>'
    + '<td class="wf-pct">100.0%</td>'
    + '</tr>'
)
# Cost rows (each as % of revenue netto)
_cost_lines = [
    ("COGS", total_cogs),
    ("Platform Fees", total_fees),
    ("Shipping (DPD)", total_shipping),
    ("3PL (Exportivo)", total_fulfillment),
    ("PPC Advertising", total_ppc),
    ("Storage Fees", total_storage),
    ("FX Spread", total_fx_spread),
]
for label, amount in _cost_lines:
    if amount == 0:
        # Show zero-value lines in muted style
        waterfall_rows += (
            '<tr class="wf-cost">'
            + '<td class="wf-label">' + '- ' + label + '</td>'
            + '<td class="wf-amount" style="color: #475569;">- PLN</td>'
            + '<td class="wf-pct" style="color: #475569;">-</td>'
            + '</tr>'
        )
    else:
        pct_val = amount / _pct_base * 100
        waterfall_rows += (
            '<tr class="wf-cost">'
            + '<td class="wf-label">' + '- ' + label + '</td>'
            + '<td class="wf-amount">' + f"-{abs(amount):,.0f} PLN" + '</td>'
            + '<td class="wf-pct">' + f"-{abs(pct_val):.1f}%" + '</td>'
            + '</tr>'
        )

# Net Profit result row
_profit_pct = total_profit / _pct_base * 100
waterfall_rows += (
    '<tr class="' + _profit_row_cls + '">'
    + '<td class="wf-label">= NET PROFIT</td>'
    + '<td class="wf-amount">' + f"{total_profit:,.0f} PLN" + '</td>'
    + '<td class="wf-pct">' + f"{_profit_pct:+.1f}%" + '</td>'
    + '</tr>'
)

waterfall_html = (
    '<div class="profit-waterfall-wrap">'
    + '<div class="profit-waterfall-title">Profit Waterfall - '
    + str(period_map.get(days, "")) + ' period</div>'
    + '<table class="profit-waterfall-table">'
    + waterfall_rows
    + '</table>'
    + '</div>'
)
st.markdown(waterfall_html, unsafe_allow_html=True)


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

table_html = render_table_html(visible, items_df)
st.markdown(table_html, unsafe_allow_html=True)

# CSV export for orders - includes new profit breakdown columns
_orders_export_cols = [
    "order_date", "external_id", "platform_name", "shipping_country",
    "unit_count", "first_name", "revenue_pln", "vat_amount_pln",
    "revenue_net_pln", "cogs_pln", "fees_pln", "shipping_pln",
    "fulfillment_cost_pln", "ppc_cost_pln", "storage_fee_pln",
    "fx_spread_pln", "profit_pln", "margin_pct",
]
_orders_export_cols = [c for c in _orders_export_cols if c in filtered.columns]
_orders_csv = filtered[_orders_export_cols].to_csv(index=False).encode("utf-8")
st.download_button("Download CSV", _orders_csv, "orders.csv", "text/csv", key="dl_orders")

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
# ORDER DETAILS (expandable) - Full Fee/Cost Breakdown
# ============================================================
st.markdown(
    '<div class="section-header">ORDER DETAILS</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div style="font-family: var(--font-mono); font-size: 0.72rem;'
    ' color: #64748b; margin-bottom: 12px;">'
    'Expand an order to see the full margin waterfall: revenue, platform fees, COGS, shipping, and profit.'
    '</div>',
    unsafe_allow_html=True,
)

render_order_details(visible, items_df)


# ============================================================
# PROFIT DISTRIBUTION CHART
# ============================================================
st.markdown(
    '<div class="section-header">PROFIT DISTRIBUTION</div>',
    unsafe_allow_html=True,
)

render_profit_distribution(filtered)
