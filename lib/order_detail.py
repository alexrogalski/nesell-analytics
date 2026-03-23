"""Order detail expanders with waterfall chart and line items."""
import streamlit as st
import pandas as pd
from lib.theme import COLORS
from lib.order_table import _fmt, PLATFORM_COLORS


# Fee rate constants (must match lib/data.py)
_AMAZON_FBA_FEE_RATE = 0.3473
_AMAZON_FBM_FEE_RATE = 0.1545
_PLATFORM_FEE_RATES = {
    "allegro": 0.10, "temu": 0.0, "empik": 0.15,
}
_AMAZON_PLATFORMS = {
    "amazon_de", "amazon_fr", "amazon_it", "amazon_es",
    "amazon_nl", "amazon_se", "amazon_pl", "amazon_be", "amazon_gb",
}


def _waterfall_row(label, amount, pct_of_rev=None, color="#94a3b8",
                   is_subtotal=False, badge="", warn=""):
    """Build one row of the margin waterfall table."""
    amt_str = f"{amount:,.2f}"
    style = (
        'font-weight: 700; font-size: 0.82rem; border-top: 1px solid #334155;'
        if is_subtotal else ''
    )
    pct_html = ""
    if pct_of_rev is not None:
        pct_html = (
            '<span style="color: #64748b; font-size: 0.65rem; margin-left: 6px;">'
            + f"({pct_of_rev:+.1f}%)"
            + '</span>'
        )
    badge_html = ""
    if badge:
        badge_html = (
            '<span style="font-size: 0.58rem; padding: 1px 5px; border-radius: 3px;'
            ' margin-left: 6px; background: rgba(100,116,139,0.15); color: #94a3b8;">'
            + badge
            + '</span>'
        )
    warn_html = ""
    if warn:
        warn_html = (
            '<span style="font-size: 0.58rem; padding: 1px 5px; border-radius: 3px;'
            ' margin-left: 6px; background: rgba(245,158,11,0.12); color: #f59e0b;">'
            + warn
            + '</span>'
        )
    return (
        '<tr style="' + style + '">'
        + '<td style="padding: 6px 12px; font-family: var(--font-mono);'
        + ' font-size: 0.75rem; color: #94a3b8;">'
        + label + badge_html + warn_html + '</td>'
        + '<td style="padding: 6px 12px; font-family: var(--font-mono);'
        + ' font-size: 0.75rem; text-align: right; color: '
        + color + '; ' + style + '">'
        + amt_str + ' PLN' + pct_html + '</td>'
        + '</tr>'
    )


def render_order_details(visible, items_df, detail_limit=30):
    """Render expandable order detail sections for visible orders."""
    detail_limit = min(detail_limit, len(visible))

    for i in range(detail_limit):
        row = visible.iloc[i]
        order_id = int(row["id"])
        ext_id = str(row.get("external_id", ""))
        order_date = str(row["order_date"])[:10]
        platform = str(row.get("platform_display", row.get("platform_name", "")))
        platform_code = str(row.get("platform_name", ""))
        profit = float(row.get("profit_pln", 0))
        revenue = float(row.get("revenue_pln", 0))
        cogs_val = float(row.get("cogs_pln", 0))
        fees_val = float(row.get("fees_pln", 0))
        shipping_val = float(row.get("shipping_pln", 0))
        margin = float(row.get("margin_pct", 0))
        roi = float(row.get("roi_pct", 0))
        fulfillment = str(row.get("fulfillment", ""))
        has_cogs = bool(row.get("has_cogs", False))
        profit_icon = "+" if profit > 0 else ""

        # Determine fee source & rate
        actual_fee_pln = float(row.get("platform_fee_pln", 0))
        fee_is_actual = actual_fee_pln > 0
        if platform_code in _AMAZON_PLATFORMS:
            fee_rate = _AMAZON_FBM_FEE_RATE if fulfillment == "FBM" else _AMAZON_FBA_FEE_RATE
            fee_label = f"{fulfillment} {fee_rate * 100:.1f}%"
        else:
            fee_rate = _PLATFORM_FEE_RATES.get(platform_code, 0)
            fee_label = f"{fee_rate * 100:.0f}%"
        fee_source_badge = "actual" if fee_is_actual else "estimated"

        # Shipping source detection
        seller_ship = float(row.get("seller_shipping_cost_pln", 0))
        ship_is_seller_cost = seller_ship > 0
        ship_source_badge = "DPD invoice" if ship_is_seller_cost else "estimate"
        ship_warn = "" if ship_is_seller_cost or shipping_val == 0 else "no invoice data"

        expander_label = (
            order_date
            + "  |  "
            + ext_id[:25]
            + "  |  "
            + platform
            + ("  " + fulfillment if fulfillment else "")
            + "  |  "
            + profit_icon
            + _fmt(profit)
            + " PLN"
        )
        with st.expander(expander_label, expanded=False):

            # --- Margin waterfall table ---
            wf_rows = ""

            # Revenue
            wf_rows += _waterfall_row(
                "Revenue (buyer paid)", revenue, color="#e2e8f0",
            )

            # Platform fee
            fee_pct_of_rev = -(fees_val / revenue * 100) if revenue > 0 else 0
            wf_rows += _waterfall_row(
                "Platform Fee",
                -fees_val,
                pct_of_rev=fee_pct_of_rev,
                color=COLORS["warning"],
                badge=fee_label + " / " + fee_source_badge,
            )

            # COGS
            cogs_pct_of_rev = -(cogs_val / revenue * 100) if revenue > 0 else 0
            if has_cogs:
                wf_rows += _waterfall_row(
                    "COGS (product cost)",
                    -cogs_val,
                    pct_of_rev=cogs_pct_of_rev,
                    color=COLORS["danger"],
                )
            else:
                wf_rows += _waterfall_row(
                    "COGS (product cost)",
                    0,
                    color="#64748b",
                    warn="missing COGS data",
                )

            # Shipping
            if shipping_val > 0:
                ship_pct_of_rev = -(shipping_val / revenue * 100) if revenue > 0 else 0
                wf_rows += _waterfall_row(
                    "Shipping (seller cost)",
                    -shipping_val,
                    pct_of_rev=ship_pct_of_rev,
                    color="#06b6d4",
                    badge=ship_source_badge,
                    warn=ship_warn,
                )
            else:
                wf_rows += _waterfall_row(
                    "Shipping (seller cost)",
                    0,
                    color="#64748b",
                    badge="FBA" if fulfillment == "FBA" else "n/a",
                )

            # Separator + Profit
            p_color = COLORS["success"] if profit > 0 else COLORS["danger"]
            wf_rows += _waterfall_row(
                "= Net Profit",
                profit,
                color=p_color,
                is_subtotal=True,
            )

            waterfall_html = (
                '<table style="width: 100%; border-collapse: collapse;'
                ' background: rgba(15, 23, 41, 0.5); border: 1px solid #1a2332;'
                ' border-radius: 6px; overflow: hidden; margin-bottom: 12px;">'
                + '<thead><tr style="border-bottom: 1px solid #1e293b; background: #080c16;">'
                + '<th style="padding: 7px 12px; font-family: var(--font-mono);'
                + ' font-size: 0.55rem; text-transform: uppercase;'
                + ' letter-spacing: 0.06em; color: #64748b; text-align: left;">Component</th>'
                + '<th style="padding: 7px 12px; font-family: var(--font-mono);'
                + ' font-size: 0.55rem; text-transform: uppercase;'
                + ' letter-spacing: 0.06em; color: #64748b; text-align: right;">Amount</th>'
                + '</tr></thead>'
                + '<tbody>' + wf_rows + '</tbody>'
                + '</table>'
            )
            st.markdown(waterfall_html, unsafe_allow_html=True)

            # --- KPI pills: Margin + ROI ---
            margin_tier = "high" if margin > 30 else ("mid" if margin > 10 else "low")
            roi_tier = "high" if roi > 50 else ("mid" if roi > 0 else "low")
            margin_color = {"high": COLORS["success"], "mid": COLORS["warning"], "low": COLORS["danger"]}
            kpi_pills_html = (
                '<div style="display: flex; gap: 16px; margin-bottom: 12px;">'
                # Margin pill
                + '<div style="background: var(--bg-card); border: 1px solid #1e293b;'
                + ' border-radius: 6px; padding: 8px 16px; display: flex;'
                + ' align-items: center; gap: 10px;">'
                + '<span style="font-family: var(--font-mono); font-size: 0.6rem;'
                + ' text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">Margin</span>'
                + '<span style="font-family: var(--font-mono); font-size: 1.1rem;'
                + ' font-weight: 700; color: '
                + margin_color[margin_tier] + ';">'
                + f"{margin:.1f}%</span>"
                + '</div>'
                # ROI pill
                + '<div style="background: var(--bg-card); border: 1px solid #1e293b;'
                + ' border-radius: 6px; padding: 8px 16px; display: flex;'
                + ' align-items: center; gap: 10px;">'
                + '<span style="font-family: var(--font-mono); font-size: 0.6rem;'
                + ' text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">ROI</span>'
                + '<span style="font-family: var(--font-mono); font-size: 1.1rem;'
                + ' font-weight: 700; color: '
                + (margin_color[roi_tier]) + ';">'
                + (f"{roi:.0f}%" if has_cogs else "n/a")
                + '</span>'
                + '</div>'
                # Fulfillment badge
                + ('<div style="background: var(--bg-card); border: 1px solid #1e293b;'
                   + ' border-radius: 6px; padding: 8px 16px; display: flex;'
                   + ' align-items: center; gap: 10px;">'
                   + '<span style="font-family: var(--font-mono); font-size: 0.6rem;'
                   + ' text-transform: uppercase; letter-spacing: 0.08em; color: #64748b;">Fulfillment</span>'
                   + '<span style="font-family: var(--font-mono); font-size: 0.9rem;'
                   + ' font-weight: 600; color: '
                   + ('#3b82f6' if fulfillment == 'FBA' else '#10b981') + ';">'
                   + fulfillment
                   + '</span></div>'
                   if fulfillment else '')
                + '</div>'
            )
            st.markdown(kpi_pills_html, unsafe_allow_html=True)

            # --- Order meta info ---
            country = str(row.get("shipping_country", "--")).upper()
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

            # --- Line items table with per-item COGS + margin ---
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
                    + '<th class="r">Line Revenue</th>'
                    + '<th class="r">Line Cost</th>'
                    + '<th class="r">Item Margin</th>'
                    + '</tr></thead><tbody>'
                )
                li_rows = ""
                for _, item in order_items.iterrows():
                    sku = str(item.get("sku", ""))
                    name = str(item.get("name", ""))[:40]
                    qty = int(item.get("quantity", 1))
                    u_price = float(item.get("unit_price_pln", 0))
                    u_cost = float(item.get("unit_cost_pln", 0))
                    line_rev = float(item.get("line_revenue_pln", u_price * qty))
                    line_cost = float(item.get("line_cost_pln", u_cost * qty))
                    item_margin = line_rev - line_cost
                    cost_str = f"{u_cost:,.0f}" if u_cost > 0 else "?"
                    cost_cls = "" if u_cost > 0 else ' class="cogs-missing"'
                    margin_cls = (
                        ' style="color: ' + COLORS["success"] + ';"'
                        if item_margin > 0
                        else ' style="color: ' + COLORS["danger"] + ';"'
                    ) if u_cost > 0 else ' class="cogs-missing"'
                    margin_str = f"{item_margin:,.0f}" if u_cost > 0 else "?"
                    li_rows += (
                        '<tr>'
                        + '<td class="primary" style="max-width: 140px;'
                        + ' overflow: hidden; text-overflow: ellipsis;'
                        + ' white-space: nowrap;">'
                        + sku + '</td>'
                        + '<td style="max-width: 180px; overflow: hidden;'
                        + ' text-overflow: ellipsis; white-space: nowrap;">'
                        + name + '</td>'
                        + '<td class="r">' + str(qty) + '</td>'
                        + '<td class="r">' + f"{u_price:,.0f}" + '</td>'
                        + '<td class="r"' + cost_cls + '>'
                        + cost_str + '</td>'
                        + '<td class="r primary">'
                        + f"{line_rev:,.0f}" + '</td>'
                        + '<td class="r" style="color: '
                        + COLORS["danger"] + ';">'
                        + f"{line_cost:,.0f}" + '</td>'
                        + '<td class="r"' + margin_cls + '>'
                        + margin_str + '</td>'
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
