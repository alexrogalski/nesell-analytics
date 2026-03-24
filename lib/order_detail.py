"""Order detail expanders: full fee/cost breakdown per order."""
import json
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

# EU VAT rates (must match lib/data.py)
EU_VAT_RATES = {
    "DE": 19, "FR": 20, "IT": 22, "ES": 21,
    "NL": 21, "SE": 25, "PL": 23, "BE": 21,
    "AT": 20, "GB": 20, "EE": 22, "FI": 25.5,
    "IE": 23, "PT": 23, "LU": 17, "CZ": 21,
}


def _wf_row(label, amount_pln, pct=None, color="#94a3b8",
            bold=False, badge="", warn="", indent=False, orig_amount=None, orig_ccy=None):
    """Build one waterfall row. All amounts shown with PLN suffix."""
    style_parts = []
    if bold:
        style_parts.append('font-weight: 700; border-top: 1px solid #334155;')
    td_style = ' '.join(style_parts)

    # Amount string with PLN
    if amount_pln == 0 and not bold:
        amt_html = '<span style="color: #475569;">0 PLN</span>'
    else:
        amt_html = f'{amount_pln:,.2f} <span style="font-size:0.6rem;color:#64748b">PLN</span>'

    # Original currency (shown in parentheses if different from PLN)
    orig_html = ""
    if orig_amount is not None and orig_ccy and orig_ccy != "PLN":
        orig_html = (
            f'<div style="font-size:0.6rem;color:#475569;margin-top:1px">'
            f'{orig_amount:,.2f} {orig_ccy}</div>'
        )

    # Percentage
    pct_html = ""
    if pct is not None and pct != 0:
        pct_html = (
            f'<span style="color:#64748b;font-size:0.62rem;margin-left:4px">'
            f'({pct:+.1f}%)</span>'
        )

    # Badge (e.g. "actual", "estimated", "FBA 34.7%")
    badge_html = ""
    if badge:
        badge_html = (
            f'<span style="font-size:0.55rem;padding:1px 5px;border-radius:3px;'
            f'margin-left:6px;background:rgba(100,116,139,0.15);color:#94a3b8">'
            f'{badge}</span>'
        )

    # Warning (e.g. "missing COGS")
    warn_html = ""
    if warn:
        warn_html = (
            f'<span style="font-size:0.55rem;padding:1px 5px;border-radius:3px;'
            f'margin-left:6px;background:rgba(245,158,11,0.12);color:#f59e0b">'
            f'{warn}</span>'
        )

    label_pad = "padding-left: 24px;" if indent else ""

    return (
        f'<tr style="{td_style}">'
        f'<td style="padding:5px 12px;font-family:var(--font-mono);'
        f'font-size:0.73rem;color:#94a3b8;{label_pad}">'
        f'{label}{badge_html}{warn_html}</td>'
        f'<td style="padding:5px 12px;font-family:var(--font-mono);'
        f'font-size:0.73rem;text-align:right;color:{color};{td_style}">'
        f'{amt_html}{pct_html}{orig_html}</td>'
        f'</tr>'
    )


def _section_header(text):
    """Small section divider inside the expander."""
    return (
        f'<tr><td colspan="2" style="padding:8px 12px 3px;font-family:var(--font-mono);'
        f'font-size:0.55rem;text-transform:uppercase;letter-spacing:0.08em;'
        f'color:#475569;border-top:1px solid #1e293b">{text}</td></tr>'
    )


def render_order_details(visible, items_df, detail_limit=30, auto_expand=None):
    """Render expandable order detail sections for visible orders."""
    detail_limit = min(detail_limit, len(visible))
    # Auto-expand if only 1 order (selected via selectbox)
    if auto_expand is None:
        auto_expand = detail_limit == 1

    for i in range(detail_limit):
        row = visible.iloc[i]
        order_id = int(row["id"])
        ext_id = str(row.get("external_id", ""))
        order_date = str(row["order_date"])[:10]
        platform = str(row.get("platform_display", row.get("platform_name", "")))
        platform_code = str(row.get("platform_name", ""))
        currency = str(row.get("currency", "EUR")).upper()
        country = str(row.get("shipping_country", "")).upper()[:2]
        fulfillment = str(row.get("fulfillment", ""))

        # Amounts
        revenue_brutto = float(row.get("revenue_pln", 0))
        vat_amount = float(row.get("vat_amount_pln", 0))
        revenue_net = float(row.get("revenue_net_pln", revenue_brutto - vat_amount))
        vat_rate = float(row.get("vat_rate", EU_VAT_RATES.get(country, 23) / 100))
        cogs_val = float(row.get("cogs_pln", 0))
        fees_val = float(row.get("fees_pln", 0))
        shipping_val = float(row.get("shipping_pln", 0))
        fulfillment_cost = float(row.get("fulfillment_cost_pln", 0))
        ppc_cost = float(row.get("ppc_cost_pln", 0))
        storage_fee = float(row.get("storage_fee_pln", 0))
        fx_spread = float(row.get("fx_spread_pln", 0))
        return_cost = float(row.get("return_cost_pln", 0))
        packaging_cost = float(row.get("packaging_cost_pln", 0))
        has_return = bool(row.get("has_return", False))
        profit = float(row.get("profit_pln", 0))
        margin = float(row.get("margin_pct", 0))
        roi = float(row.get("roi_pct", 0))
        has_cogs = bool(row.get("has_cogs", False))
        total_paid_orig = float(row.get("total_paid", 0))

        # Parse fee breakdown from notes (Amazon Finances API data)
        notes_str = str(row.get("notes", "")) if row.get("notes") else ""
        fee_breakdown = {}
        if notes_str and notes_str.startswith("{"):
            try:
                fee_breakdown = json.loads(notes_str)
            except (json.JSONDecodeError, ValueError):
                pass

        # Fee source detection
        actual_fee = float(row.get("platform_fee", 0))
        fee_is_actual = actual_fee > 0
        if platform_code in _AMAZON_PLATFORMS:
            fee_rate = _AMAZON_FBM_FEE_RATE if fulfillment == "FBM" else _AMAZON_FBA_FEE_RATE
            fee_label = f"{fulfillment} ~{fee_rate * 100:.1f}%"
        else:
            fee_rate = _PLATFORM_FEE_RATES.get(platform_code, 0)
            fee_label = f"{fee_rate * 100:.0f}%"
        fee_badge = "actual (Finances API)" if fee_is_actual else "estimated"

        # Shipping source
        seller_ship = float(row.get("seller_shipping_cost_pln", 0))
        ship_badge = "DPD actual" if seller_ship > 0 else ("DPD estimate" if shipping_val > 0 else "n/a")

        # pct base
        pct_base = revenue_net if revenue_net > 0 else 1

        # Expander label
        profit_icon = "+" if profit > 0 else ""
        expander_label = (
            f"{order_date}  |  {ext_id[:25]}  |  {platform}"
            + (f" {fulfillment}" if fulfillment else "")
            + f"  |  {profit_icon}{_fmt(profit)} PLN"
        )

        with st.expander(expander_label, expanded=auto_expand):

            wf = ""

            # === REVENUE SECTION ===
            wf += _section_header("Revenue")
            wf += _wf_row(
                "Revenue (brutto)", revenue_brutto, color="#e2e8f0",
                orig_amount=total_paid_orig, orig_ccy=currency,
            )
            vat_pct_label = EU_VAT_RATES.get(country, int(vat_rate * 100))
            wf += _wf_row(
                f"VAT ({country} {vat_pct_label}%)", -vat_amount,
                pct=-(vat_amount / revenue_brutto * 100) if revenue_brutto > 0 else 0,
                color="#94a3b8",
            )
            wf += _wf_row(
                "= Revenue (netto)", revenue_net, color="#e2e8f0", bold=True,
            )

            # === PLATFORM FEES SECTION ===
            wf += _section_header("Platform Fees")

            if fee_breakdown:
                # Detailed breakdown from Amazon Finances API
                commission = abs(float(fee_breakdown.get("commission", 0)))
                fba_fee = abs(float(fee_breakdown.get("fba_fee", 0)))
                other_fees = abs(float(fee_breakdown.get("other_fees", 0)))
                tax_amz = float(fee_breakdown.get("tax", 0))
                refund_amz = float(fee_breakdown.get("refund", 0))

                # Convert from original currency to PLN using the ratio
                if actual_fee > 0 and (commission + fba_fee + other_fees) > 0:
                    ratio = fees_val / (commission + fba_fee + other_fees)
                else:
                    ratio = 1.0

                if commission > 0:
                    wf += _wf_row(
                        "Referral Fee", -(commission * ratio),
                        pct=-(commission * ratio / pct_base * 100),
                        color=COLORS["warning"], indent=True,
                        badge="actual",
                        orig_amount=commission, orig_ccy=currency,
                    )
                if fba_fee > 0:
                    wf += _wf_row(
                        "FBA Fulfillment Fee", -(fba_fee * ratio),
                        pct=-(fba_fee * ratio / pct_base * 100),
                        color=COLORS["warning"], indent=True,
                        badge="actual",
                        orig_amount=fba_fee, orig_ccy=currency,
                    )
                if other_fees > 0:
                    wf += _wf_row(
                        "Other Amazon Fees", -(other_fees * ratio),
                        pct=-(other_fees * ratio / pct_base * 100),
                        color=COLORS["warning"], indent=True,
                        badge="DST, closing, etc.",
                        orig_amount=other_fees, orig_ccy=currency,
                    )
                if refund_amz < 0:
                    wf += _wf_row(
                        "Refund deduction", refund_amz * ratio,
                        color=COLORS["danger"], indent=True,
                        orig_amount=refund_amz, orig_ccy=currency,
                    )

                # Total fees line
                wf += _wf_row(
                    "= Total Platform Fees", -fees_val,
                    pct=-(fees_val / pct_base * 100),
                    color=COLORS["warning"], bold=True,
                    badge=fee_badge,
                )
            else:
                # No breakdown available, show single line
                wf += _wf_row(
                    "Platform Fee", -fees_val,
                    pct=-(fees_val / pct_base * 100),
                    color=COLORS["warning"],
                    badge=f"{fee_label} / {fee_badge}",
                )

            # === COGS SECTION ===
            wf += _section_header("Cost of Goods Sold")
            if has_cogs:
                wf += _wf_row(
                    "COGS (product cost)", -cogs_val,
                    pct=-(cogs_val / pct_base * 100),
                    color=COLORS["danger"],
                )
            else:
                wf += _wf_row(
                    "COGS (product cost)", 0,
                    color="#64748b", warn="missing - no cost data for this SKU",
                )

            # === SHIPPING & LOGISTICS ===
            wf += _section_header("Shipping & Logistics")
            if shipping_val > 0:
                # Platform-specific shipping labels
                _plat = platform_code or ""
                if fulfillment == "PRINTFUL":
                    ship_label, ship_badge_text = "Printful Shipping", "Printful dropship"
                elif _plat == "allegro":
                    ship_label, ship_badge_text = "Allegro Shipping", "Allegro Billing API"
                elif _plat == "temu":
                    ship_label, ship_badge_text = "Temu Shipping", "buyer delivery estimate"
                elif _plat == "empik":
                    ship_label, ship_badge_text = "Empik Shipping", "buyer delivery estimate"
                else:
                    ship_label, ship_badge_text = "DPD Shipping", ship_badge
                wf += _wf_row(
                    ship_label, -shipping_val,
                    pct=-(shipping_val / pct_base * 100),
                    color="#06b6d4", badge=ship_badge_text,
                )
            elif fulfillment == "FBA":
                wf += _wf_row(
                    "Shipping", 0, color="#64748b",
                    badge="included in FBA fee above",
                )
            elif fulfillment == "PRINTFUL":
                wf += _wf_row("Shipping", 0, color="#64748b", badge="Printful dropship")
            else:
                wf += _wf_row("Shipping", 0, color="#64748b", badge="n/a")

            if fulfillment_cost > 0:
                wf += _wf_row(
                    "3PL Exportivo", -fulfillment_cost,
                    pct=-(fulfillment_cost / pct_base * 100),
                    color="#06b6d4", badge="5 PLN/order",
                )

            if packaging_cost > 0:
                wf += _wf_row(
                    "Packaging", -packaging_cost,
                    pct=-(packaging_cost / pct_base * 100),
                    color="#06b6d4", badge="2 PLN/FBM order",
                )

            if has_return and return_cost > 0:
                wf += _wf_row(
                    "Return Processing", -return_cost,
                    pct=-(return_cost / pct_base * 100),
                    color=COLORS["danger"], badge="2.50 PLN Exportivo",
                )

            # === OTHER COSTS ===
            if ppc_cost > 0 or storage_fee > 0 or fx_spread > 0:
                wf += _section_header("Other Costs")
                if ppc_cost > 0:
                    wf += _wf_row(
                        "PPC Advertising", -ppc_cost,
                        pct=-(ppc_cost / pct_base * 100),
                        color="#a78bfa", badge="proportional allocation",
                    )
                if storage_fee > 0:
                    wf += _wf_row(
                        "FBA Storage Fee", -storage_fee,
                        pct=-(storage_fee / pct_base * 100),
                        color="#a78bfa", badge="monthly allocated",
                    )
                if fx_spread > 0:
                    wf += _wf_row(
                        "Amazon ACCS Spread", -fx_spread,
                        pct=-(fx_spread / pct_base * 100),
                        color="#a78bfa", badge="~1.2% estimate",
                    )

            # === RESULT ===
            p_color = COLORS["success"] if profit > 0 else COLORS["danger"]
            wf += _wf_row(
                "= NET PROFIT", profit,
                pct=(profit / pct_base * 100),
                color=p_color, bold=True,
            )

            # Render waterfall table
            waterfall_html = (
                '<table style="width:100%;border-collapse:collapse;'
                'background:rgba(15,23,41,0.5);border:1px solid #1a2332;'
                'border-radius:6px;overflow:hidden;margin-bottom:12px">'
                '<thead><tr style="border-bottom:1px solid #1e293b;background:#080c16">'
                '<th style="padding:7px 12px;font-family:var(--font-mono);'
                'font-size:0.55rem;text-transform:uppercase;'
                'letter-spacing:0.06em;color:#64748b;text-align:left">Component</th>'
                '<th style="padding:7px 12px;font-family:var(--font-mono);'
                'font-size:0.55rem;text-transform:uppercase;'
                'letter-spacing:0.06em;color:#64748b;text-align:right">Amount</th>'
                '</tr></thead>'
                '<tbody>' + wf + '</tbody>'
                '</table>'
            )
            st.markdown(waterfall_html, unsafe_allow_html=True)

            # --- KPI pills: Margin + ROI + Fulfillment ---
            margin_color = COLORS["success"] if margin > 10 else (COLORS["warning"] if margin > 0 else COLORS["danger"])
            roi_color = COLORS["success"] if roi > 50 else (COLORS["warning"] if roi > 0 else COLORS["danger"])
            kpi_html = (
                '<div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap">'
                + f'<div style="background:var(--bg-card);border:1px solid #1e293b;'
                  f'border-radius:6px;padding:6px 14px;display:flex;align-items:center;gap:8px">'
                  f'<span style="font-family:var(--font-mono);font-size:0.58rem;'
                  f'text-transform:uppercase;letter-spacing:0.08em;color:#64748b">Margin</span>'
                  f'<span style="font-family:var(--font-mono);font-size:1rem;'
                  f'font-weight:700;color:{margin_color}">{margin:.1f}%</span></div>'
                + f'<div style="background:var(--bg-card);border:1px solid #1e293b;'
                  f'border-radius:6px;padding:6px 14px;display:flex;align-items:center;gap:8px">'
                  f'<span style="font-family:var(--font-mono);font-size:0.58rem;'
                  f'text-transform:uppercase;letter-spacing:0.08em;color:#64748b">ROI</span>'
                  f'<span style="font-family:var(--font-mono);font-size:1rem;'
                  f'font-weight:700;color:{roi_color}">{"n/a" if not has_cogs else f"{roi:.0f}%"}</span></div>'
            )
            if fulfillment:
                ff_color = '#3b82f6' if fulfillment == 'FBA' else ('#a78bfa' if fulfillment == 'PRINTFUL' else '#10b981')
                kpi_html += (
                    f'<div style="background:var(--bg-card);border:1px solid #1e293b;'
                    f'border-radius:6px;padding:6px 14px;display:flex;align-items:center;gap:8px">'
                    f'<span style="font-family:var(--font-mono);font-size:0.58rem;'
                    f'text-transform:uppercase;letter-spacing:0.08em;color:#64748b">Fulfillment</span>'
                    f'<span style="font-family:var(--font-mono);font-size:0.85rem;'
                    f'font-weight:600;color:{ff_color}">{fulfillment}</span></div>'
                )
            kpi_html += '</div>'
            st.markdown(kpi_html, unsafe_allow_html=True)

            # --- Order meta ---
            status = str(row.get("status", "--"))
            buyer = str(row.get("buyer_email", "")) if row.get("buyer_email") else ""
            meta_html = (
                '<div class="order-meta-row">'
                + f'<div class="order-meta-item"><span class="key">Country:</span>{country}</div>'
                + f'<div class="order-meta-item"><span class="key">Status:</span>{status}</div>'
                + f'<div class="order-meta-item"><span class="key">Currency:</span>{currency}</div>'
                + f'<div class="order-meta-item"><span class="key">Original:</span>{total_paid_orig:,.2f} {currency}</div>'
            )
            if buyer:
                meta_html += f'<div class="order-meta-item"><span class="key">Buyer:</span>{buyer}</div>'
            meta_html += '</div>'
            st.markdown(meta_html, unsafe_allow_html=True)

            # --- Line items table ---
            order_items = (
                items_df[items_df["order_id"] == order_id]
                if not items_df.empty
                else pd.DataFrame()
            )
            if not order_items.empty:
                li_header = (
                    '<table class="line-items-table">'
                    '<thead><tr>'
                    '<th style="width:40px"></th>'
                    '<th>SKU</th><th>Name</th>'
                    '<th class="r">Qty</th>'
                    '<th class="r">Price</th>'
                    '<th class="r">Cost</th>'
                    '<th class="r">Revenue</th>'
                    '<th class="r">Cost</th>'
                    '<th class="r">Margin</th>'
                    '</tr></thead><tbody>'
                )
                li_rows = ""
                for _, item in order_items.iterrows():
                    sku = str(item.get("sku", ""))
                    name = str(item.get("name", ""))[:40]
                    qty = int(item.get("quantity", 1))
                    u_price = float(item.get("unit_price_pln", 0))
                    u_cost = float(item.get("unit_cost_pln", 0))
                    u_price_orig = float(item.get("unit_price", 0))
                    item_ccy = str(item.get("currency", currency)).upper()
                    line_rev = float(item.get("line_revenue_pln", u_price * qty))
                    line_cost = float(item.get("line_cost_pln", u_cost * qty))
                    item_margin = line_rev - line_cost

                    price_title = f"Orig: {u_price_orig:,.2f} {item_ccy}"
                    cost_str = f'{u_cost:,.0f} <span style="font-size:0.55rem;color:#64748b">PLN</span>' if u_cost > 0 else '<span class="cogs-missing">?</span>'
                    margin_color_i = COLORS["success"] if item_margin > 0 else COLORS["danger"]
                    margin_str = f'{item_margin:,.0f} <span style="font-size:0.55rem;color:#64748b">PLN</span>' if u_cost > 0 else '<span class="cogs-missing">?</span>'

                    img_url = str(item.get("image_url", "")) if item.get("image_url") else ""
                    img_td = (
                        f'<td style="width:36px;padding:3px"><img src="{img_url}" '
                        f'style="width:32px;height:32px;object-fit:cover;border-radius:4px;'
                        f'border:1px solid #1e293b" onerror="this.style.display=\'none\'"/></td>'
                    ) if img_url else '<td style="width:36px"></td>'

                    li_rows += (
                        '<tr>'
                        + img_td
                        + f'<td class="primary" style="max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{sku}</td>'
                        f'<td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{name}</td>'
                        f'<td class="r">{qty}</td>'
                        f'<td class="r" title="{price_title}">{u_price:,.0f} <span style="font-size:0.55rem;color:#64748b">PLN</span></td>'
                        f'<td class="r">{cost_str}</td>'
                        f'<td class="r primary">{line_rev:,.0f} <span style="font-size:0.55rem;color:#64748b">PLN</span></td>'
                        f'<td class="r" style="color:{COLORS["danger"]}">{line_cost:,.0f} <span style="font-size:0.55rem;color:#64748b">PLN</span></td>'
                        f'<td class="r" style="color:{margin_color_i}">{margin_str}</td>'
                        '</tr>'
                    )
                st.markdown(li_header + li_rows + '</tbody></table>', unsafe_allow_html=True)
            else:
                st.markdown(
                    '<div style="font-family:var(--font-mono);font-size:0.72rem;'
                    'color:#475569;padding:8px 0">'
                    'No line items found for this order.</div>',
                    unsafe_allow_html=True,
                )
