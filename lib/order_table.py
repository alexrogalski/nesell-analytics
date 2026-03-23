"""Orders table HTML rendering: generates the main orders list table."""
import pandas as pd
from lib.theme import COLORS


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

# Currency display suffix (small, muted, appended after the number)
_CCY_SPAN = '<span style="font-size:0.6rem;color:#64748b;margin-left:2px">'


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


def _val_pln(val):
    """Format a cell value with 'PLN' suffix in small muted text."""
    return _fmt(val) + _CCY_SPAN + 'PLN</span>'


def _val_ccy(val, ccy):
    """Format a cell value with the given currency suffix."""
    return _fmt(val) + _CCY_SPAN + ccy + '</span>'


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


def render_table_html(visible, items_df):
    """Build orders table HTML from visible rows. Returns table_html string."""

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
        + '<col class="col-ship"/>'
        + '<col class="col-other"/>'
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
        + '<th class="r">Ship</th>'
        + '<th class="r">Other</th>'
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
        currency = str(row.get("currency", "EUR")).upper()
        total_paid_orig = float(row.get("total_paid", 0) or 0)
        unit_count = int(row.get("unit_count", 0))
        first_name = str(row.get("first_name", ""))[:35]
        # Use revenue_net_pln if available, fallback to revenue_pln
        revenue_net = float(row.get("revenue_net_pln", row.get("revenue_pln", 0)) or 0)
        cogs = float(row.get("cogs_pln", 0) or 0)
        fees = float(row.get("fees_pln", 0) or 0)
        shipping = float(row.get("shipping_pln", 0) or 0)
        # Other costs: fulfillment + ppc + storage + fx_spread
        fulfillment = float(row.get("fulfillment_cost_pln", 0) or 0)
        ppc = float(row.get("ppc_cost_pln", 0) or 0)
        storage = float(row.get("storage_fee_pln", 0) or 0)
        fx_spread = float(row.get("fx_spread_pln", 0) or 0)
        other_costs = fulfillment + ppc + storage + fx_spread
        profit = float(row.get("profit_pln", 0) or 0)
        margin = float(row.get("margin_pct", 0) or 0)
        has_cogs = bool(row.get("has_cogs", False))

        # Revenue cell: show PLN value + original amount in tooltip
        orig_tooltip = f"{total_paid_orig:,.2f} {currency}"
        rev_td = (
            '<td class="r rev-cell" title="Orig: ' + orig_tooltip + '">'
            + _val_pln(revenue_net) + '</td>'
        )

        # COGS display
        if has_cogs:
            cogs_td = '<td class="r">' + _val_pln(cogs) + '</td>'
        else:
            cogs_td = '<td class="r cogs-missing">n/a</td>'

        # Fees
        fees_td = '<td class="r fees-cell">' + _val_pln(fees) + '</td>'

        # Shipping
        ship_td = '<td class="r">' + _val_pln(shipping) + '</td>'

        # Other costs display: gray dash if zero
        if other_costs > 0:
            other_td = '<td class="r">' + _val_pln(other_costs) + '</td>'
        else:
            other_td = '<td class="r" style="color: #475569;">-</td>'

        # Profit with currency
        profit_cls = "positive" if profit > 0 else "negative"
        profit_td = (
            '<td class="r profit-cell ' + profit_cls + '">'
            + _val_pln(profit) + '</td>'
        )

        # Items display
        items_text = (
            '<span class="items-count">'
            + str(unit_count)
            + 'x</span> '
            + first_name
        ) if unit_count > 0 else "0"

        # Return indicator in order ID cell
        has_return = bool(row.get("has_return", False))
        ret_indicator = (
            ' <span style="color: #ef4444; font-size: 0.6rem; font-weight: 600;">RET</span>'
            if has_return else ''
        )

        rows_html += (
            '<tr>'
            + '<td>' + order_date + '</td>'
            + '<td title="' + ext_id + '">'
            + '<span style="color: #e2e8f0;">' + ext_id_short + '</span>'
            + ret_indicator
            + '</td>'
            + '<td class="c">' + _plat_badge_html(platform) + '</td>'
            + '<td class="c"><span class="country-badge">'
            + country_label + '</span></td>'
            + '<td><span class="items-text">' + items_text + '</span></td>'
            + rev_td
            + cogs_td
            + fees_td
            + ship_td
            + other_td
            + profit_td
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
    return table_html
