"""Reusable HTML rendering functions for the nesell-analytics dashboard.

All inline HTML/CSS for tables, badges, KPI strips, and indicators lives here.
Pages import these functions instead of duplicating HTML strings.
"""
from __future__ import annotations

import pandas as pd
from typing import Optional


# ---------------------------------------------------------------------------
# Shared CSS -- injected ONCE per page via render_table_css()
# ---------------------------------------------------------------------------

_TABLE_CSS = """
<style>
/* === Shared HTML table styles (lib/html_tables.py) === */
.nt-table-wrap {
    overflow-x: auto;
    border-radius: 6px;
    border: 1px solid #1e293b;
}
.nt-table-wrap.scroll-y {
    overflow-y: auto;
}
.nt-table {
    width: 100%;
    border-collapse: collapse;
    background: #111827;
}
.nt-table thead tr {
    border-bottom: 2px solid #1e293b;
    background: #0d1117;
}
.nt-table thead th {
    padding: 10px 8px;
    font-family: var(--font-mono, monospace);
    font-size: 0.58rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #64748b;
    white-space: nowrap;
    font-weight: 500;
    text-align: left;
    position: sticky;
    top: 0;
    z-index: 1;
    background: #0d1117;
}
.nt-table thead th.r { text-align: right; }
.nt-table thead th.c { text-align: center; }
.nt-table tbody tr {
    border-bottom: 1px solid #1e293b;
    transition: background-color 0.15s ease;
}
.nt-table tbody tr:nth-child(even) { background: #0f1729; }
.nt-table tbody tr:nth-child(odd) { background: #111827; }
.nt-table tbody tr:hover { background: #1a2332 !important; }
.nt-table tbody td {
    padding: 7px 8px;
    font-family: var(--font-mono, monospace);
    font-size: 0.75rem;
    color: #94a3b8;
    vertical-align: middle;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.nt-table tbody td.r { text-align: right; }
.nt-table tbody td.c { text-align: center; }
.nt-table tbody td.primary { color: #e2e8f0; }
.nt-table tbody td.bold { font-weight: 600; }

/* Thumbnail */
.nt-thumb {
    width: 36px; height: 36px;
    object-fit: cover;
    border-radius: 4px;
    border: 1px solid #1e293b;
}
.nt-thumb-placeholder {
    width: 36px; height: 36px;
    background: #1e293b;
    border-radius: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.45rem;
    color: #475569;
}

/* Large thumbnail (40px) */
.nt-thumb-lg { width: 40px; height: 40px; }
.nt-thumb-placeholder-lg {
    width: 40px; height: 40px;
    background: #1e293b;
    border-radius: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.5rem;
    color: #475569;
}

/* Source / status badge */
.nt-badge {
    display: inline-block;
    font-family: var(--font-mono, monospace);
    font-size: 0.6rem;
    padding: 2px 5px;
    border-radius: 3px;
    white-space: nowrap;
}

/* Margin pill (inline variant matching CSS-based .margin-pill) */
.nt-margin-pill {
    display: inline-block;
    font-family: var(--font-mono, monospace);
    font-size: 0.68rem;
    font-weight: 600;
    padding: 2px 8px;
    border-radius: 10px;
    white-space: nowrap;
}
.nt-margin-pill.high { color: #10b981; background: rgba(16,185,129,0.12); }
.nt-margin-pill.mid  { color: #f59e0b; background: rgba(245,158,11,0.12); }
.nt-margin-pill.low  { color: #ef4444; background: rgba(239,68,68,0.12); }

/* Action link */
.nt-action-link {
    color: #3b82f6;
    font-family: var(--font-mono, monospace);
    font-size: 0.7rem;
    text-decoration: none;
    background: rgba(59,130,246,0.08);
    padding: 3px 8px;
    border-radius: 3px;
    border: 1px solid rgba(59,130,246,0.2);
}
.nt-action-link:hover { text-decoration: underline; }

/* Alert banner */
.nt-alert {
    border-radius: 6px;
    padding: 14px 18px;
    margin: 12px 0;
}
.nt-alert.warning {
    background: #1c1208;
    border: 1px solid #92400e;
    border-left: 4px solid #f59e0b;
}
.nt-alert.success {
    background: #0b1a12;
    border: 1px solid #065f46;
    border-left: 4px solid #10b981;
}
.nt-alert-title {
    font-family: var(--font-mono, monospace);
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 4px;
}
.nt-alert-body {
    font-family: var(--font-mono, monospace);
    font-size: 0.85rem;
}
.nt-alert-detail {
    font-family: var(--font-mono, monospace);
    font-size: 0.75rem;
    margin-top: 4px;
}
</style>
"""

_css_injected = False


def render_table_css() -> str:
    """Return the shared CSS block. Call once per page, inject via st.markdown."""
    global _css_injected
    if not _css_injected:
        _css_injected = True
        return _TABLE_CSS
    return ""


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _hex_to_rgba(hex_color: str, alpha: float = 0.1) -> str:
    """Convert #rrggbb to rgba(r,g,b,alpha)."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _hex_to_rgb_csv(hex_color: str) -> str:
    """Convert #rrggbb to 'r,g,b' string."""
    h = hex_color.lstrip("#")
    return ",".join(str(int(h[i:i+2], 16)) for i in (0, 2, 4))


# ---------------------------------------------------------------------------
# render_thumbnail
# ---------------------------------------------------------------------------

def render_thumbnail(img_url: Optional[str], size: str = "sm") -> str:
    """Return HTML for a product thumbnail or N/A placeholder.

    Args:
        img_url: URL or None
        size: 'sm' (36px) or 'lg' (40px)
    """
    lg = size == "lg"
    cls_img = "nt-thumb-lg" if lg else "nt-thumb"
    cls_ph = "nt-thumb-placeholder-lg" if lg else "nt-thumb-placeholder"

    if img_url and str(img_url) != "None" and str(img_url).startswith("http"):
        return f'<img src="{img_url}" class="{cls_img}" loading="lazy" />'
    return f'<div class="{cls_ph}">N/A</div>'


# ---------------------------------------------------------------------------
# render_badge
# ---------------------------------------------------------------------------

def render_badge(text: str, color: str = "#64748b") -> str:
    """Return an inline badge <span> with given text and color.

    The background is derived from the color at 10% opacity.
    """
    rgb_csv = _hex_to_rgb_csv(color)
    return (
        f'<span class="nt-badge" style="color: {color}; '
        f'background: rgba({rgb_csv},0.1);">{text}</span>'
    )


# ---------------------------------------------------------------------------
# render_source_badge
# ---------------------------------------------------------------------------

_SOURCE_COLORS = {
    "printful": "#10b981",
    "pft": "#10b981",
    "wholesale": "#3b82f6",
    "hurtownia": "#3b82f6",
}


def render_source_badge(source: str) -> str:
    """Return a colored badge for a product source (printful, wholesale, etc.)."""
    color = _SOURCE_COLORS.get(str(source).lower(), "#64748b")
    return render_badge(source, color)


# ---------------------------------------------------------------------------
# render_margin_pill
# ---------------------------------------------------------------------------

def render_margin_pill(margin_pct: float) -> str:
    """Return a green/yellow/red margin pill.

    >30% = green (high), 10-30% = yellow (mid), <10% = red (low).
    """
    tier = "high" if margin_pct > 30 else ("mid" if margin_pct > 10 else "low")
    return f'<span class="nt-margin-pill {tier}">{margin_pct:.1f}%</span>'


# ---------------------------------------------------------------------------
# render_margin_color
# ---------------------------------------------------------------------------

def margin_color(margin_pct: float) -> str:
    """Return hex color string based on margin tier."""
    if margin_pct > 30:
        return "#10b981"
    elif margin_pct > 10:
        return "#fbbf24"
    return "#ef4444"


# ---------------------------------------------------------------------------
# render_action_link
# ---------------------------------------------------------------------------

def render_action_link(url: str, text: str = "Edit") -> str:
    """Return an action link button (e.g., 'Edit in BL')."""
    return (
        f'<a href="{url}" target="_blank" class="nt-action-link">'
        f'{text} &#8594;</a>'
    )


# ---------------------------------------------------------------------------
# render_alert_banner
# ---------------------------------------------------------------------------

def render_alert_banner(
    title: str,
    body: str,
    detail: str = "",
    variant: str = "warning",
    title_color: str = "",
    body_color: str = "",
    detail_color: str = "",
) -> str:
    """Return an alert banner (warning or success).

    Args:
        title: small uppercase header
        body: main message text
        detail: optional sub-text
        variant: 'warning' (amber) or 'success' (green)
    """
    _colors = {
        "warning": ("#f59e0b", "#fbbf24", "#92400e"),
        "success": ("#10b981", "#34d399", "#065f46"),
    }
    t_col, b_col, d_col = _colors.get(variant, _colors["warning"])
    t_col = title_color or t_col
    b_col = body_color or b_col
    d_col = detail_color or d_col

    detail_html = ""
    if detail:
        detail_html = f'<div class="nt-alert-detail" style="color: {d_col};">{detail}</div>'

    return (
        f'<div class="nt-alert {variant}">'
        f'<div class="nt-alert-title" style="color: {t_col};">{title}</div>'
        f'<div class="nt-alert-body" style="color: {b_col};">{body}</div>'
        f'{detail_html}'
        f'</div>'
    )


# ---------------------------------------------------------------------------
# render_data_table
# ---------------------------------------------------------------------------

def render_data_table(
    df: pd.DataFrame,
    columns_config: list[dict],
    title: Optional[str] = None,
    max_height: Optional[int] = None,
    row_height: int = 44,
) -> str:
    """Render a full HTML data table.

    Args:
        df: DataFrame with the data to display (already sorted/filtered).
        columns_config: list of column specs. Each dict has:
            - 'key': column name in df (or callable taking row -> str)
            - 'label': header text
            - 'align': 'left' (default), 'right', 'center'
            - 'width': optional CSS width (e.g. '46px', '200px')
            - 'render': optional callable(row) -> str for custom cell HTML
            - 'color': optional static color for the cell text
            - 'bold': optional bool for font-weight 600
        title: optional section title above the table
        max_height: optional max height in px (enables scroll-y)
        row_height: estimated row height for auto-sizing (default 44)

    Returns:
        Full HTML string ready for st.html() or st.markdown(unsafe_allow_html=True).
    """
    # Auto-compute height if not specified
    if max_height is None:
        auto_h = min(550, 46 + len(df) * row_height)
    else:
        auto_h = max_height

    # Build header
    ths = ""
    for col in columns_config:
        align_cls = ""
        if col.get("align") == "right":
            align_cls = ' class="r"'
        elif col.get("align") == "center":
            align_cls = ' class="c"'
        w_style = f' style="width: {col["width"]};"' if col.get("width") else ""
        ths += f'<th{align_cls}{w_style}>{col["label"]}</th>'

    # Build rows
    rows = ""
    for _, row in df.iterrows():
        tds = ""
        for col in columns_config:
            # Cell content
            if col.get("render"):
                content = col["render"](row)
            elif callable(col.get("key")):
                content = col["key"](row)
            else:
                val = row.get(col["key"], "")
                content = str(val) if val is not None else ""

            # Cell classes
            classes = []
            if col.get("align") == "right":
                classes.append("r")
            elif col.get("align") == "center":
                classes.append("c")
            if col.get("bold"):
                classes.append("bold")

            # Inline style
            style_parts = []
            if col.get("color"):
                c = col["color"]
                # color can be a string or a callable
                if callable(c):
                    style_parts.append(f"color: {c(row)};")
                else:
                    style_parts.append(f"color: {c};")
            if col.get("max_width"):
                style_parts.append(f"max-width: {col['max_width']};")
                style_parts.append("overflow: hidden; text-overflow: ellipsis;")

            cls_attr = f' class="{" ".join(classes)}"' if classes else ""
            st_attr = f' style="{" ".join(style_parts)}"' if style_parts else ""
            tds += f"<td{cls_attr}{st_attr}>{content}</td>"

        rows += f"<tr>{tds}</tr>"

    scroll_cls = ' scroll-y' if max_height else ""
    height_style = f' style="max-height: {auto_h}px;"' if max_height else ""

    title_html = ""
    if title:
        title_html = f'<div class="section-header">{title}</div>'

    return f"""{title_html}<div class="nt-table-wrap{scroll_cls}"{height_style}>
<table class="nt-table">
<thead><tr>{ths}</tr></thead>
<tbody>{rows}</tbody>
</table>
</div>"""


# ---------------------------------------------------------------------------
# render_cogs_gap_table
# ---------------------------------------------------------------------------

def render_cogs_gap_table(
    df: pd.DataFrame,
    show_source: bool = False,
    show_name: bool = False,
    max_rows: int = 50,
) -> str:
    """Render a COGS gap table with thumbnails and Baselinker links.

    Args:
        df: DataFrame with columns: sku, revenue_pln, units, orders_count,
            and optionally: image_url, name, source/source_label.
        show_source: whether to show a Source column
        show_name: whether to show a Name column
        max_rows: maximum rows to show
    """
    top = df.head(max_rows)

    # Build columns config
    cols: list[dict] = [
        {
            "key": "sku",
            "label": "",
            "width": "46px",
            "render": lambda r: f'<div style="padding: 0;">{render_thumbnail(r.get("image_url"))}</div>',
        },
        {
            "key": "sku",
            "label": "SKU",
            "color": "#e2e8f0",
            "max_width": "220px",
        },
    ]

    if show_name:
        cols.append({
            "key": "name",
            "label": "Name",
            "color": "#94a3b8",
            "max_width": "200px",
            "render": lambda r: str(r.get("name", ""))[:40],
        })

    if show_source:
        cols.append({
            "key": "source",
            "label": "Source",
            "align": "center",
            "render": lambda r: render_source_badge(
                r.get("source_label", r.get("source", "unknown"))
            ),
        })

    cols.extend([
        {
            "key": "revenue_pln",
            "label": "Revenue (PLN)",
            "align": "right",
            "color": "#fbbf24",
            "render": lambda r: f'{r.get("revenue_pln", 0):,.0f}',
        },
        {
            "key": "units",
            "label": "Units",
            "align": "right",
            "render": lambda r: str(int(r.get("units", 0))),
        },
        {
            "key": "orders_count",
            "label": "Orders",
            "align": "right",
            "render": lambda r: str(int(r.get("orders_count", 0))),
        },
        {
            "key": "sku",
            "label": "Action",
            "align": "center",
            "render": lambda r: render_action_link(
                f'https://panel-f.baselinker.com/products.html?search={r.get("sku", "")}',
                "Edit in BL",
            ),
        },
    ])

    return render_data_table(
        top,
        columns_config=cols,
        max_height=min(500, 46 + len(top) * 44),
        row_height=44,
    )


# ---------------------------------------------------------------------------
# render_product_table
# ---------------------------------------------------------------------------

def render_product_table(df: pd.DataFrame, max_rows: int = 50) -> str:
    """Render the SKU profitability table with thumbnails, source badges, margins.

    Expected columns: sku, name, source, units, revenue_per_unit, cost_per_unit,
                      revenue_pln, cogs, fees, cm3, cm3_pct, image_url.
    """
    top = df.head(max_rows)

    cols: list[dict] = [
        {
            "key": "image_url",
            "label": "",
            "width": "50px",
            "render": lambda r: render_thumbnail(r.get("image_url"), size="lg"),
        },
        {
            "key": "sku",
            "label": "SKU",
            "color": "#e2e8f0",
            "max_width": "180px",
        },
        {
            "key": "name",
            "label": "Name",
            "color": "#94a3b8",
            "max_width": "160px",
            "render": lambda r: str(r.get("name", ""))[:40],
        },
        {
            "key": "source",
            "label": "Source",
            "align": "center",
            "render": lambda r: render_source_badge(r.get("source", "unknown")),
        },
        {
            "key": "units",
            "label": "Units",
            "align": "right",
            "render": lambda r: str(int(r.get("units", 0))),
        },
        {
            "key": "revenue_per_unit",
            "label": "Avg Price",
            "align": "right",
            "render": lambda r: f'{r.get("revenue_per_unit", 0):,.0f}',
        },
        {
            "key": "cost_per_unit",
            "label": "Unit Cost",
            "align": "right",
            "render": lambda r: f'{r.get("cost_per_unit", 0):,.0f}',
        },
        {
            "key": "revenue_pln",
            "label": "Revenue",
            "align": "right",
            "color": "#e2e8f0",
            "render": lambda r: f'{r.get("revenue_pln", 0):,.0f}',
        },
        {
            "key": "cogs",
            "label": "COGS",
            "align": "right",
            "color": "#ef4444",
            "render": lambda r: f'{r.get("cogs", 0):,.0f}',
        },
        {
            "key": "fees",
            "label": "Fees",
            "align": "right",
            "color": "#fbbf24",
            "render": lambda r: f'{r.get("fees", 0):,.0f}',
        },
        {
            "key": "cm3",
            "label": "CM3",
            "align": "right",
            "color": "#10b981",
            "bold": True,
            "render": lambda r: f'{r.get("cm3", 0):,.0f}',
        },
        {
            "key": "cm3_pct",
            "label": "Margin%",
            "align": "right",
            "bold": True,
            "render": lambda r: render_margin_pill(r.get("cm3_pct", 0)),
        },
    ]

    return render_data_table(
        top,
        columns_config=cols,
        max_height=min(550, 42 + len(top) * 45),
        row_height=45,
    )
