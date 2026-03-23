"""P&L - Sellerboard-style waterfall, contribution margins, fee decomposition."""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import (
    load_daily_metrics, load_platforms, load_products,
    load_refund_summary, load_amazon_ad_spend, load_amazon_storage_fees,
    load_fx_rates, load_tacos_trend, load_organic_paid_split,
    load_orders_enriched,
)
from lib.metrics import calc_period_kpis, daily_summary, calc_contribution_margins, platform_summary
from lib.charts import multi_line

setup_page("P&L")

st.markdown('<div class="section-header">PROFIT & LOSS</div>', unsafe_allow_html=True)

# Sidebar
period_options = {7: "7D", 14: "14D", 30: "30D", 60: "60D", 90: "90D"}
days = st.sidebar.selectbox(
    "PERIOD", list(period_options.keys()), index=2,
    format_func=lambda x: period_options[x], key="pnl_period",
)

platforms = load_platforms()

# ---------------------------------------------------------------------------
# Primary data source: load_orders_enriched (has VAT, 3PL, PPC, storage, FX)
# Falls back to daily_metrics if enriched data is unavailable.
# ---------------------------------------------------------------------------
_enriched_result = load_orders_enriched(days=days)
if isinstance(_enriched_result, tuple):
    orders_enriched, _items_df = _enriched_result
else:
    orders_enriched = _enriched_result
    _items_df = pd.DataFrame()


def _safe_col_sum(df, col, default=0.0):
    """Sum a column if it exists, otherwise return default."""
    if col in df.columns:
        return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
    return default


# Also load daily_metrics (needed for daily CM trend and period comparison)
df = load_daily_metrics(days=days * 2)

if orders_enriched.empty and df.empty:
    st.warning("No data available. Run ETL: python3.11 -m etl.run")
    st.stop()

# Platform mapping for daily_metrics
if not df.empty:
    df["platform"] = df["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))

# Platform filter (union of platforms from both sources)
_plat_sets = set()
if not df.empty:
    _plat_sets.update(df["platform"].unique())
if not orders_enriched.empty and "platform_name" in orders_enriched.columns:
    _plat_sets.update(orders_enriched["platform_name"].unique())
all_platforms = sorted(_plat_sets) if _plat_sets else []
selected_platforms = st.sidebar.multiselect("PLATFORMS", all_platforms, default=all_platforms, key="pnl_plats")

if not df.empty:
    df = df[df["platform"].isin(selected_platforms)]
if not orders_enriched.empty and "platform_name" in orders_enriched.columns:
    orders_enriched = orders_enriched[orders_enriched["platform_name"].isin(selected_platforms)]

if orders_enriched.empty and df.empty:
    st.warning("No data for selected filters.")
    st.stop()

# Current period boundaries
now = datetime.now().date()
current_start = now - timedelta(days=days)

# Filter enriched orders to current period
if not orders_enriched.empty:
    orders_enriched["_date_parsed"] = pd.to_datetime(
        orders_enriched["order_date"], errors="coerce"
    ).dt.date
    oe_current = orders_enriched[orders_enriched["_date_parsed"] >= current_start].copy()
else:
    oe_current = pd.DataFrame()

# Filter daily_metrics to current period
if not df.empty:
    df["date_parsed"] = pd.to_datetime(df["date"]).dt.date
    df_current = df[df["date_parsed"] >= current_start]
else:
    df_current = pd.DataFrame()

# --- Load supplementary cost data (used by sections that still need them) ---

# Refunds
refund_data = load_refund_summary(days=days)
total_refunds = refund_data.get("estimated_refund_cost_pln", 0)
refund_units = refund_data.get("total_units_returned", 0)
refund_rate = refund_data.get("refund_rate_pct", 0)
refund_by_date = refund_data.get("refund_by_date", pd.DataFrame())

# FX rate for EUR->PLN (used by PPC/storage detail sections)
fx_df = load_fx_rates(days=days)
eur_rate = 4.30  # fallback
if not fx_df.empty and "currency" in fx_df.columns:
    eur_rows = fx_df[fx_df["currency"] == "EUR"]
    if not eur_rows.empty:
        eur_rate = float(eur_rows["rate_pln"].iloc[-1])

# PPC / Advertising spend (for the PPC detail section)
ads_df = load_amazon_ad_spend(days=days)
total_ppc = 0.0
total_ppc_pln = 0.0
ads_daily = pd.DataFrame()
ad_spend_by_date = {}
if not ads_df.empty:
    ads_daily = ads_df.groupby("date").agg({"spend": "sum", "sales": "sum"}).reset_index()
    for _, row in ads_daily.iterrows():
        d = str(row["date"])[:10]
        spend_pln = float(row["spend"]) * eur_rate
        ad_spend_by_date[d] = spend_pln
    total_ppc = float(ads_df["spend"].sum())
    total_ppc_pln = sum(ad_spend_by_date.values())

# Storage fees (for the storage detail section)
storage_df = load_amazon_storage_fees()
total_storage = 0.0
total_storage_pln = 0.0
if not storage_df.empty:
    month_start = current_start.strftime("%Y-%m")
    month_end = now.strftime("%Y-%m")
    storage_period = storage_df[
        (storage_df["month"] >= month_start) & (storage_df["month"] <= month_end)
    ]
    if not storage_period.empty:
        total_storage = float(storage_period["estimated_storage_fee"].sum())
        storage_currency = storage_period["currency"].iloc[0] if "currency" in storage_period.columns else "EUR"
        if storage_currency == "PLN":
            total_storage_pln = total_storage
        else:
            total_storage_pln = total_storage * eur_rate

# ============================================================================
# 1. P&L WATERFALL (from enriched orders with full cost breakdown)
# ============================================================================
st.markdown('<div class="section-header">P&L WATERFALL</div>', unsafe_allow_html=True)

# Aggregate from enriched orders (preferred, has all new columns)
if not oe_current.empty:
    total_revenue_brutto = _safe_col_sum(oe_current, "revenue_pln")
    total_vat = _safe_col_sum(oe_current, "vat_amount_pln")
    total_revenue_net = _safe_col_sum(oe_current, "revenue_net_pln")
    total_cogs = _safe_col_sum(oe_current, "cogs_pln")
    total_fees = _safe_col_sum(oe_current, "fees_pln")
    total_shipping = _safe_col_sum(oe_current, "shipping_pln")
    total_3pl = _safe_col_sum(oe_current, "fulfillment_cost_pln")
    total_ppc_enriched = _safe_col_sum(oe_current, "ppc_cost_pln")
    total_storage_enriched = _safe_col_sum(oe_current, "storage_fee_pln")
    total_fx_spread = _safe_col_sum(oe_current, "fx_spread_pln")
    total_costs = _safe_col_sum(oe_current, "total_costs_pln")
    net_profit = _safe_col_sum(oe_current, "profit_pln")
else:
    # Fallback: use daily_metrics (old columns)
    total_revenue_brutto = _safe_col_sum(df_current, "revenue_pln")
    total_vat = 0.0
    total_revenue_net = total_revenue_brutto  # no VAT info in legacy data
    total_cogs = _safe_col_sum(df_current, "cogs")
    total_fees = _safe_col_sum(df_current, "fees")
    total_shipping = _safe_col_sum(df_current, "shipping_cost")
    total_3pl = 0.0
    total_ppc_enriched = total_ppc_pln
    total_storage_enriched = total_storage_pln
    total_fx_spread = 0.0
    total_costs = total_cogs + total_fees + total_shipping + total_ppc_pln + total_storage_pln
    net_profit = total_revenue_net - total_costs - total_refunds

# Full waterfall:
# Revenue Brutto -> VAT -> Revenue Netto -> COGS -> Fees -> Shipping
# -> 3PL -> PPC -> Storage -> FX Spread -> Refunds -> NET PROFIT

wf_labels = []
wf_values = []
wf_measures = []
wf_text = []

# Revenue Brutto (starting absolute)
wf_labels.append("Revenue Brutto")
wf_values.append(total_revenue_brutto)
wf_measures.append("absolute")
wf_text.append(f"{total_revenue_brutto:,.0f}")

# VAT deduction
wf_labels.append("VAT")
wf_values.append(-total_vat)
wf_measures.append("relative")
wf_text.append(f"-{total_vat:,.0f}" if total_vat > 0 else "0")

# Revenue Netto subtotal
wf_labels.append("Revenue Netto")
wf_values.append(0)
wf_measures.append("total")
wf_text.append(f"{total_revenue_net:,.0f}")

# COGS
wf_labels.append("COGS")
wf_values.append(-total_cogs)
wf_measures.append("relative")
wf_text.append(f"-{total_cogs:,.0f}" if total_cogs > 0 else "0")

# Platform Fees
wf_labels.append("Fees")
wf_values.append(-total_fees)
wf_measures.append("relative")
wf_text.append(f"-{total_fees:,.0f}" if total_fees > 0 else "0")

# Shipping
wf_labels.append("Shipping")
wf_values.append(-total_shipping)
wf_measures.append("relative")
wf_text.append(f"-{total_shipping:,.0f}" if total_shipping > 0 else "0")

# Exportivo 3PL (fulfillment)
wf_labels.append("3PL")
wf_values.append(-total_3pl)
wf_measures.append("relative")
wf_text.append(f"-{total_3pl:,.0f}" if total_3pl > 0 else "0")

# PPC / Ads
wf_labels.append("PPC/Ads")
wf_values.append(-total_ppc_enriched)
wf_measures.append("relative")
wf_text.append(f"-{total_ppc_enriched:,.0f}" if total_ppc_enriched > 0 else "0")

# FBA Storage
wf_labels.append("Storage")
wf_values.append(-total_storage_enriched)
wf_measures.append("relative")
wf_text.append(f"-{total_storage_enriched:,.0f}" if total_storage_enriched > 0 else "0")

# FX Spread (Amazon ACCS)
wf_labels.append("FX Spread")
wf_values.append(-total_fx_spread)
wf_measures.append("relative")
wf_text.append(f"-{total_fx_spread:,.0f}" if total_fx_spread > 0 else "0")

# Refunds
wf_labels.append("Refunds")
wf_values.append(-total_refunds)
wf_measures.append("relative")
wf_text.append(f"-{total_refunds:,.0f}" if total_refunds > 0 else "0")

# Net Profit (final total)
_final_profit = net_profit - total_refunds if not oe_current.empty else net_profit
wf_labels.append("Net Profit")
wf_values.append(0)
wf_measures.append("total")
wf_text.append(f"{_final_profit:,.0f}")

fig_wf = go.Figure(go.Waterfall(
    x=wf_labels,
    y=wf_values,
    measure=wf_measures,
    connector=dict(line=dict(color=COLORS["border"])),
    increasing=dict(marker=dict(color=COLORS["success"])),
    decreasing=dict(marker=dict(color=COLORS["danger"])),
    totals=dict(marker=dict(color=COLORS["primary"])),
    textposition="outside",
    text=wf_text,
    textfont=dict(size=10),
))
fig_wf.update_layout(title="", height=500, showlegend=False)
st.plotly_chart(fig_wf, use_container_width=True)

# KPI summary row
_rev_base = total_revenue_net if total_revenue_net > 0 else 1  # avoid division by zero
_margin_pct = _final_profit / _rev_base * 100 if total_revenue_net > 0 else 0
_cost_pct = (total_costs + total_refunds) / _rev_base * 100 if total_revenue_net > 0 else 0
_vat_pct = total_vat / total_revenue_brutto * 100 if total_revenue_brutto > 0 else 0

w1, w2, w3, w4, w5, w6 = st.columns(6)
w1.metric("Revenue Netto", f"{total_revenue_net:,.0f} PLN",
          delta=f"VAT: -{total_vat:,.0f} ({_vat_pct:.1f}%)")
w2.metric("Total Costs", f"{total_costs + total_refunds:,.0f} PLN",
          delta=f"{_cost_pct:.1f}% of net rev")
w3.metric("Net Profit", f"{_final_profit:,.0f} PLN",
          delta=f"{_margin_pct:.1f}% margin")
w4.metric(
    "Refunds",
    f"-{total_refunds:,.0f} PLN",
    delta=f"{refund_units} units ({refund_rate:.1f}%)" if refund_units > 0 else "0 returns",
    delta_color="inverse",
)
# COGS coverage: what % of net revenue has COGS data
if not oe_current.empty:
    _rev_with_cogs = _safe_col_sum(oe_current[oe_current.get("cogs_pln", pd.Series(0)) > 0], "revenue_net_pln") if "cogs_pln" in oe_current.columns else 0
    _cogs_coverage = _rev_with_cogs / total_revenue_net * 100 if total_revenue_net > 0 else 0
elif not df_current.empty:
    _cogs_coverage = _safe_col_sum(df_current[df_current["cogs"] > 0], "revenue_pln") / total_revenue_brutto * 100 if total_revenue_brutto > 0 else 0
else:
    _cogs_coverage = 0
w5.metric("COGS Coverage", f"{_cogs_coverage:.0f}%")

# FX Spread indicator
w6.metric("FX Spread (ACCS)", f"-{total_fx_spread:,.0f} PLN",
          delta=f"{total_fx_spread / total_revenue_brutto * 100:.2f}% of brutto" if total_revenue_brutto > 0 else "N/A")

# --- Cost breakdown detail ---
st.markdown('<div class="section-header">COST BREAKDOWN</div>', unsafe_allow_html=True)

cost_items = {
    "COGS": total_cogs,
    "Platform Fees": total_fees,
    "Shipping": total_shipping,
    "3PL (Exportivo)": total_3pl,
    "FBA Storage": total_storage_enriched,
    "PPC / Ads": total_ppc_enriched,
    "FX Spread": total_fx_spread,
    "Refunds": total_refunds,
}
_total_all_costs = sum(cost_items.values())

c1, c2 = st.columns([2, 1])

with c1:
    cost_df = pd.DataFrame([
        {"Cost Type": k, "Amount (PLN)": v, "% of Net Revenue": v / total_revenue_net * 100 if total_revenue_net > 0 else 0}
        for k, v in cost_items.items() if v > 0
    ])
    if not cost_df.empty:
        cost_df = cost_df.sort_values("Amount (PLN)", ascending=True)
        cost_colors = {
            "COGS": COLORS["danger"],
            "Platform Fees": COLORS["warning"],
            "Shipping": COLORS["info"],
            "3PL (Exportivo)": "#14b8a6",
            "FBA Storage": "#8b5cf6",
            "PPC / Ads": "#ec4899",
            "FX Spread": "#6366f1",
            "Refunds": "#f97316",
        }
        fig_costs = go.Figure()
        fig_costs.add_trace(go.Bar(
            y=cost_df["Cost Type"],
            x=cost_df["Amount (PLN)"],
            orientation="h",
            marker_color=[cost_colors.get(ct, COLORS["muted"]) for ct in cost_df["Cost Type"]],
            text=cost_df.apply(
                lambda r: f'{r["Amount (PLN)"]:,.0f} PLN ({r["% of Net Revenue"]:.1f}%)', axis=1
            ),
            textposition="outside",
        ))
        fig_costs.update_layout(height=max(250, len(cost_df) * 50), showlegend=False, title="")
        st.plotly_chart(fig_costs, use_container_width=True)
    else:
        st.info("No cost data to display.")

with c2:
    st.markdown("**P&L Summary**")
    summary_data = {
        "Line Item": [
            "Revenue Brutto",
            "VAT",
            "Revenue Netto",
            "Total Costs",
            "Refunds",
            "Net Profit",
        ],
        "PLN": [
            f"{total_revenue_brutto:,.0f}",
            f"-{total_vat:,.0f}",
            f"{total_revenue_net:,.0f}",
            f"-{total_costs:,.0f}",
            f"-{total_refunds:,.0f}",
            f"{_final_profit:,.0f}",
        ],
        "% Net Rev": [
            f"{total_revenue_brutto / total_revenue_net * 100:.1f}%" if total_revenue_net > 0 else "N/A",
            f"{total_vat / total_revenue_net * 100:.1f}%" if total_revenue_net > 0 else "0%",
            "100.0%",
            f"{total_costs / total_revenue_net * 100:.1f}%" if total_revenue_net > 0 else "0%",
            f"{total_refunds / total_revenue_net * 100:.1f}%" if total_revenue_net > 0 else "0%",
            f"{_margin_pct:.1f}%",
        ],
    }
    _pnl_summary_df = pd.DataFrame(summary_data)
    st.dataframe(_pnl_summary_df, use_container_width=True, hide_index=True)
    _pnl_csv = _pnl_summary_df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", _pnl_csv, "pnl_summary.csv", "text/csv", key="dl_pnl_summary")

    # Data availability notes
    missing = []
    if total_ppc_enriched == 0 and total_ppc_pln == 0:
        missing.append("PPC/Ads: No data. Import CSV via `--ads-csv`")
    if total_storage_enriched == 0 and total_storage_pln == 0:
        missing.append("Storage: No data in `amazon_storage_fees` table")
    if total_refunds == 0:
        missing.append("Refunds: No return data in `amazon_returns`")
    if total_vat == 0:
        missing.append("VAT: No enriched order data. Run ETL to populate")
    if total_3pl == 0:
        missing.append("3PL: No fulfillment cost data (Exportivo 5 PLN/FBM)")
    if total_fx_spread == 0:
        missing.append("FX Spread: No ACCS spread data")
    if missing:
        with st.expander("Missing data sources", expanded=False):
            for m in missing:
                st.caption(m)

# --- 2. Daily CM trend ---
st.markdown('<div class="section-header">DAILY CONTRIBUTION MARGINS</div>', unsafe_allow_html=True)

daily = daily_summary(
    df,
    refund_by_date=refund_by_date,
    ad_spend_by_date=ad_spend_by_date if ad_spend_by_date else None,
    storage_fees_total=total_storage_pln,
    period_days=days,
)
if not daily.empty:
    daily["date"] = pd.to_datetime(daily["date"])
    chart_cutoff = datetime.now() - timedelta(days=days)
    daily_chart = daily[daily["date"] >= chart_cutoff].copy()

    if not daily_chart.empty and all(c in daily_chart.columns for c in ["cm1", "cm2", "cm3"]):
        y_cols = ["cm1", "cm2", "cm3"]
        names = ["CM1 (Rev-COGS)", "CM2 (CM1-Fees)", "CM3 (CM2-Ship)"]
        colors = [COLORS["cm1"], COLORS["cm2"], COLORS["cm3"]]

        # Add Net Profit line if it differs from CM3
        if "net_profit" in daily_chart.columns:
            has_extra_costs = False
            for extra_col in ["ppc_cost", "storage_cost", "refunds"]:
                if extra_col in daily_chart.columns and daily_chart[extra_col].sum() > 0:
                    has_extra_costs = True
                    break
            if has_extra_costs:
                y_cols.append("net_profit")
                names.append("Net Profit")
                colors.append("#f97316")

        # Add 7d MA for the bottom line
        bottom_col = "net_profit" if "net_profit" in daily_chart.columns else "cm3"
        ma_col = f"{bottom_col}_7d"
        if ma_col not in daily_chart.columns:
            daily_chart[ma_col] = daily_chart[bottom_col].rolling(7, min_periods=1).mean()
        y_cols.append(ma_col)
        names.append(f"7d MA")
        colors.append(COLORS["muted"])

        fig_cm = multi_line(daily_chart, "date", y_cols, colors=colors, names=names, height=380)
        st.plotly_chart(fig_cm, use_container_width=True)

# --- 3. Source split (Printful vs Resell) ---
st.markdown('<div class="section-header">SOURCE SPLIT: PRINTFUL vs RESELL</div>', unsafe_allow_html=True)

products_df = load_products()

# Use enriched orders for source split if available (has net revenue and profit_pln)
_source_df = oe_current if not oe_current.empty else df_current
if not products_df.empty and not _source_df.empty:
    source_map = dict(zip(products_df["sku"], products_df["source"].fillna("unknown")))
    df_with_source = _source_df.copy()

    # Determine SKU column name
    _sku_col = "first_sku" if "first_sku" in df_with_source.columns else "sku"
    if _sku_col in df_with_source.columns:
        df_with_source["source"] = df_with_source[_sku_col].map(source_map).fillna("unknown")
    else:
        df_with_source["source"] = "unknown"

    df_with_source["source_group"] = df_with_source["source"].apply(
        lambda x: "Printful" if str(x).lower() in ["printful", "pft"] else "Resell/Other"
    )

    # Adapt column names based on which data source we are using
    if not oe_current.empty:
        _rev_col = "revenue_net_pln"
        _cogs_col = "cogs_pln"
        _fees_col = "fees_pln"
        _profit_col = "profit_pln"
    else:
        _rev_col = "revenue_pln"
        _cogs_col = "cogs"
        _fees_col = "fees"
        _profit_col = "profit"

    _agg_dict = {}
    for _c in [_rev_col, _cogs_col, _fees_col, _profit_col]:
        if _c in df_with_source.columns:
            _agg_dict[_c] = "sum"
    if "orders_count" in df_with_source.columns:
        _agg_dict["orders_count"] = "sum"
    elif "id" in df_with_source.columns:
        _agg_dict["id"] = "count"
    if "units" in df_with_source.columns:
        _agg_dict["units"] = "sum"
    elif "unit_count" in df_with_source.columns:
        _agg_dict["unit_count"] = "sum"

    if _agg_dict:
        source_agg = df_with_source.groupby("source_group").agg(_agg_dict).reset_index()

        # Normalize column names for display
        _rename = {}
        if _rev_col in source_agg.columns and _rev_col != "revenue_pln":
            _rename[_rev_col] = "revenue_pln"
        if _cogs_col in source_agg.columns and _cogs_col != "cogs":
            _rename[_cogs_col] = "cogs"
        if _fees_col in source_agg.columns and _fees_col != "fees":
            _rename[_fees_col] = "fees"
        if _profit_col in source_agg.columns and _profit_col != "profit":
            _rename[_profit_col] = "profit"
        if "id" in source_agg.columns:
            _rename["id"] = "orders_count"
        if "unit_count" in source_agg.columns:
            _rename["unit_count"] = "units"
        if _rename:
            source_agg = source_agg.rename(columns=_rename)

        # Ensure required columns exist for calc_contribution_margins
        for _needed in ["revenue_pln", "cogs", "fees", "profit", "orders_count", "units"]:
            if _needed not in source_agg.columns:
                source_agg[_needed] = 0

        source_agg = calc_contribution_margins(source_agg)

        s1, s2 = st.columns(2)
        for i, (_, row) in enumerate(source_agg.iterrows()):
            col = s1 if i == 0 else s2
            with col:
                _label = "Net Revenue" if not oe_current.empty else "Revenue"
                st.markdown(f"**{row['source_group']}**")
                st.metric(_label, f"{row['revenue_pln']:,.0f} PLN")
                st.metric("CM1", f"{row['cm1']:,.0f} PLN ({row['cm1_pct']:.1f}%)")
                st.metric("CM3", f"{row['cm3']:,.0f} PLN ({row['cm3_pct']:.1f}%)")
                _units = int(row["units"]) if "units" in row.index else 0
                st.metric("Units", f"{_units:,}")

# --- 4. Fee decomposition by platform ---
st.markdown('<div class="section-header">FEE DECOMPOSITION BY PLATFORM</div>', unsafe_allow_html=True)

if not df_current.empty:
    plat_summary = platform_summary(df_current, platforms)
    if not plat_summary.empty:
        plat_fees = plat_summary[["platform", "fees", "cogs", "revenue_pln"]].copy()
        plat_fees["fees_pct"] = np.where(plat_fees["revenue_pln"] > 0, plat_fees["fees"] / plat_fees["revenue_pln"] * 100, 0)
        plat_fees = plat_fees.sort_values("fees", ascending=True)

        fig_fees = go.Figure()
        fig_fees.add_trace(go.Bar(
            y=plat_fees["platform"], x=plat_fees["fees"],
            name="Platform Fees", orientation="h", marker_color=COLORS["warning"],
            text=plat_fees["fees_pct"].map(lambda x: f"{x:.1f}%"), textposition="outside",
        ))
        fig_fees.update_layout(height=max(250, len(plat_fees) * 40), showlegend=False, title="")
        st.plotly_chart(fig_fees, use_container_width=True)

# --- 5. PPC / Advertising detail ---
if not ads_df.empty:
    st.markdown('<div class="section-header">PPC / ADVERTISING</div>', unsafe_allow_html=True)

    ppc1, ppc2, ppc3, ppc4, ppc5 = st.columns(5)
    total_impressions = int(ads_df["impressions"].sum())
    total_clicks = int(ads_df["clicks"].sum())
    total_ad_sales = float(ads_df["sales"].sum())
    total_ad_orders = int(ads_df["orders"].sum()) if "orders" in ads_df.columns else 0
    overall_acos = (total_ppc / total_ad_sales * 100) if total_ad_sales > 0 else 0
    overall_roas = (total_ad_sales / total_ppc) if total_ppc > 0 else 0
    ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0

    # TACoS = total ad spend / total revenue (all channels)
    _tacos_base = total_revenue_net if total_revenue_net > 0 else total_revenue_brutto
    overall_tacos = (total_ppc_pln / _tacos_base * 100) if _tacos_base > 0 else 0
    tacos_status = "HEALTHY" if overall_tacos < 10 else ("MODERATE" if overall_tacos <= 15 else "HIGH")

    ppc1.metric("Ad Spend", f"{total_ppc:,.2f} EUR", delta=f"{total_ppc_pln:,.0f} PLN")
    ppc2.metric("Ad Sales", f"{total_ad_sales:,.2f} EUR")
    ppc3.metric("ACOS", f"{overall_acos:.1f}%", delta=f"ROAS: {overall_roas:.2f}x")
    ppc4.metric("CTR", f"{ctr:.2f}%", delta=f"{total_clicks:,} clicks / {total_impressions:,} impr.")
    ppc5.metric("TACoS", f"{overall_tacos:.1f}%", delta=tacos_status,
                delta_color="normal" if overall_tacos < 10 else ("off" if overall_tacos <= 15 else "inverse"))

    # Daily PPC trend
    if not ads_daily.empty and len(ads_daily) > 1:
        ads_daily_chart = ads_daily.copy()
        ads_daily_chart["date"] = pd.to_datetime(ads_daily_chart["date"])
        fig_ppc = go.Figure()
        fig_ppc.add_trace(go.Bar(
            x=ads_daily_chart["date"], y=ads_daily_chart["spend"],
            name="Spend", marker_color=COLORS["danger"], opacity=0.7,
        ))
        fig_ppc.add_trace(go.Bar(
            x=ads_daily_chart["date"], y=ads_daily_chart["sales"],
            name="Sales", marker_color=COLORS["success"], opacity=0.7,
        ))
        fig_ppc.update_layout(
            height=300, barmode="group", title="Daily PPC Spend vs Sales (EUR)",
            legend=dict(orientation="h", y=-0.15),
        )
        st.plotly_chart(fig_ppc, use_container_width=True)

    # TACoS Trend
    tacos_data = load_tacos_trend(days=days)
    if not tacos_data.empty and tacos_data["ad_spend_pln"].sum() > 0:
        tacos_chart = tacos_data[tacos_data["revenue_pln"] > 0].copy()
        if not tacos_chart.empty:
            tacos_chart["date"] = pd.to_datetime(tacos_chart["date"])
            fig_tacos = go.Figure()
            fig_tacos.add_trace(go.Scatter(
                x=tacos_chart["date"], y=tacos_chart["tacos"],
                mode="lines", name="TACoS %",
                line=dict(color=COLORS["muted"], width=1), opacity=0.4,
            ))
            fig_tacos.add_trace(go.Scatter(
                x=tacos_chart["date"], y=tacos_chart["tacos_7d"],
                mode="lines", name="TACoS 7d MA",
                line=dict(color=COLORS["warning"], width=2),
            ))
            fig_tacos.add_hline(y=10, line_dash="dot", line_color=COLORS["success"], opacity=0.5,
                                annotation_text="10% (healthy)")
            fig_tacos.add_hline(y=15, line_dash="dot", line_color=COLORS["danger"], opacity=0.5,
                                annotation_text="15% (high)")
            fig_tacos.update_layout(height=280, title="TACoS Trend (7d Moving Average)",
                                    legend=dict(orientation="h", y=-0.15))
            st.plotly_chart(fig_tacos, use_container_width=True)

# --- 6. Storage fees detail ---
if not storage_df.empty and total_storage > 0:
    st.markdown('<div class="section-header">FBA STORAGE FEES</div>', unsafe_allow_html=True)

    st1, st2, st3 = st.columns(3)
    _storage_display = total_storage_enriched if total_storage_enriched > 0 else total_storage_pln
    st1.metric("Total Storage Fees", f"{total_storage:,.2f} EUR", delta=f"{_storage_display:,.0f} PLN")
    st2.metric("% of Net Revenue", f"{_storage_display/total_revenue_net*100:.2f}%" if total_revenue_net > 0 else "N/A")
    st3.metric("ASINs with Storage", f"{storage_df['asin'].nunique()}")

    top_storage = storage_df.groupby(["asin", "product_name"]).agg(
        total_fee=("estimated_storage_fee", "sum"),
        avg_qty=("avg_qty", "mean"),
    ).reset_index().sort_values("total_fee", ascending=False).head(10)

    if not top_storage.empty:
        st.dataframe(
            top_storage.rename(columns={
                "asin": "ASIN", "product_name": "Product",
                "total_fee": "Storage Fee (EUR)", "avg_qty": "Avg Qty",
            }),
            use_container_width=True, hide_index=True,
        )

# --- 7. Period comparison table ---
st.markdown('<div class="section-header">PERIOD COMPARISON</div>', unsafe_allow_html=True)

kpis = calc_period_kpis(df, days, refund_summary=refund_data,
                        ppc_total=total_ppc_pln, storage_total=total_storage_pln)
if kpis:
    comp_data = {
        "Metric": [
            "Revenue (Brutto)", "VAT", "Revenue (Netto)", "COGS", "Platform Fees", "Shipping",
            "3PL (Exportivo)", "FBA Storage", "PPC / Ads", "FX Spread", "Refunds",
            "Net Profit", "Margin %", "ROI %", "Orders", "Units", "AOV",
        ],
        f"Current {days}d": [
            f"{total_revenue_brutto:,.0f}",
            f"-{total_vat:,.0f}",
            f"{total_revenue_net:,.0f}",
            f"{total_cogs:,.0f}" if not oe_current.empty else f"{kpis['cogs']:,.0f}",
            f"{total_fees:,.0f}" if not oe_current.empty else f"{kpis['fees']:,.0f}",
            f"{total_shipping:,.0f}" if not oe_current.empty else f"{kpis.get('shipping', 0):,.0f}",
            f"{total_3pl:,.0f}",
            f"{total_storage_enriched:,.0f}" if not oe_current.empty else f"{kpis.get('storage', 0):,.0f}",
            f"{total_ppc_enriched:,.0f}" if not oe_current.empty else f"{kpis.get('ppc', 0):,.0f}",
            f"{total_fx_spread:,.0f}",
            f"{total_refunds:,.0f}" if total_refunds > 0 else f"{kpis.get('refunds', 0):,.0f}",
            f"{_final_profit:,.0f}",
            f"{_margin_pct:.1f}%",
            f"{kpis.get('roi', 0):.1f}%",
            f"{kpis['orders']:,}",
            f"{kpis['units']:,}",
            f"{kpis['aov']:,.0f}",
        ],
        f"Previous {days}d": [
            f"{kpis['revenue_prev']:,.0f}",  # brutto (no VAT data for prev period)
            "N/A",  # VAT not available for prev period from daily_metrics
            "N/A",
            f"{kpis['cogs_prev']:,.0f}",
            f"{kpis['fees_prev']:,.0f}",
            f"{kpis.get('shipping_prev', 0):,.0f}",
            "N/A",
            f"{kpis.get('storage_prev', 0):,.0f}",
            f"{kpis.get('ppc_prev', 0):,.0f}",
            "N/A",
            f"{kpis.get('refunds_prev', 0):,.0f}",
            f"{kpis['profit_prev']:,.0f}",
            f"{kpis['margin_prev']:.1f}%",
            f"{kpis.get('roi_prev', 0):.1f}%",
            f"{kpis['orders_prev']:,}",
            f"{kpis['units_prev']:,}",
            f"{kpis['aov_prev']:,.0f}",
        ],
        "Change %": [
            f"{kpis['revenue_delta']:+.1f}%",
            "",
            "",
            f"{kpis['cogs_delta']:+.1f}%",
            f"{kpis['fees_delta']:+.1f}%",
            f"{kpis.get('shipping_delta', 0):+.1f}%",
            "",
            f"{kpis.get('storage_delta', 0):+.1f}%",
            f"{kpis.get('ppc_delta', 0):+.1f}%",
            "",
            f"{kpis.get('refunds_delta', 0):+.1f}%",
            f"{kpis['profit_delta']:+.1f}%",
            f"{kpis['margin'] - kpis['margin_prev']:+.1f}pp",
            f"{kpis.get('roi', 0) - kpis.get('roi_prev', 0):+.1f}pp",
            f"{kpis['orders_delta']:+.1f}%",
            f"{kpis['units_delta']:+.1f}%",
            f"{kpis['aov_delta']:+.1f}%",
        ],
    }
    _comp_df = pd.DataFrame(comp_data)
    st.dataframe(_comp_df, use_container_width=True, hide_index=True)
    _comp_csv = _comp_df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", _comp_csv, "period_comparison.csv", "text/csv", key="dl_period_comp")

# --- 8. Revenue Composition: Organic vs Paid ---
st.markdown('<div class="section-header">REVENUE COMPOSITION: ORGANIC vs PAID</div>', unsafe_allow_html=True)

org_paid = load_organic_paid_split(days=days)
if not org_paid.empty:
    total_organic = org_paid["organic_pln"].sum()
    total_paid = org_paid["paid_pln"].sum()
    total_all = org_paid["revenue_pln"].sum()
    organic_pct = total_organic / total_all * 100 if total_all > 0 else 100
    paid_pct = 100 - organic_pct

    # Trend direction
    trend_str = None
    if len(org_paid) > 14:
        recent_org = org_paid.tail(7)["organic_pct"].mean()
        older_org = org_paid.head(7)["organic_pct"].mean()
        trend_str = "improving" if recent_org > older_org else "declining"

    rp1, rp2, rp3 = st.columns(3)
    rp1.metric("Organic Revenue", f"{total_organic:,.0f} PLN", delta=f"{organic_pct:.1f}%")
    rp2.metric("Paid Revenue", f"{total_paid:,.0f} PLN", delta=f"{paid_pct:.1f}%")
    rp3.metric("Organic Share Trend", f"{organic_pct:.1f}%", delta=trend_str)

    if total_paid == 0:
        st.info("No ad sales data. Import PPC CSV via `--ads-csv` to see organic/paid split.")
    else:
        o1, o2 = st.columns([2, 1])
        with o1:
            org_chart = org_paid.copy()
            org_chart["date"] = pd.to_datetime(org_chart["date"])
            fig_org = go.Figure()
            fig_org.add_trace(go.Scatter(
                x=org_chart["date"], y=org_chart["organic_pln"],
                mode="lines", name="Organic", stackgroup="one",
                line=dict(width=0.5, color=COLORS["success"]),
            ))
            fig_org.add_trace(go.Scatter(
                x=org_chart["date"], y=org_chart["paid_pln"],
                mode="lines", name="Paid", stackgroup="one",
                line=dict(width=0.5, color=COLORS["warning"]),
            ))
            fig_org.update_layout(height=300, title="Revenue: Organic vs Paid (PLN)",
                                  legend=dict(orientation="h", y=-0.15))
            st.plotly_chart(fig_org, use_container_width=True)

        with o2:
            fig_pie = go.Figure(go.Pie(
                labels=["Organic", "Paid"],
                values=[total_organic, total_paid],
                marker_colors=[COLORS["success"], COLORS["warning"]],
                hole=0.4, textinfo="percent+label",
            ))
            fig_pie.update_layout(height=300, showlegend=False, title="")
            st.plotly_chart(fig_pie, use_container_width=True)
else:
    st.info("No revenue data available.")
