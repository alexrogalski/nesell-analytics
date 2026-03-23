"""Markets - Marketplace comparison, FX, heatmaps."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms, load_fx_rates, load_marketplace_pnl, load_orders_enriched
from lib.metrics import calc_contribution_margins, platform_summary
from lib.charts import heatmap, treemap, bar_chart


def _safe_col(df, col, default=0):
    """Get column from DataFrame, returning default Series if missing."""
    if col in df.columns:
        return df[col]
    return pd.Series(default, index=df.index)

setup_page("Markets")

st.markdown('<div class="section-header">MARKETPLACE INTELLIGENCE</div>', unsafe_allow_html=True)

# Sidebar
period_options = {7: "7D", 14: "14D", 30: "30D", 60: "60D", 90: "90D"}
days = st.sidebar.selectbox(
    "PERIOD", list(period_options.keys()), index=2,
    format_func=lambda x: period_options[x], key="mkt_period",
)

platforms = load_platforms()
df = load_daily_metrics(days=days * 2)

if df.empty:
    st.warning("No data available. Run ETL: python3.11 -m etl.run")
    st.stop()

df["platform"] = df["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))
df["date"] = pd.to_datetime(df["date"])

now = datetime.now()
current_start = now - timedelta(days=days)
prev_start = current_start - timedelta(days=days)

df_current = df[df["date"] >= current_start].copy()
df_prev = df[(df["date"] >= prev_start) & (df["date"] < current_start)].copy()

if df_current.empty:
    st.warning("No data for current period.")
    st.stop()

# --- 1. KPI grid per marketplace ---
st.markdown('<div class="section-header">MARKETPLACE KPIs</div>', unsafe_allow_html=True)

plat_current = platform_summary(df_current, platforms)
plat_prev = platform_summary(df_prev, platforms) if not df_prev.empty else pd.DataFrame()

if not plat_current.empty:
    # Filter out low-activity platforms to prevent misleading percentages
    # Require at least 2 orders OR 500 PLN revenue in the period
    # Using OR so high-value but low-frequency platforms (e.g. Allegro) still show
    plat_current = plat_current[
        (plat_current["orders_count"] >= 2) | (plat_current["revenue_pln"] >= 500)
    ].copy()

if not plat_current.empty:
    # Create columns for each marketplace (max 6 per row)
    plats = plat_current.sort_values("revenue_pln", ascending=False)
    n_plats = len(plats)
    cols_per_row = min(6, n_plats)

    for start_idx in range(0, n_plats, cols_per_row):
        batch = plats.iloc[start_idx:start_idx + cols_per_row]
        cols = st.columns(len(batch))
        for col, (_, row) in zip(cols, batch.iterrows()):
            with col:
                platform_name = row.get("platform", "?")
                rev = row["revenue_pln"]
                cm3 = row.get("cm3", 0)
                margin = row.get("cm3_pct", 0)
                orders = int(row.get("orders_count", 0))

                # Delta calculation
                delta_str = None
                if not plat_prev.empty and "platform" in plat_prev.columns:
                    prev_row = plat_prev[plat_prev["platform"] == platform_name]
                    if not prev_row.empty:
                        prev_rev = prev_row.iloc[0]["revenue_pln"]
                        if prev_rev > 0:
                            delta_str = f"{(rev - prev_rev) / prev_rev * 100:+.0f}%"

                st.markdown(f"**{platform_name}**")
                st.metric("Revenue", f"{rev:,.0f}", delta=delta_str)
                st.metric("CM3", f"{cm3:,.0f}", delta=f"{margin:.1f}%")
                st.metric("Orders", f"{orders:,}")

# --- 2. Revenue heatmap: Marketplace x Week ---
st.markdown('<div class="section-header">REVENUE HEATMAP (MARKETPLACE x WEEK)</div>', unsafe_allow_html=True)

if not df_current.empty:
    hm_data = df_current.copy()
    hm_data["week"] = hm_data["date"].dt.isocalendar().week.astype(int)
    hm_data["year_week"] = hm_data["date"].dt.strftime("W%V")

    hm_pivot = hm_data.groupby(["platform", "year_week"])["revenue_pln"].sum().reset_index()
    hm_wide = hm_pivot.pivot(index="platform", columns="year_week", values="revenue_pln").fillna(0)

    if not hm_wide.empty:
        # Sort columns chronologically
        hm_wide = hm_wide[sorted(hm_wide.columns)]

        fig_hm = heatmap(
            z_data=hm_wide.values.tolist(),
            x_labels=hm_wide.columns.tolist(),
            y_labels=hm_wide.index.tolist(),
            title="",
            height=max(250, len(hm_wide) * 35),
        )
        st.plotly_chart(fig_hm, use_container_width=True)

# --- 3. Growth rates per marketplace ---
st.markdown('<div class="section-header">REVENUE GROWTH vs PREVIOUS PERIOD</div>', unsafe_allow_html=True)

if not plat_current.empty and not plat_prev.empty and "platform" in plat_prev.columns:
    growth_data = []
    for _, row in plat_current.iterrows():
        plat_name = row.get("platform", "?")
        curr_rev = row["revenue_pln"]
        prev_match = plat_prev[plat_prev["platform"] == plat_name]
        prev_rev = prev_match.iloc[0]["revenue_pln"] if not prev_match.empty else 0
        growth = ((curr_rev - prev_rev) / prev_rev * 100) if prev_rev > 0 else 0
        growth_data.append({"Platform": plat_name, "Growth %": growth, "Current": curr_rev, "Previous": prev_rev})

    growth_df = pd.DataFrame(growth_data).sort_values("Growth %", ascending=True)

    if not growth_df.empty:
        import plotly.graph_objects as go
        colors = [COLORS["success"] if g >= 0 else COLORS["danger"] for g in growth_df["Growth %"]]
        fig_growth = go.Figure(go.Bar(
            y=growth_df["Platform"], x=growth_df["Growth %"],
            orientation="h", marker_color=colors,
            text=growth_df["Growth %"].map(lambda x: f"{x:+.1f}%"),
            textposition="outside",
        ))
        fig_growth.update_layout(height=max(250, len(growth_df) * 40), title="", xaxis_title="Growth %")
        st.plotly_chart(fig_growth, use_container_width=True)

# --- 4. FX rates chart ---
st.markdown('<div class="section-header">FX RATES TREND</div>', unsafe_allow_html=True)

fx_df = load_fx_rates(days=days)
if not fx_df.empty:
    fx_df["date"] = pd.to_datetime(fx_df["date"])
    # Get available currency pairs
    # DB column is rate_pln; normalize to "rate" for pivot
    if "rate_pln" in fx_df.columns:
        fx_df.rename(columns={"rate_pln": "rate"}, inplace=True)
    if "currency" in fx_df.columns and "rate" in fx_df.columns:
        fx_pivot = fx_df.pivot(index="date", columns="currency", values="rate")
        fx_cols = [c for c in fx_pivot.columns if c in ["EUR", "SEK", "GBP", "USD"]]
        if fx_cols:
            from lib.charts import multi_line
            fx_plot = fx_pivot[fx_cols].reset_index()
            fig_fx = multi_line(
                fx_plot, "date", fx_cols,
                names=[f"{c}/PLN" for c in fx_cols],
                title="", height=300,
            )
            st.plotly_chart(fig_fx, use_container_width=True)
    else:
        st.info("FX data format not recognized.")
else:
    st.info("No FX data available. Run: python3.11 -m etl.run --fx")

# --- 5. Marketplace share treemap ---
st.markdown('<div class="section-header">MARKETPLACE SHARE</div>', unsafe_allow_html=True)

if not plat_current.empty:
    plat_tree = plat_current[plat_current["revenue_pln"] > 0].copy()
    if not plat_tree.empty and "platform" in plat_tree.columns:
        labels = ["All"] + plat_tree["platform"].tolist()
        parents = [""] + ["All"] * len(plat_tree)
        values = [plat_tree["revenue_pln"].sum()] + plat_tree["revenue_pln"].tolist()

        fig_tree = treemap(labels, parents, values, title="", height=400)
        st.plotly_chart(fig_tree, use_container_width=True)

# --- 6. P&L by Marketplace ---
st.markdown('<div class="section-header">P&L BY MARKETPLACE</div>', unsafe_allow_html=True)

# Load enriched order data for detailed cost breakdown
_mkt_enriched = False
_mkt_enr_agg = pd.DataFrame()

with st.spinner("Loading enriched P&L data..."):
    _mkt_enr_result = load_orders_enriched(days=days)

if isinstance(_mkt_enr_result, tuple) and len(_mkt_enr_result) == 2:
    _mkt_ord_df, _ = _mkt_enr_result
    if not _mkt_ord_df.empty and "platform_name" in _mkt_ord_df.columns:
        _mkt_enriched = True
        _enr_agg_dict = {"revenue_pln": "sum", "id": "count"}
        for _ec in ["revenue_net_pln", "vat_amount_pln", "cogs_pln", "fees_pln",
                     "shipping_pln", "fulfillment_cost_pln", "ppc_cost_pln",
                     "storage_fee_pln", "fx_spread_pln", "total_costs_pln", "profit_pln"]:
            if _ec in _mkt_ord_df.columns:
                _enr_agg_dict[_ec] = "sum"
        _mkt_enr_agg = _mkt_ord_df.groupby("platform_name").agg(_enr_agg_dict).reset_index()
        _mkt_enr_agg.rename(columns={"platform_name": "marketplace", "id": "orders_count"}, inplace=True)
        _mkt_enr_agg = _mkt_enr_agg.sort_values("revenue_pln", ascending=False).reset_index(drop=True)

        # Compute margin based on net revenue
        if "revenue_net_pln" in _mkt_enr_agg.columns and "profit_pln" in _mkt_enr_agg.columns:
            _mkt_enr_agg["margin_pct"] = np.where(
                _mkt_enr_agg["revenue_net_pln"] > 0,
                _mkt_enr_agg["profit_pln"] / _mkt_enr_agg["revenue_net_pln"] * 100,
                0,
            )
        else:
            _mkt_enr_agg["margin_pct"] = 0.0

# Fallback to basic marketplace P&L if enriched data unavailable
mkt_pnl = load_marketplace_pnl(days=days)

if _mkt_enriched and not _mkt_enr_agg.empty:
    _display_df = _mkt_enr_agg

    # KPI cards for top marketplaces (show net revenue and profit)
    top_mkts = _display_df.head(6)
    mkt_cols = st.columns(min(6, len(top_mkts)))
    for _col, (_, _row) in zip(mkt_cols, top_mkts.iterrows()):
        with _col:
            st.markdown(f"**{_row['marketplace']}**")
            _rev_net = _row.get("revenue_net_pln", _row["revenue_pln"])
            _profit = _row.get("profit_pln", 0)
            st.metric("Revenue (net)", f"{_rev_net:,.0f} PLN")
            st.metric("Profit", f"{_profit:,.0f} PLN",
                      delta=f"{_row['margin_pct']:.1f}% margin")
            st.metric("Orders", f"{int(_row['orders_count']):,}")

    # Full P&L table with new cost columns
    _tbl_cols = ["marketplace", "revenue_pln"]
    _tbl_names = ["Marketplace", "Rev (gross)"]

    if "revenue_net_pln" in _display_df.columns:
        _tbl_cols.append("revenue_net_pln")
        _tbl_names.append("Rev (net)")
    if "vat_amount_pln" in _display_df.columns:
        _tbl_cols.append("vat_amount_pln")
        _tbl_names.append("VAT")
    if "cogs_pln" in _display_df.columns:
        _tbl_cols.append("cogs_pln")
        _tbl_names.append("COGS")
    if "fees_pln" in _display_df.columns:
        _tbl_cols.append("fees_pln")
        _tbl_names.append("Fees")
    if "shipping_pln" in _display_df.columns:
        _tbl_cols.append("shipping_pln")
        _tbl_names.append("Shipping")
    if "fulfillment_cost_pln" in _display_df.columns:
        _tbl_cols.append("fulfillment_cost_pln")
        _tbl_names.append("3PL")
    if "ppc_cost_pln" in _display_df.columns:
        _tbl_cols.append("ppc_cost_pln")
        _tbl_names.append("PPC")
    if "storage_fee_pln" in _display_df.columns:
        _tbl_cols.append("storage_fee_pln")
        _tbl_names.append("Storage")
    if "fx_spread_pln" in _display_df.columns:
        _tbl_cols.append("fx_spread_pln")
        _tbl_names.append("FX Spread")
    if "profit_pln" in _display_df.columns:
        _tbl_cols.append("profit_pln")
        _tbl_names.append("Profit")

    _tbl_cols.extend(["margin_pct", "orders_count"])
    _tbl_names.extend(["Margin %", "Orders"])

    pnl_display = _display_df[_tbl_cols].copy()
    pnl_display.columns = _tbl_names

    # Format numeric columns
    _money_cols = [c for c in _tbl_names if c not in ("Marketplace", "Margin %", "Orders")]
    for _c in _money_cols:
        pnl_display[_c] = pnl_display[_c].map(lambda x: f"{x:,.0f}")
    pnl_display["Margin %"] = pnl_display["Margin %"].map(lambda x: f"{x:.1f}%")
    pnl_display["Orders"] = pnl_display["Orders"].map(lambda x: f"{int(x):,}")

    st.dataframe(pnl_display, use_container_width=True, hide_index=True)

    _mkt_csv = _display_df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", _mkt_csv, "marketplace_pnl.csv", "text/csv", key="dl_mkt_pnl")

    # Horizontal stacked bar: full cost breakdown per marketplace
    import plotly.graph_objects as go
    bar_data = _display_df.sort_values("revenue_pln", ascending=True)
    fig_mkt = go.Figure()

    # Cost components in stacking order
    _bar_components = [
        ("cogs_pln", "COGS", COLORS["danger"]),
        ("fees_pln", "Fees", COLORS["warning"]),
        ("shipping_pln", "Shipping", COLORS["info"]),
        ("fulfillment_cost_pln", "3PL", "#a78bfa"),
        ("ppc_cost_pln", "PPC", "#f472b6"),
        ("storage_fee_pln", "Storage", "#fb923c"),
        ("fx_spread_pln", "FX Spread", "#94a3b8"),
        ("profit_pln", "Profit", COLORS["success"]),
    ]

    for _bcol, _bname, _bcolor in _bar_components:
        if _bcol in bar_data.columns and bar_data[_bcol].sum() != 0:
            fig_mkt.add_trace(go.Bar(
                y=bar_data["marketplace"], x=bar_data[_bcol],
                name=_bname, orientation="h", marker_color=_bcolor,
            ))

    fig_mkt.update_layout(
        barmode="stack", height=max(250, len(bar_data) * 45),
        title="Net Revenue Breakdown by Marketplace",
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig_mkt, use_container_width=True)

elif not mkt_pnl.empty:
    # Fallback: use basic marketplace P&L (no enriched columns)
    top_mkts = mkt_pnl.head(6)
    mkt_cols = st.columns(min(6, len(top_mkts)))
    for _col, (_, _row) in zip(mkt_cols, top_mkts.iterrows()):
        with _col:
            st.markdown(f"**{_row['marketplace']}**")
            st.metric("Revenue", f"{_row['revenue_pln']:,.0f} PLN")
            st.metric("Profit", f"{_row['gross_profit']:,.0f} PLN",
                      delta=f"{_row['margin_pct']:.1f}% margin")

    pnl_display = mkt_pnl[["marketplace", "revenue_pln", "cogs", "platform_fees",
                            "shipping_cost", "gross_profit", "margin_pct",
                            "orders_count", "units"]].copy()
    pnl_display.columns = ["Marketplace", "Revenue", "COGS", "Fees", "Shipping",
                            "Profit", "Margin %", "Orders", "Units"]
    for _c in ["Revenue", "COGS", "Fees", "Shipping", "Profit"]:
        pnl_display[_c] = pnl_display[_c].map(lambda x: f"{x:,.0f}")
    pnl_display["Margin %"] = pnl_display["Margin %"].map(lambda x: f"{x:.1f}%")

    st.dataframe(pnl_display, use_container_width=True, hide_index=True)

    _mkt_csv = mkt_pnl.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", _mkt_csv, "marketplace_pnl.csv", "text/csv", key="dl_mkt_pnl")

    import plotly.graph_objects as go
    bar_data = mkt_pnl.sort_values("revenue_pln", ascending=True)
    fig_mkt = go.Figure()
    fig_mkt.add_trace(go.Bar(
        y=bar_data["marketplace"], x=bar_data["cogs"],
        name="COGS", orientation="h", marker_color=COLORS["danger"],
    ))
    fig_mkt.add_trace(go.Bar(
        y=bar_data["marketplace"], x=bar_data["platform_fees"],
        name="Fees", orientation="h", marker_color=COLORS["warning"],
    ))
    fig_mkt.add_trace(go.Bar(
        y=bar_data["marketplace"], x=bar_data["shipping_cost"],
        name="Shipping", orientation="h", marker_color=COLORS["info"],
    ))
    fig_mkt.add_trace(go.Bar(
        y=bar_data["marketplace"], x=bar_data["gross_profit"],
        name="Profit", orientation="h", marker_color=COLORS["success"],
    ))
    fig_mkt.update_layout(
        barmode="stack", height=max(250, len(bar_data) * 40),
        title="Revenue Breakdown by Marketplace",
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig_mkt, use_container_width=True)
else:
    st.info("No marketplace P&L data available.")
