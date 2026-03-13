"""Markets - Marketplace comparison, FX, heatmaps."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms, load_fx_rates
from lib.metrics import calc_contribution_margins, platform_summary
from lib.charts import heatmap, treemap, bar_chart

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
    # Require at least 5 orders AND 500 PLN revenue in the period
    plat_current = plat_current[
        (plat_current["orders_count"] >= 5) & (plat_current["revenue_pln"] >= 500)
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
