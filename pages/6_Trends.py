"""Trends - MoM, seasonality, growth trajectory."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms, load_orders_enriched
from lib.metrics import daily_summary, calc_contribution_margins
from lib.charts import multi_line, heatmap, area_chart

setup_page("Trends")

st.markdown('<div class="section-header">TREND ANALYSIS</div>', unsafe_allow_html=True)

# Load all available data
platforms = load_platforms()
df = load_daily_metrics(days=180)

if df.empty:
    st.warning("No data available. Run ETL: python3.11 -m etl.run")
    st.stop()

df["platform"] = df["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))

# Platform filter
all_platforms = sorted(df["platform"].unique())
selected_platforms = st.sidebar.multiselect("PLATFORMS", all_platforms, default=all_platforms, key="trend_plats")
df = df[df["platform"].isin(selected_platforms)]

if df.empty:
    st.warning("No data for selected filters.")
    st.stop()

daily = daily_summary(df)
daily["date"] = pd.to_datetime(daily["date"])
daily = daily.sort_values("date")

# Load enriched order-level data for accurate profit/margin (net-revenue based)
_enriched_orders = pd.DataFrame()
try:
    _enriched_result = load_orders_enriched(days=180)
    if isinstance(_enriched_result, tuple) and len(_enriched_result) == 2:
        _enriched_orders = _enriched_result[0]
except Exception:
    pass

# Build daily profit from enriched orders (net-revenue based) if available
_daily_profit = pd.DataFrame()
if not _enriched_orders.empty and "profit_pln" in _enriched_orders.columns:
    _eo = _enriched_orders.copy()
    _eo["_date"] = pd.to_datetime(_eo["order_date"], errors="coerce").dt.date.astype(str)
    _daily_profit = _eo.groupby("_date").agg(
        profit_net=("profit_pln", "sum"),
        revenue_net=("revenue_net_pln", "sum"),
        revenue_brutto=("revenue_pln", "sum"),
        total_costs=("total_costs_pln", "sum"),
    ).reset_index().rename(columns={"_date": "date"})
    _daily_profit["date"] = pd.to_datetime(_daily_profit["date"])
    _daily_profit["margin_net_pct"] = np.where(
        _daily_profit["revenue_net"] > 0,
        _daily_profit["profit_net"] / _daily_profit["revenue_net"] * 100,
        0,
    )
    # Rolling averages for profit
    _daily_profit = _daily_profit.sort_values("date")
    _daily_profit["profit_net_7d"] = _daily_profit["profit_net"].rolling(7, min_periods=1).mean()
    _daily_profit["profit_net_30d"] = _daily_profit["profit_net"].rolling(30, min_periods=1).mean()

# --- 1. Revenue decomposition ---
st.markdown('<div class="section-header">REVENUE DECOMPOSITION</div>', unsafe_allow_html=True)

if not daily.empty:
    y_cols = ["revenue_pln"]
    names = ["Revenue brutto (daily)"]
    colors = [COLORS["primary"]]

    if "revenue_pln_7d" in daily.columns:
        y_cols.append("revenue_pln_7d")
        names.append("7d MA")
        colors.append(COLORS["success"])
    if "revenue_pln_30d" in daily.columns:
        y_cols.append("revenue_pln_30d")
        names.append("30d MA")
        colors.append(COLORS["warning"])

    fig_decomp = multi_line(daily, "date", y_cols, colors=colors, names=names, height=400)
    st.plotly_chart(fig_decomp, use_container_width=True)

# --- 1b. Profit trend (net-revenue based) ---
if not _daily_profit.empty:
    st.markdown('<div class="section-header">PROFIT TREND (NET REVENUE BASED)</div>', unsafe_allow_html=True)

    _profit_cols = ["profit_net"]
    _profit_names = ["Profit (daily)"]
    _profit_colors = [COLORS.get("cm3", COLORS["success"])]

    if "profit_net_7d" in _daily_profit.columns:
        _profit_cols.append("profit_net_7d")
        _profit_names.append("Profit 7d MA")
        _profit_colors.append(COLORS.get("info", COLORS["primary"]))
    if "profit_net_30d" in _daily_profit.columns:
        _profit_cols.append("profit_net_30d")
        _profit_names.append("Profit 30d MA")
        _profit_colors.append(COLORS["warning"])

    fig_profit = multi_line(_daily_profit, "date", _profit_cols, colors=_profit_colors, names=_profit_names, height=400)
    st.plotly_chart(fig_profit, use_container_width=True)

# --- 2. Month-over-month comparison ---
st.markdown('<div class="section-header">MONTH-OVER-MONTH COMPARISON</div>', unsafe_allow_html=True)

daily["month"] = daily["date"].dt.to_period("M").astype(str)
available_months = sorted(daily["month"].unique(), reverse=True)

if len(available_months) >= 2:
    col_m1, col_m2 = st.columns(2)
    with col_m1:
        month1 = st.selectbox("Month 1 (newer)", available_months, index=0, key="m1")
    with col_m2:
        month2 = st.selectbox("Month 2 (older)", available_months, index=min(1, len(available_months) - 1), key="m2")

    m1_data = daily[daily["month"] == month1].copy()
    m2_data = daily[daily["month"] == month2].copy()

    # Merge enriched profit data if available
    def _merge_enriched(mdf, month_str):
        if _daily_profit.empty:
            return mdf
        _ep = _daily_profit.copy()
        _ep["month"] = _ep["date"].dt.to_period("M").astype(str)
        _ep_month = _ep[_ep["month"] == month_str]
        if _ep_month.empty:
            return mdf
        _ep_month = _ep_month.rename(columns={"date": "_ep_date"})
        mdf = mdf.copy()
        mdf["_date_str"] = mdf["date"].dt.strftime("%Y-%m-%d")
        _ep_month = _ep_month.copy()
        _ep_month["_date_str"] = _ep_month["_ep_date"].dt.strftime("%Y-%m-%d")
        mdf = mdf.merge(
            _ep_month[["_date_str", "profit_net", "revenue_net", "total_costs", "margin_net_pct"]],
            on="_date_str", how="left",
        )
        for c in ["profit_net", "revenue_net", "total_costs", "margin_net_pct"]:
            if c in mdf.columns:
                mdf[c] = mdf[c].fillna(0)
        mdf.drop(columns=["_date_str"], inplace=True, errors="ignore")
        return mdf

    m1_data = _merge_enriched(m1_data, month1)
    m2_data = _merge_enriched(m2_data, month2)

    # Comparison table
    _has_enriched = "profit_net" in m1_data.columns and m1_data["profit_net"].abs().sum() > 0

    def month_agg(mdf):
        result = {
            "Revenue (brutto)": mdf["revenue_pln"].sum(),
        }
        if _has_enriched and "revenue_net" in mdf.columns:
            result["Revenue (netto)"] = mdf["revenue_net"].sum()
        if _has_enriched and "profit_net" in mdf.columns:
            _profit = mdf["profit_net"].sum()
            _rev_net = mdf["revenue_net"].sum() if "revenue_net" in mdf.columns else 0
            result["Profit"] = _profit
            result["Margin %"] = (_profit / _rev_net * 100) if _rev_net > 0 else 0
        else:
            _cm3 = mdf["cm3"].sum() if "cm3" in mdf.columns else 0
            _rev = mdf["revenue_pln"].sum()
            result["Profit (CM3)"] = _cm3
            result["Margin %"] = (_cm3 / _rev * 100) if _rev > 0 and _cm3 != 0 else 0
        result["Orders"] = int(mdf["orders_count"].sum())
        result["Units"] = int(mdf["units"].sum())
        return result

    s1 = month_agg(m1_data)
    s2 = month_agg(m2_data)

    comp_rows = []
    for key in s1:
        v1 = s1[key]
        v2 = s2[key]
        if key == "Margin %":
            delta = f"{v1 - v2:+.1f}pp"
            v1_fmt = f"{v1:.1f}%"
            v2_fmt = f"{v2:.1f}%"
        elif isinstance(v1, float):
            delta = f"{(v1 - v2) / v2 * 100:+.1f}%" if v2 != 0 else "N/A"
            v1_fmt = f"{v1:,.0f}"
            v2_fmt = f"{v2:,.0f}"
        else:
            delta = f"{(v1 - v2) / v2 * 100:+.1f}%" if v2 != 0 else "N/A"
            v1_fmt = f"{v1:,}"
            v2_fmt = f"{v2:,}"
        comp_rows.append({"Metric": key, month1: v1_fmt, month2: v2_fmt, "Change": delta})

    _mom_df = pd.DataFrame(comp_rows)
    st.dataframe(_mom_df, use_container_width=True, hide_index=True)
    _mom_csv = _mom_df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", _mom_csv, "mom_comparison.csv", "text/csv", key="dl_mom_comp")

    # Daily overlay
    import plotly.graph_objects as go
    m1_daily = m1_data.copy()
    m2_daily = m2_data.copy()
    m1_daily["day"] = m1_daily["date"].dt.day
    m2_daily["day"] = m2_daily["date"].dt.day

    m1_by_day = m1_daily.groupby("day")["revenue_pln"].sum().reset_index()
    m2_by_day = m2_daily.groupby("day")["revenue_pln"].sum().reset_index()

    fig_mom = go.Figure()
    fig_mom.add_trace(go.Scatter(
        x=m1_by_day["day"], y=m1_by_day["revenue_pln"],
        name=month1, mode="lines+markers",
        line=dict(color=COLORS["primary"], width=2.5),
    ))
    fig_mom.add_trace(go.Scatter(
        x=m2_by_day["day"], y=m2_by_day["revenue_pln"],
        name=month2, mode="lines+markers",
        line=dict(color=COLORS["muted"], width=2, dash="dash"),
    ))
    fig_mom.update_layout(
        height=350, xaxis_title="Day of Month", yaxis_title="Revenue (PLN)",
        title="Daily Revenue Overlay",
    )
    st.plotly_chart(fig_mom, use_container_width=True)
else:
    st.info("Need at least 2 months of data for comparison.")

# --- 3. Day-of-week pattern ---
st.markdown('<div class="section-header">DAY-OF-WEEK PATTERN</div>', unsafe_allow_html=True)

if not daily.empty:
    daily["weekday"] = daily["date"].dt.dayofweek
    daily["weekday_name"] = daily["date"].dt.strftime("%a")

    # Order by weekday (Mon=0 to Sun=6)
    weekday_agg = daily.groupby(["weekday", "weekday_name"]).agg(
        avg_revenue=("revenue_pln", "mean"),
        avg_orders=("orders_count", "mean"),
        avg_units=("units", "mean"),
    ).reset_index().sort_values("weekday")

    col_wd1, col_wd2 = st.columns(2)

    with col_wd1:
        import plotly.graph_objects as go
        fig_wd_rev = go.Figure(go.Bar(
            x=weekday_agg["weekday_name"], y=weekday_agg["avg_revenue"],
            marker_color=COLORS["primary"],
            text=weekday_agg["avg_revenue"].map(lambda x: f"{x:,.0f}"),
            textposition="outside",
        ))
        fig_wd_rev.update_layout(height=320, title="Avg Revenue by Day of Week", yaxis_title="PLN")
        st.plotly_chart(fig_wd_rev, use_container_width=True)

    with col_wd2:
        fig_wd_ord = go.Figure(go.Bar(
            x=weekday_agg["weekday_name"], y=weekday_agg["avg_orders"],
            marker_color=COLORS["info"],
            text=weekday_agg["avg_orders"].map(lambda x: f"{x:.0f}"),
            textposition="outside",
        ))
        fig_wd_ord.update_layout(height=320, title="Avg Orders by Day of Week", yaxis_title="Orders")
        st.plotly_chart(fig_wd_ord, use_container_width=True)

    # Weekly heatmap: week number x weekday
    daily["week_num"] = daily["date"].dt.isocalendar().week.astype(int)
    week_heatmap = daily.pivot_table(
        index="weekday_name", columns="week_num", values="revenue_pln", aggfunc="sum"
    ).fillna(0)

    # Reorder rows to Mon-Sun
    day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    week_heatmap = week_heatmap.reindex([d for d in day_order if d in week_heatmap.index])

    if not week_heatmap.empty:
        fig_week_hm = heatmap(
            z_data=week_heatmap.values.tolist(),
            x_labels=[f"W{w}" for w in week_heatmap.columns.tolist()],
            y_labels=week_heatmap.index.tolist(),
            title="Revenue Heatmap (Weekday x Week)",
            height=280,
        )
        st.plotly_chart(fig_week_hm, use_container_width=True)

# --- 4. Growth trajectory ---
st.markdown('<div class="section-header">GROWTH TRAJECTORY</div>', unsafe_allow_html=True)

if not daily.empty and len(daily) >= 14:
    # Rolling 30d revenue
    daily_sorted = daily.sort_values("date").copy()
    daily_sorted["rolling_30d_rev"] = daily_sorted["revenue_pln"].rolling(30, min_periods=7).sum()

    # Linear regression on rolling revenue
    valid = daily_sorted[daily_sorted["rolling_30d_rev"].notna()].copy()
    if len(valid) >= 7:
        x_num = np.arange(len(valid))
        y_vals = valid["rolling_30d_rev"].values

        # Polyfit
        coeffs = np.polyfit(x_num, y_vals, 1)
        trend_line = np.polyval(coeffs, x_num)

        # Confidence band (1 std)
        residuals = y_vals - trend_line
        std_resid = np.std(residuals)

        import plotly.graph_objects as go
        fig_growth = go.Figure()

        # Confidence band
        fig_growth.add_trace(go.Scatter(
            x=valid["date"], y=trend_line + std_resid,
            mode="lines", line=dict(width=0), showlegend=False,
        ))
        fig_growth.add_trace(go.Scatter(
            x=valid["date"], y=trend_line - std_resid,
            mode="lines", line=dict(width=0), showlegend=False,
            fill="tonexty",
            fillcolor=f"rgba(59,130,246,0.1)",
        ))

        # Actual rolling revenue
        fig_growth.add_trace(go.Scatter(
            x=valid["date"], y=valid["rolling_30d_rev"],
            mode="lines", name="Rolling 30d Revenue",
            line=dict(color=COLORS["primary"], width=2),
        ))

        # Trend line
        fig_growth.add_trace(go.Scatter(
            x=valid["date"], y=trend_line,
            mode="lines", name="Linear Trend",
            line=dict(color=COLORS["warning"], width=1.5, dash="dash"),
        ))

        # Growth rate annotation
        daily_growth = coeffs[0]
        monthly_growth_pct = (daily_growth * 30 / np.mean(y_vals) * 100) if np.mean(y_vals) > 0 else 0

        fig_growth.update_layout(
            height=400,
            title=f"Rolling 30d Revenue (trend: {monthly_growth_pct:+.1f}%/month)",
            yaxis_title="PLN (rolling 30d)",
        )
        st.plotly_chart(fig_growth, use_container_width=True)

        # Growth stats
        g1, g2, g3, g4 = st.columns(4)
        g1.metric("Revenue Trend", f"{monthly_growth_pct:+.1f}%/mo")
        g2.metric("Current 30d Revenue", f"{valid['rolling_30d_rev'].iloc[-1]:,.0f} PLN")
        g3.metric("Trend Fit (R2)", f"{1 - np.var(residuals) / np.var(y_vals):.3f}" if np.var(y_vals) > 0 else "N/A")

        # Show rolling 30d profit from enriched data if available
        if not _daily_profit.empty:
            _dp_sorted = _daily_profit.sort_values("date").copy()
            _dp_sorted["rolling_30d_profit"] = _dp_sorted["profit_net"].rolling(30, min_periods=7).sum()
            _dp_valid = _dp_sorted[_dp_sorted["rolling_30d_profit"].notna()]
            if not _dp_valid.empty:
                g4.metric("Current 30d Profit", f"{_dp_valid['rolling_30d_profit'].iloc[-1]:,.0f} PLN")
            else:
                g4.metric("Current 30d Profit", "N/A")
        else:
            g4.metric("Current 30d Profit", "N/A")

    # --- Profit growth trajectory (net-revenue based) ---
    if not _daily_profit.empty and len(_daily_profit) >= 14:
        _dp_sorted = _daily_profit.sort_values("date").copy()
        _dp_sorted["rolling_30d_profit"] = _dp_sorted["profit_net"].rolling(30, min_periods=7).sum()
        _dp_valid = _dp_sorted[_dp_sorted["rolling_30d_profit"].notna()].copy()

        if len(_dp_valid) >= 7:
            _x_num = np.arange(len(_dp_valid))
            _y_profit = _dp_valid["rolling_30d_profit"].values
            _coeffs_p = np.polyfit(_x_num, _y_profit, 1)
            _trend_p = np.polyval(_coeffs_p, _x_num)
            _resid_p = _y_profit - _trend_p
            _std_p = np.std(_resid_p)
            _monthly_growth_p = (_coeffs_p[0] * 30 / np.mean(_y_profit) * 100) if np.mean(_y_profit) != 0 else 0

            import plotly.graph_objects as go
            fig_profit_growth = go.Figure()
            fig_profit_growth.add_trace(go.Scatter(
                x=_dp_valid["date"], y=_trend_p + _std_p,
                mode="lines", line=dict(width=0), showlegend=False,
            ))
            fig_profit_growth.add_trace(go.Scatter(
                x=_dp_valid["date"], y=_trend_p - _std_p,
                mode="lines", line=dict(width=0), showlegend=False,
                fill="tonexty", fillcolor="rgba(16,185,129,0.1)",
            ))
            fig_profit_growth.add_trace(go.Scatter(
                x=_dp_valid["date"], y=_dp_valid["rolling_30d_profit"],
                mode="lines", name="Rolling 30d Profit",
                line=dict(color=COLORS.get("cm3", COLORS["success"]), width=2),
            ))
            fig_profit_growth.add_trace(go.Scatter(
                x=_dp_valid["date"], y=_trend_p,
                mode="lines", name="Linear Trend",
                line=dict(color=COLORS["warning"], width=1.5, dash="dash"),
            ))
            fig_profit_growth.update_layout(
                height=400,
                title=f"Rolling 30d Profit (trend: {_monthly_growth_p:+.1f}%/month)",
                yaxis_title="PLN (rolling 30d profit)",
            )
            st.plotly_chart(fig_profit_growth, use_container_width=True)
