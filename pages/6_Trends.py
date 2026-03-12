"""Trends - MoM, seasonality, growth trajectory."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics, load_platforms
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

# --- 1. Revenue decomposition ---
st.markdown('<div class="section-header">REVENUE DECOMPOSITION</div>', unsafe_allow_html=True)

if not daily.empty:
    y_cols = ["revenue_pln"]
    names = ["Revenue (daily)"]
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

    # Comparison table
    def month_agg(mdf):
        return {
            "Revenue": mdf["revenue_pln"].sum(),
            "CM3": mdf["cm3"].sum() if "cm3" in mdf.columns else 0,
            "Margin %": (mdf["cm3"].sum() / mdf["revenue_pln"].sum() * 100) if mdf["revenue_pln"].sum() > 0 and "cm3" in mdf.columns else 0,
            "Orders": int(mdf["orders_count"].sum()),
            "Units": int(mdf["units"].sum()),
        }

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

    st.dataframe(pd.DataFrame(comp_rows), use_container_width=True, hide_index=True)

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
        g1, g2, g3 = st.columns(3)
        g1.metric("Monthly Trend", f"{monthly_growth_pct:+.1f}%")
        g2.metric("Current 30d Revenue", f"{valid['rolling_30d_rev'].iloc[-1]:,.0f} PLN")
        g3.metric("Trend Fit (R2)", f"{1 - np.var(residuals) / np.var(y_vals):.3f}" if np.var(y_vals) > 0 else "N/A")
