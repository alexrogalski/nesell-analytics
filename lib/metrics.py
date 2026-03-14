"""KPI calculation helpers for nesell-analytics dashboard."""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Platform fee rates (for reference only; actual fee calculation uses
# FBA/FBM-aware rates in lib/data.py and etl/aggregator.py)
# Amazon FBA: ~34.73% (referral ~15% + FBA fulfillment ~18-19% + other ~1%)
# Amazon FBM: ~15.45% (referral fee only)
PLATFORM_FEES = {
    "amazon_de": 0.3473, "amazon_fr": 0.3473, "amazon_it": 0.3473,
    "amazon_es": 0.3473, "amazon_nl": 0.3473, "amazon_se": 0.3473,
    "amazon_pl": 0.1867, "amazon_be": 0.3473,
    "allegro": 0.10,
    "temu": 0.0,
    "empik": 0.15,
}


def calc_period_kpis(df, days):
    """Calculate KPIs for a period and its comparison period."""
    if df.empty:
        return {}

    now = datetime.now().date()
    current_start = now - timedelta(days=days)
    prev_start = current_start - timedelta(days=days)

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    current = df[(df["date"] >= current_start) & (df["date"] <= now)]
    previous = df[(df["date"] >= prev_start) & (df["date"] < current_start)]

    def agg(subset):
        result = {
            "revenue": subset["revenue_pln"].sum() if "revenue_pln" in subset.columns else 0,
            "cogs": subset["cogs"].sum() if "cogs" in subset.columns else 0,
            "fees": subset["fees"].sum() if "fees" in subset.columns else 0,
            "shipping": subset["shipping_cost"].sum() if "shipping_cost" in subset.columns else 0,
            "profit": subset["profit"].sum() if "profit" in subset.columns else 0,
            "orders": int(subset["orders_count"].sum()) if "orders_count" in subset.columns else 0,
            "units": int(subset["units"].sum()) if "units" in subset.columns else 0,
        }
        return result

    c = agg(current)
    p = agg(previous)

    c["margin"] = (c["profit"] / c["revenue"] * 100) if c["revenue"] > 0 else 0
    c["aov"] = (c["revenue"] / c["orders"]) if c["orders"] > 0 else 0
    c["roi"] = (c["profit"] / c["cogs"] * 100) if c["cogs"] > 0 else 0
    p["margin"] = (p["profit"] / p["revenue"] * 100) if p["revenue"] > 0 else 0
    p["aov"] = (p["revenue"] / p["orders"]) if p["orders"] > 0 else 0
    p["roi"] = (p["profit"] / p["cogs"] * 100) if p["cogs"] > 0 else 0

    # Deltas
    result = {}
    for key in c:
        result[key] = c[key]
        result[f"{key}_prev"] = p[key]
        if p[key] != 0:
            result[f"{key}_delta"] = ((c[key] - p[key]) / abs(p[key])) * 100
        else:
            result[f"{key}_delta"] = 0

    return result


def calc_contribution_margins(df):
    """Calculate CM1, CM2, CM3, and ROI from daily metrics.

    Sellerboard-aligned P&L waterfall:
      CM1 = Revenue - COGS
      CM2 = CM1 - Platform Fees
      CM3 = CM2 - Shipping = Net Profit
      Margin % = CM3 / Revenue * 100  (Sellerboard "margin")
      ROI % = CM3 / COGS * 100  (Sellerboard "ROI")
    """
    if df.empty:
        return df
    df = df.copy()
    df["cm1"] = df["revenue_pln"] - df["cogs"]  # After COGS
    df["cm2"] = df["cm1"] - df["fees"]  # After platform fees
    df["cm3"] = df["profit"]  # After all costs (= profit in current schema)
    df["cm1_pct"] = np.where(
        df["revenue_pln"] > 0, df["cm1"] / df["revenue_pln"] * 100, 0
    )
    df["cm2_pct"] = np.where(
        df["revenue_pln"] > 0, df["cm2"] / df["revenue_pln"] * 100, 0
    )
    df["cm3_pct"] = np.where(
        df["revenue_pln"] > 0, df["cm3"] / df["revenue_pln"] * 100, 0
    )
    # ROI = Net Profit / COGS (Sellerboard-style return on investment)
    df["roi_pct"] = np.where(
        df["cogs"] > 0, df["cm3"] / df["cogs"] * 100, 0
    )
    return df


def daily_summary(df):
    """Aggregate daily_metrics to daily totals."""
    if df.empty:
        return df
    agg_dict = {
        "revenue_pln": "sum",
        "cogs": "sum",
        "fees": "sum",
        "profit": "sum",
        "orders_count": "sum",
        "units": "sum",
    }
    if "shipping_cost" in df.columns:
        agg_dict["shipping_cost"] = "sum"
    grouped = df.groupby("date").agg(agg_dict).reset_index()
    grouped = calc_contribution_margins(grouped)
    # Moving averages
    for col in ["revenue_pln", "cm3", "cm3_pct"]:
        if col in grouped.columns:
            grouped[f"{col}_7d"] = grouped[col].rolling(7, min_periods=1).mean()
            grouped[f"{col}_30d"] = grouped[col].rolling(30, min_periods=1).mean()
    return grouped


def product_profitability(df):
    """Aggregate daily_metrics to product-level P&L."""
    if df.empty:
        return df
    grouped = (
        df.groupby("sku")
        .agg(
            {
                "revenue_pln": "sum",
                "cogs": "sum",
                "fees": "sum",
                "profit": "sum",
                "orders_count": "sum",
                "units": "sum",
            }
        )
        .reset_index()
    )
    grouped = calc_contribution_margins(grouped)
    grouped["revenue_per_unit"] = np.where(
        grouped["units"] > 0, grouped["revenue_pln"] / grouped["units"], 0
    )
    grouped["cost_per_unit"] = np.where(
        grouped["units"] > 0, grouped["cogs"] / grouped["units"], 0
    )
    grouped["cm3_per_unit"] = np.where(
        grouped["units"] > 0, grouped["cm3"] / grouped["units"], 0
    )
    grouped["roi_pct"] = np.where(
        grouped["cogs"] > 0, grouped["cm3"] / grouped["cogs"] * 100, 0
    )
    grouped = grouped.sort_values("cm3", ascending=False)
    return grouped


def platform_summary(df, platforms_map=None):
    """Aggregate by platform."""
    if df.empty:
        return df
    grouped = (
        df.groupby("platform_id")
        .agg(
            {
                "revenue_pln": "sum",
                "cogs": "sum",
                "fees": "sum",
                "profit": "sum",
                "orders_count": "sum",
                "units": "sum",
            }
        )
        .reset_index()
    )
    grouped = calc_contribution_margins(grouped)
    if platforms_map:
        grouped["platform"] = grouped["platform_id"].map(
            lambda x: platforms_map.get(x, {}).get("code", f"#{x}")
        )
    grouped = grouped.sort_values("revenue_pln", ascending=False)
    return grouped
