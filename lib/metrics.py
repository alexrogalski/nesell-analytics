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


def calc_period_kpis(df, days, refund_summary=None, ppc_total=0.0,
                     storage_total=0.0):
    """Calculate KPIs for a period and its comparison period.

    Args:
        df: daily_metrics DataFrame
        days: period length in days
        refund_summary: optional dict from load_refund_summary() with refund data
        ppc_total: total PPC/ads spend in PLN for the period
        storage_total: total storage fees in PLN for the period
    """
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
            "refunds": subset["refunds"].sum() if "refunds" in subset.columns else 0,
            "ppc": subset["ppc_cost"].sum() if "ppc_cost" in subset.columns else 0,
            "storage": subset["storage_cost"].sum() if "storage_cost" in subset.columns else 0,
        }
        return result

    c = agg(current)
    p = agg(previous)

    # Add PPC and storage from params if not already in df
    if ppc_total > 0 and c["ppc"] == 0:
        c["ppc"] = ppc_total
    if storage_total > 0 and c["storage"] == 0:
        c["storage"] = storage_total

    # Add refund data from summary if available and not already in df
    if refund_summary and c["refunds"] == 0:
        c["refunds"] = refund_summary.get("estimated_refund_cost_pln", 0)
        c["refund_units"] = refund_summary.get("total_units_returned", 0)
        c["refund_rate"] = refund_summary.get("refund_rate_pct", 0)
    else:
        c["refund_units"] = int(current["units_returned"].sum()) if "units_returned" in current.columns else 0
        c["refund_rate"] = (c["refund_units"] / c["units"] * 100) if c["units"] > 0 else 0
    p["refund_units"] = 0
    p["refund_rate"] = 0

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


def calc_contribution_margins(df, refund_by_date=None, ad_spend_by_date=None,
                              storage_fees_total=0.0, period_days=30):
    """Calculate CM1, CM2, CM3, and Net Profit from daily metrics.

    Sellerboard-aligned P&L waterfall:
      CM1 = Revenue - COGS
      CM2 = CM1 - Platform Fees
      CM3 = CM2 - Shipping
      Net Profit = CM3 - Storage Fees - PPC/Ads - Refunds
      Margin % = Net Profit / Revenue * 100  (Sellerboard "margin")
      ROI % = Net Profit / COGS * 100  (Sellerboard "ROI")

    Args:
        df: DataFrame with daily metrics (must have revenue_pln, cogs, fees, profit columns)
        refund_by_date: optional DataFrame with (date, units_returned) for refund deduction
        ad_spend_by_date: optional dict {date_str: spend_pln} for PPC cost allocation
        storage_fees_total: total storage fees in PLN for the period (spread evenly)
        period_days: number of days in the period (for spreading monthly costs)
    """
    if df.empty:
        return df
    df = df.copy()
    df["cm1"] = df["revenue_pln"] - df["cogs"]  # After COGS
    df["cm2"] = df["cm1"] - df["fees"]  # After platform fees

    # Shipping
    shipping_col = "shipping_cost" if "shipping_cost" in df.columns else None
    if shipping_col:
        df["cm3"] = df["cm2"] - df[shipping_col]
    else:
        df["cm3"] = df["cm2"]

    # PPC / Advertising costs
    if ad_spend_by_date and isinstance(ad_spend_by_date, dict) and len(ad_spend_by_date) > 0:
        df["date_str"] = df["date"].astype(str).str[:10]
        df["ppc_cost"] = df["date_str"].map(ad_spend_by_date).fillna(0)
        if "date_str" in df.columns:
            df.drop(columns=["date_str"], inplace=True)
    else:
        df["ppc_cost"] = 0.0

    # Storage fees: spread evenly across days
    if storage_fees_total > 0 and len(df) > 0:
        n_days = df["date"].nunique() if "date" in df.columns else len(df)
        daily_storage = storage_fees_total / max(n_days, 1)
        # Allocate proportionally to daily revenue
        total_rev = df["revenue_pln"].sum()
        if total_rev > 0:
            df["storage_cost"] = df["revenue_pln"] / total_rev * storage_fees_total
        else:
            df["storage_cost"] = daily_storage / max(len(df), 1)
    else:
        df["storage_cost"] = 0.0

    # Refunds: merge refund data if available
    if refund_by_date is not None and not refund_by_date.empty:
        refund_by_date = refund_by_date.copy()
        refund_by_date["date"] = refund_by_date["date"].astype(str)
        df["date_str"] = df["date"].astype(str).str[:10]
        df = df.merge(
            refund_by_date[["date", "units_returned"]],
            left_on="date_str", right_on="date", how="left",
            suffixes=("", "_ref"),
        )
        df["units_returned"] = df["units_returned"].fillna(0)
        # Estimate refund cost per day: units_returned * avg revenue per unit
        total_units = df["units"].sum() if "units" in df.columns else 0
        total_rev = df["revenue_pln"].sum()
        avg_rev_per_unit = total_rev / total_units if total_units > 0 else 0
        df["refunds"] = df["units_returned"] * avg_rev_per_unit
        # Clean up merge artifacts
        if "date_ref" in df.columns:
            df.drop(columns=["date_ref"], inplace=True)
        if "date_str" in df.columns:
            df.drop(columns=["date_str"], inplace=True)
    else:
        df["refunds"] = 0.0
        df["units_returned"] = 0

    # Net Profit = CM3 - Storage - PPC - Refunds
    df["net_profit"] = df["cm3"] - df["storage_cost"] - df["ppc_cost"] - df["refunds"]

    df["cm1_pct"] = np.where(
        df["revenue_pln"] > 0, df["cm1"] / df["revenue_pln"] * 100, 0
    )
    df["cm2_pct"] = np.where(
        df["revenue_pln"] > 0, df["cm2"] / df["revenue_pln"] * 100, 0
    )
    df["cm3_pct"] = np.where(
        df["revenue_pln"] > 0, df["cm3"] / df["revenue_pln"] * 100, 0
    )
    df["net_profit_pct"] = np.where(
        df["revenue_pln"] > 0, df["net_profit"] / df["revenue_pln"] * 100, 0
    )
    # ROI = Net Profit / COGS (Sellerboard-style return on investment)
    df["roi_pct"] = np.where(
        df["cogs"] > 0, df["net_profit"] / df["cogs"] * 100, 0
    )
    return df


def daily_summary(df, refund_by_date=None, ad_spend_by_date=None,
                  storage_fees_total=0.0, period_days=30):
    """Aggregate daily_metrics to daily totals.

    Args:
        df: daily_metrics DataFrame
        refund_by_date: optional DataFrame with (date, units_returned) for refund integration
        ad_spend_by_date: optional dict {date_str: spend_pln} for PPC costs
        storage_fees_total: total storage fees in PLN for the period
        period_days: number of days in the period
    """
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
    grouped = calc_contribution_margins(
        grouped, refund_by_date=refund_by_date,
        ad_spend_by_date=ad_spend_by_date,
        storage_fees_total=storage_fees_total,
        period_days=period_days,
    )
    # Moving averages
    for col in ["revenue_pln", "net_profit", "net_profit_pct", "cm3", "cm3_pct"]:
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
