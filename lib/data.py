"""Data loading layer for nesell-analytics dashboard.

Uses Supabase PostgREST API with caching via st.cache_data.
Credentials loaded from st.secrets (Streamlit Cloud) or etl.config (local).
"""
import os
import requests
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# --- Supabase credentials ---
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except Exception:
    try:
        from etl import config
        SUPABASE_URL = config.SUPABASE_URL
        SUPABASE_KEY = config.SUPABASE_KEY
    except Exception:
        SUPABASE_URL = os.getenv("SUPABASE_URL", "")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}


def _get(table, params=None, limit=10000):
    """Generic PostgREST GET with pagination."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    all_rows = []
    offset = 0
    batch = 1000
    while True:
        p = dict(params or {})
        p["limit"] = batch
        p["offset"] = offset
        resp = requests.get(
            url, headers={**HEADERS, "Prefer": "count=exact"}, params=p
        )
        if resp.status_code != 200:
            break
        rows = resp.json()
        all_rows.extend(rows)
        if len(rows) < batch or len(all_rows) >= limit:
            break
        offset += batch
    return all_rows


@st.cache_data(ttl=300)
def load_daily_metrics(days=90):
    """Load aggregated daily metrics."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _get(
        "daily_metrics",
        {
            "select": "date,platform_id,sku,orders_count,units_sold,revenue,revenue_pln,cogs,platform_fees,shipping_cost,gross_profit,margin_pct",
            "date": f"gte.{cutoff}",
            "order": "date.asc",
        },
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Normalize column names for downstream use
    rename_map = {
        "units_sold": "units",
        "platform_fees": "fees",
        "gross_profit": "profit",
        "margin_pct": "margin",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
    for col in ["revenue_pln", "cogs", "fees", "profit", "margin", "revenue", "shipping_cost"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["units", "orders_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300)
def load_orders(days=90):
    """Load orders."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _get(
        "orders",
        {
            "select": "id,external_id,platform_id,platform_order_id,order_date,status,shipping_country,total_paid,currency,shipping_cost",
            "order_date": f"gte.{cutoff}",
            "order": "order_date.desc",
        },
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_products():
    """Load all products."""
    rows = _get(
        "products",
        {"select": "sku,name,cost_pln,cost_eur,source,is_parent,parent_sku"},
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_platforms():
    """Load platform definitions. Returns dict {id: {id, code, name, ...}}."""
    rows = _get("platforms", {"select": "id,code,name"})
    return {r["id"]: r for r in rows}


@st.cache_data(ttl=300)
def load_amazon_traffic(days=30):
    """Load Amazon traffic data."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _get(
        "amazon_traffic",
        {
            "select": "date,asin,marketplace_id,sessions,page_views,buy_box_pct,units_ordered,ordered_product_sales,currency",
            "date": f"gte.{cutoff}",
            "order": "date.asc",
        },
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_amazon_inventory():
    """Load latest Amazon FBA inventory."""
    rows = _get(
        "amazon_inventory",
        {
            "select": "snapshot_date,sku,fnsku,asin,product_name,country,fulfillable_qty",
            "order": "snapshot_date.desc",
            "limit": "500",
        },
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_amazon_returns(days=90):
    """Load Amazon returns."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _get(
        "amazon_returns",
        {
            "select": "return_date,order_id,sku,asin,product_name,quantity,reason,detailed_disposition,status",
            "return_date": f"gte.{cutoff}",
            "order": "return_date.desc",
        },
        limit=1000,
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_amazon_bsr():
    """Load Amazon Best Seller Rank data."""
    rows = _get(
        "amazon_bsr",
        {
            "select": "snapshot_date,asin,marketplace_id,category_name,rank",
            "order": "snapshot_date.desc",
        },
        limit=5000,
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_amazon_pricing():
    """Load Amazon competitive pricing data."""
    rows = _get(
        "amazon_pricing",
        {
            "select": "snapshot_date,asin,marketplace_id,landed_price,listing_price,shipping_price,currency,condition_value",
            "order": "snapshot_date.desc",
        },
        limit=5000,
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_fx_rates(days=90):
    """Load FX rates."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _get(
        "fx_rates",
        {"date": f"gte.{cutoff}", "order": "date.asc"},
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_cogs_gaps(days=90):
    """Load SKUs with revenue but no COGS, aggregated by SKU."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _get("daily_metrics", {
        "date": f"gte.{cutoff}",
        "cogs": "eq.0",
        "revenue_pln": "gt.0",
        "select": "sku,revenue_pln,units_sold,orders_count",
    })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for col in ["revenue_pln"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["units_sold", "orders_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    # Aggregate by SKU
    agg_dict = {"revenue_pln": "sum", "orders_count": "sum"}
    if "units_sold" in df.columns:
        agg_dict["units_sold"] = "sum"
    grouped = df.groupby("sku").agg(agg_dict).reset_index()
    grouped.rename(columns={"units_sold": "units"}, inplace=True)
    grouped = grouped.sort_values("revenue_pln", ascending=False)
    return grouped


@st.cache_data(ttl=300)
def load_data_coverage(days=90):
    """Calculate data coverage stats: COGS %, fee %, data freshness."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _get("daily_metrics", {
        "date": f"gte.{cutoff}",
        "select": "date,revenue_pln,cogs,platform_fees",
    })
    df = pd.DataFrame(rows)
    if df.empty:
        return {"cogs_coverage": 0, "fee_coverage": 0, "last_date": "N/A", "total_revenue": 0}
    for col in ["revenue_pln", "cogs", "platform_fees"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    total_rev = df["revenue_pln"].sum()
    rev_with_cogs = df[df["cogs"] > 0]["revenue_pln"].sum()
    rev_with_fees = df[df["platform_fees"] > 0]["revenue_pln"].sum()
    return {
        "cogs_coverage": (rev_with_cogs / total_rev * 100) if total_rev > 0 else 0,
        "fee_coverage": (rev_with_fees / total_rev * 100) if total_rev > 0 else 0,
        "last_date": df["date"].max(),
        "total_revenue": total_rev,
        "revenue_without_cogs": total_rev - rev_with_cogs,
    }


def get_marketplace_names():
    """Get marketplace ID to display name mapping."""
    try:
        from etl.config import MARKETPLACE_TO_PLATFORM
        mkt_names = {v: v.replace("amazon_", "").upper() for v in MARKETPLACE_TO_PLATFORM.values()}
        return {k: mkt_names.get(v, v) for k, v in MARKETPLACE_TO_PLATFORM.items()}
    except ImportError:
        return {}
