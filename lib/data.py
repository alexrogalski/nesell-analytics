"""Data loading layer for nesell-analytics dashboard.

Uses Supabase PostgREST API with caching via st.cache_data.
Credentials loaded from st.secrets (Streamlit Cloud) or etl.config (local).
"""
import os
import re
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
    if not SUPABASE_URL:
        return []
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    all_rows = []
    offset = 0
    batch = 1000
    while True:
        p = dict(params or {})
        p["limit"] = batch
        p["offset"] = offset
        try:
            resp = requests.get(
                url, headers={**HEADERS, "Prefer": "count=exact"}, params=p
            )
        except requests.exceptions.RequestException:
            break
        if resp.status_code not in (200, 206):
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
        {"select": "sku,name,cost_pln,cost_eur,source,is_parent,parent_sku,image_url"},
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


@st.cache_data(ttl=300)
def load_order_items(order_ids=None):
    """Load order items, optionally filtered by order IDs.

    Deduplicates by (order_id, sku) keeping the row with the lowest id,
    because the ETL may insert duplicate rows across multiple runs.
    """
    params = {
        "select": "id,order_id,sku,product_id,name,quantity,unit_price,currency,unit_price_pln,unit_cost,unit_cost_pln,asin",
    }
    if order_ids:
        ids_str = ",".join(str(i) for i in order_ids)
        params["order_id"] = f"in.({ids_str})"
    rows = _get("order_items", params, limit=50000)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Deduplicate: keep the first (lowest id) row per (order_id, sku)
    df = df.sort_values("id").drop_duplicates(subset=["order_id", "sku"], keep="first").reset_index(drop=True)
    return df


@st.cache_data(ttl=300)
def load_orders_enriched(days=30):
    """Load orders enriched with items, products, platforms, and fx_rates.

    Returns a tuple (orders_df, items_df) where orders_df has per-order
    profit breakdown and items_df has all line items keyed by order_id.
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Load orders
    order_rows = _get("orders", {
        "select": "id,external_id,platform_id,platform_order_id,order_date,status,buyer_email,shipping_country,shipping_cost,total_paid,currency,total_paid_pln,platform_fee,platform_fee_pln,seller_shipping_cost_pln,notes",
        "order_date": f"gte.{cutoff}",
        "order": "order_date.desc",
    }, limit=50000)
    orders = pd.DataFrame(order_rows)
    if orders.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Load related data
    order_ids = orders["id"].tolist()
    items_df = load_order_items(order_ids)
    products = load_products()
    platforms = load_platforms()
    fx_rows = _get("fx_rates", {"order": "date.desc"}, limit=5000)
    fx_df = pd.DataFrame(fx_rows)

    # Build fx lookup: (date_str, currency) -> rate_pln
    fx_lookup = {}
    latest_fx = {}
    if not fx_df.empty:
        for _, r in fx_df.iterrows():
            key = (str(r["date"]), str(r["currency"]).upper())
            fx_lookup[key] = float(r["rate_pln"])
            cur = str(r["currency"]).upper()
            if cur not in latest_fx:
                latest_fx[cur] = float(r["rate_pln"])
    latest_fx["PLN"] = 1.0
    fx_lookup_default = latest_fx

    def get_fx(date_str, currency):
        cur = str(currency).upper()
        if cur == "PLN":
            return 1.0
        day = str(date_str)[:10]
        rate = fx_lookup.get((day, cur))
        if rate:
            return rate
        return fx_lookup_default.get(cur, 1.0)

    # Build products cost lookup: sku -> (cost_pln, cost_eur, image_url, name)
    prod_lookup = {}
    if not products.empty:
        for _, p in products.iterrows():
            prod_lookup[str(p.get("sku", ""))] = {
                "cost_pln": float(p["cost_pln"]) if pd.notna(p.get("cost_pln")) else 0,
                "cost_eur": float(p["cost_eur"]) if pd.notna(p.get("cost_eur")) else 0,
                "image_url": p.get("image_url"),
                "product_name": p.get("name", ""),
            }

    def _find_product_cost(sku):
        """Look up product cost with SKU fallback strategies.

        Tries: exact match, then strip size suffix (-S, -M, -L, -XL),
        then strip trailing dashes, then base SKU (before first dash
        for non-PFT/non-EAN SKUs).
        """
        sku = str(sku).strip()
        if not sku:
            return 0, 0

        # 1) Exact match
        prod = prod_lookup.get(sku)
        if prod and (prod["cost_pln"] > 0 or prod["cost_eur"] > 0):
            return prod["cost_pln"], prod["cost_eur"]

        # 2) Strip trailing size suffix: -S, -M, -L, -XL, -XXL, -XXXL
        stripped = re.sub(r'-(XXX?L|XL|L|M|S)$', '', sku)
        if stripped != sku:
            prod = prod_lookup.get(stripped)
            if prod and (prod["cost_pln"] > 0 or prod["cost_eur"] > 0):
                return prod["cost_pln"], prod["cost_eur"]

        # 3) Strip trailing dashes (e.g., "888408282750--" -> "888408282750-" -> "888408282750")
        s = sku.rstrip('-')
        while s != sku:
            prod = prod_lookup.get(s)
            if prod and (prod["cost_pln"] > 0 or prod["cost_eur"] > 0):
                return prod["cost_pln"], prod["cost_eur"]
            sku = s
            s = sku.rstrip('-')

        # 4) Add trailing dash variants (some products stored with extra dashes)
        for suffix in ['-', '--', '---']:
            prod = prod_lookup.get(sku + suffix)
            if prod and (prod["cost_pln"] > 0 or prod["cost_eur"] > 0):
                return prod["cost_pln"], prod["cost_eur"]

        return 0, 0

    # Build order_id -> order_date lookup for FX conversion
    order_date_lookup = {}
    for _, o in orders.iterrows():
        order_date_lookup[o["id"]] = str(o["order_date"])[:10]

    # Build order_id -> currency lookup
    order_currency_lookup = {}
    for _, o in orders.iterrows():
        order_currency_lookup[o["id"]] = str(o.get("currency", "EUR"))

    # Platform fee rates (non-Amazon platforms only; Amazon uses FBA/FBM detection)
    platform_fee_rates = {
        "allegro": 0.10, "temu": 0.0, "empik": 0.15,
    }

    # Amazon fee rates: FBA includes referral + fulfillment (~34%), FBM is referral only (~15.45%)
    # These match the rates in etl/aggregator.py computed from real Finances API data.
    AMAZON_FBA_FEE_RATE = 0.3473  # referral (~15%) + FBA fulfillment (~18-19%) + other (~1%)
    AMAZON_FBM_FEE_RATE = 0.1545  # referral fee only

    # Detect FBA vs FBM: numeric external_id = FBM (from Baselinker), otherwise FBA (from SP-API)
    amazon_platform_names = {
        "amazon_de", "amazon_fr", "amazon_it", "amazon_es",
        "amazon_nl", "amazon_se", "amazon_pl", "amazon_be", "amazon_gb",
    }

    # Enrich items with COGS and unit_price_pln
    if not items_df.empty:
        items_df["unit_cost_pln"] = pd.to_numeric(items_df["unit_cost_pln"], errors="coerce").fillna(0)
        items_df["unit_cost"] = pd.to_numeric(items_df["unit_cost"], errors="coerce").fillna(0)
        items_df["unit_price"] = pd.to_numeric(items_df["unit_price"], errors="coerce").fillna(0)
        items_df["unit_price_pln"] = pd.to_numeric(items_df["unit_price_pln"], errors="coerce").fillna(0)
        items_df["quantity"] = pd.to_numeric(items_df["quantity"], errors="coerce").fillna(1).astype(int)

        for idx, item in items_df.iterrows():
            oid = item["order_id"]
            date_str = order_date_lookup.get(oid, "2026-03-01")
            item_currency = str(item.get("currency", "")) or order_currency_lookup.get(oid, "EUR")

            # Fill missing unit_price_pln from unit_price * fx_rate
            if item["unit_price_pln"] == 0 and item["unit_price"] > 0:
                items_df.at[idx, "unit_price_pln"] = item["unit_price"] * get_fx(date_str, item_currency)

            # Fill missing unit_cost_pln from products table (with SKU fallback)
            if item["unit_cost_pln"] == 0:
                sku = str(item.get("sku", ""))
                cost_pln, cost_eur = _find_product_cost(sku)
                if cost_pln > 0:
                    items_df.at[idx, "unit_cost_pln"] = cost_pln
                elif cost_eur > 0:
                    items_df.at[idx, "unit_cost_pln"] = cost_eur * get_fx(date_str, "EUR")

        items_df["line_cost_pln"] = items_df["unit_cost_pln"] * items_df["quantity"]
        items_df["line_revenue_pln"] = items_df["unit_price_pln"] * items_df["quantity"]
    else:
        items_df = pd.DataFrame(columns=["order_id", "sku", "name", "quantity",
                                          "unit_price", "unit_price_pln", "unit_cost_pln",
                                          "line_cost_pln", "line_revenue_pln", "asin",
                                          "currency", "product_id"])

    # Aggregate items per order
    if not items_df.empty:
        items_agg = items_df.groupby("order_id").agg(
            item_count=("sku", "count"),
            unit_count=("quantity", "sum"),
            cogs_pln=("line_cost_pln", "sum"),
            first_sku=("sku", "first"),
            first_name=("name", "first"),
        ).reset_index()
    else:
        items_agg = pd.DataFrame(columns=["order_id", "item_count", "unit_count",
                                           "cogs_pln", "first_sku", "first_name"])

    # Merge items aggregation into orders
    orders = orders.merge(items_agg, left_on="id", right_on="order_id", how="left", suffixes=("", "_items"))
    for col in ["item_count", "unit_count"]:
        orders[col] = pd.to_numeric(orders[col], errors="coerce").fillna(0).astype(int)
    orders["cogs_pln"] = pd.to_numeric(orders["cogs_pln"], errors="coerce").fillna(0)

    # Convert numeric columns
    for col in ["total_paid", "total_paid_pln", "shipping_cost", "platform_fee", "platform_fee_pln", "seller_shipping_cost_pln"]:
        orders[col] = pd.to_numeric(orders[col], errors="coerce").fillna(0)

    # Platform name
    orders["platform_name"] = orders["platform_id"].map(
        lambda x: platforms.get(x, {}).get("code", f"#{x}")
    )
    orders["platform_display"] = orders["platform_id"].map(
        lambda x: platforms.get(x, {}).get("name", f"#{x}")
    )

    # Revenue in PLN (use total_paid_pln if available, else convert)
    def calc_revenue_pln(row):
        if row["total_paid_pln"] and row["total_paid_pln"] > 0:
            return row["total_paid_pln"]
        date_str = str(row["order_date"])[:10]
        return row["total_paid"] * get_fx(date_str, row["currency"])
    orders["revenue_pln"] = orders.apply(calc_revenue_pln, axis=1)

    # Fees in PLN
    def calc_fees_pln(row):
        if row["platform_fee_pln"] and row["platform_fee_pln"] > 0:
            return row["platform_fee_pln"]
        if row["platform_fee"] and row["platform_fee"] > 0:
            date_str = str(row["order_date"])[:10]
            return row["platform_fee"] * get_fx(date_str, row["currency"])
        # Estimate from platform fee rate (FBA/FBM-aware for Amazon)
        pname = row.get("platform_name", "")
        if pname in amazon_platform_names:
            ext_id = str(row.get("external_id", ""))
            is_fbm = ext_id.isdigit()
            rate = AMAZON_FBM_FEE_RATE if is_fbm else AMAZON_FBA_FEE_RATE
        else:
            rate = platform_fee_rates.get(pname, 0)
        return row["revenue_pln"] * rate
    orders["fees_pln"] = orders.apply(calc_fees_pln, axis=1)

    # Shipping in PLN
    # Prefer seller_shipping_cost_pln (actual DPD courier cost, already in PLN)
    # over shipping_cost (buyer's delivery_price, which is a revenue component)
    def calc_shipping_pln(row):
        seller_cost = row.get("seller_shipping_cost_pln", 0)
        if seller_cost and seller_cost > 0:
            return seller_cost
        if row["shipping_cost"] > 0:
            date_str = str(row["order_date"])[:10]
            return row["shipping_cost"] * get_fx(date_str, row["currency"])
        return 0
    orders["shipping_pln"] = orders.apply(calc_shipping_pln, axis=1)

    # Profit
    orders["profit_pln"] = orders["revenue_pln"] - orders["cogs_pln"] - orders["fees_pln"] - orders["shipping_pln"]
    orders["margin_pct"] = orders.apply(
        lambda r: (r["profit_pln"] / r["revenue_pln"] * 100) if r["revenue_pln"] > 0 else 0, axis=1
    )

    # Has COGS flag
    orders["has_cogs"] = orders["cogs_pln"] > 0

    # FBA/FBM flag (for display purposes)
    orders["fulfillment"] = orders.apply(
        lambda r: "FBA" if (r["platform_name"] in amazon_platform_names
                           and not str(r.get("external_id", "")).isdigit())
        else ("FBM" if r["platform_name"] in amazon_platform_names else ""),
        axis=1,
    )

    # ROI = profit / COGS * 100 (Sellerboard-style return on investment)
    orders["roi_pct"] = orders.apply(
        lambda r: (r["profit_pln"] / r["cogs_pln"] * 100) if r["cogs_pln"] > 0 else 0, axis=1
    )

    # Fill missing item info
    orders["first_sku"] = orders["first_sku"].fillna("")
    orders["first_name"] = orders["first_name"].fillna("")

    # Sort
    orders = orders.sort_values("order_date", ascending=False).reset_index(drop=True)

    return orders, items_df


def get_marketplace_names():
    """Get marketplace ID to display name mapping."""
    try:
        from etl.config import MARKETPLACE_TO_PLATFORM
        mkt_names = {v: v.replace("amazon_", "").upper() for v in MARKETPLACE_TO_PLATFORM.values()}
        return {k: mkt_names.get(v, v) for k, v in MARKETPLACE_TO_PLATFORM.items()}
    except ImportError:
        return {}
