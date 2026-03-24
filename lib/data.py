"""Data loading layer for nesell-analytics dashboard.

Uses Supabase PostgREST API with caching via st.cache_data.
Credentials loaded from st.secrets (Streamlit Cloud) or etl.config (local).
"""
import os
import re
import time
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


def _get(table, params=None, limit=10000, retries=2):
    """Generic PostgREST GET with pagination and retry on transient errors."""
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
        resp = None
        for attempt in range(retries + 1):
            try:
                resp = requests.get(
                    url, headers={**HEADERS, "Prefer": "count=exact"}, params=p,
                    timeout=15,
                )
                if resp.status_code in (200, 206):
                    break
                if resp.status_code >= 500 and attempt < retries:
                    time.sleep(1)
                    continue
            except requests.exceptions.RequestException:
                if attempt < retries:
                    time.sleep(1)
                    continue
                return all_rows
        if resp is None or resp.status_code not in (200, 206):
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
    """Load all products.

    Uses a longer retry budget because an empty result here causes all
    product images and names to show as N/A for the cache TTL.
    """
    rows = _get(
        "products",
        {"select": "sku,name,cost_pln,cost_eur,source,is_parent,parent_sku,image_url"},
        retries=3,
    )
    df = pd.DataFrame(rows)
    # Guard: if the API returned data but image_url column is missing
    # (e.g. schema changed), add it to prevent downstream KeyErrors.
    if not df.empty and "image_url" not in df.columns:
        df["image_url"] = None
    return df


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
def load_refund_summary(days=90):
    """Load refund/return summary aggregated by date for P&L integration.

    Returns a dict with:
      - total_returns: number of return records
      - total_units_returned: total units returned
      - refund_by_date: DataFrame with (date, units_returned) aggregated daily
      - refund_by_sku: DataFrame with (sku, product_name, units_returned) aggregated by SKU
      - estimated_refund_cost_pln: estimated revenue lost (units_returned * avg revenue per unit)
    """
    rets = load_amazon_returns(days=days)
    dm = load_daily_metrics(days=days)

    result = {
        "total_returns": 0,
        "total_units_returned": 0,
        "refund_by_date": pd.DataFrame(columns=["date", "units_returned"]),
        "refund_by_sku": pd.DataFrame(columns=["sku", "product_name", "units_returned"]),
        "estimated_refund_cost_pln": 0.0,
        "refund_rate_pct": 0.0,
    }

    if rets.empty:
        return result

    rets["quantity"] = pd.to_numeric(rets["quantity"], errors="coerce").fillna(0).astype(int)
    result["total_returns"] = len(rets)
    result["total_units_returned"] = int(rets["quantity"].sum())

    # Aggregate by date
    rets_dated = rets[rets["return_date"].notna()].copy()
    if not rets_dated.empty:
        rets_dated["date"] = pd.to_datetime(rets_dated["return_date"]).dt.date.astype(str)
        by_date = rets_dated.groupby("date").agg(units_returned=("quantity", "sum")).reset_index()
        result["refund_by_date"] = by_date

    # Aggregate by SKU
    by_sku = rets.groupby(["sku", "product_name"]).agg(units_returned=("quantity", "sum")).reset_index()
    by_sku = by_sku.sort_values("units_returned", ascending=False)
    result["refund_by_sku"] = by_sku

    # Estimate refund cost: use average revenue per unit from daily_metrics
    if not dm.empty:
        for col in ["revenue_pln", "units"]:
            if col not in dm.columns:
                if col == "units" and "units_sold" in dm.columns:
                    dm["units"] = dm["units_sold"]
                else:
                    dm[col] = 0
        dm["revenue_pln"] = pd.to_numeric(dm["revenue_pln"], errors="coerce").fillna(0)
        dm["units"] = pd.to_numeric(dm["units"], errors="coerce").fillna(0)
        total_revenue = dm["revenue_pln"].sum()
        total_units = dm["units"].sum()
        if total_units > 0:
            avg_rev_per_unit = total_revenue / total_units
            result["estimated_refund_cost_pln"] = round(avg_rev_per_unit * result["total_units_returned"], 2)
            # Refund rate: returned units / total units sold
            result["refund_rate_pct"] = round(result["total_units_returned"] / total_units * 100, 2)

    return result


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
def load_amazon_restock():
    """Load latest Amazon Restock Recommendations snapshot."""
    rows = _get(
        "amazon_restock",
        {
            "select": "snapshot_date,sku,asin,product_name,recommended_qty,days_of_cover,reorder_date,marketplace_id",
            "order": "snapshot_date.desc",
        },
        limit=5000,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Keep only the latest snapshot
    latest = df["snapshot_date"].max()
    return df[df["snapshot_date"] == latest].copy()


@st.cache_data(ttl=300)
def load_amazon_aged_inventory():
    """Load latest Amazon FBA Aged Inventory snapshot."""
    rows = _get(
        "amazon_aged_inventory",
        {
            "select": "snapshot_date,sku,asin,product_name,qty_to_be_charged_ltsf,days_of_supply,inv_age_0_to_90_days,inv_age_91_to_180_days,inv_age_181_to_270_days,inv_age_271_plus_days,marketplace_id",
            "order": "snapshot_date.desc",
        },
        limit=5000,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Keep only the latest snapshot
    latest = df["snapshot_date"].max()
    return df[df["snapshot_date"] == latest].copy()


@st.cache_data(ttl=300)
def load_amazon_pricing():
    """Load Amazon competitive pricing data (including our_price from competitor_prices ETL)."""
    rows = _get(
        "amazon_pricing",
        {
            "select": "snapshot_date,asin,marketplace_id,buy_box_price,buy_box_landed_price,lowest_fba_price,lowest_fbm_price,num_offers_new,num_offers_used,our_price,currency,landed_price,listing_price,shipping_price,condition_value",
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
    # Load FX rates with server-side date filter (orders period + 7 day buffer)
    _fx_cutoff = (datetime.now() - timedelta(days=days + 7)).strftime("%Y-%m-%d")
    fx_rows = _get("fx_rates", {"date": f"gte.{_fx_cutoff}", "order": "date.desc"}, limit=5000)
    fx_df = pd.DataFrame(fx_rows)

    # Build fx lookup: (date_str, currency) -> rate_pln  [vectorized]
    fx_lookup = {}
    latest_fx = {}
    if not fx_df.empty:
        _dates = fx_df["date"].astype(str)
        _curs = fx_df["currency"].astype(str).str.upper()
        _rates = pd.to_numeric(fx_df["rate_pln"], errors="coerce").values
        fx_lookup = dict(zip(zip(_dates, _curs), _rates))
        # Latest rate per currency (fx_df ordered date.desc, first occurrence = latest)
        _dedup = fx_df.drop_duplicates(subset=["currency"], keep="first")
        latest_fx = dict(zip(
            _dedup["currency"].astype(str).str.upper(),
            pd.to_numeric(_dedup["rate_pln"], errors="coerce"),
        ))
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

    # Build products cost lookup: sku -> {cost_pln, cost_eur, ...}  [vectorized]
    prod_lookup = {}
    if not products.empty:
        _p = products.copy()
        _p["sku"] = _p["sku"].astype(str)
        _p["cost_pln"] = pd.to_numeric(_p["cost_pln"], errors="coerce").fillna(0)
        _p["cost_eur"] = pd.to_numeric(_p["cost_eur"], errors="coerce").fillna(0)
        prod_lookup = {
            row["sku"]: {
                "cost_pln": row["cost_pln"],
                "cost_eur": row["cost_eur"],
                "image_url": row.get("image_url"),
                "product_name": row.get("name", ""),
            }
            for row in _p.to_dict("records")
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

    # Build order_id -> order_date lookup for FX conversion  [vectorized]
    order_date_lookup = dict(zip(
        orders["id"],
        orders["order_date"].astype(str).str[:10],
    ))

    # Build order_id -> currency lookup  [vectorized]
    order_currency_lookup = dict(zip(
        orders["id"],
        orders["currency"].fillna("EUR").astype(str),
    ))

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

    # Enrich items with COGS and unit_price_pln  [vectorized]
    if not items_df.empty:
        items_df["unit_cost_pln"] = pd.to_numeric(items_df["unit_cost_pln"], errors="coerce").fillna(0)
        items_df["unit_cost"] = pd.to_numeric(items_df["unit_cost"], errors="coerce").fillna(0)
        items_df["unit_price"] = pd.to_numeric(items_df["unit_price"], errors="coerce").fillna(0)
        items_df["unit_price_pln"] = pd.to_numeric(items_df["unit_price_pln"], errors="coerce").fillna(0)
        items_df["quantity"] = pd.to_numeric(items_df["quantity"], errors="coerce").fillna(1).astype(int)

        # Pre-compute per-item date and currency vectors
        _item_dates = items_df["order_id"].map(order_date_lookup).fillna("2026-03-01")
        _item_cur_raw = items_df["currency"].fillna("").astype(str)
        _item_cur_order = items_df["order_id"].map(order_currency_lookup).fillna("EUR")
        _item_currencies = _item_cur_raw.where(_item_cur_raw != "", _item_cur_order)

        # Pre-compute cost map for all unique SKUs [once, not per-row]
        _unique_skus = items_df["sku"].dropna().astype(str).unique()
        _cost_cache = {s: _find_product_cost(s) for s in _unique_skus}

        # Pre-compute FX rates for all unique (date, currency) pairs [once]
        _fx_pairs = set(zip(_item_dates, _item_currencies))
        # Also pre-compute EUR rates for cost conversion
        for d in _item_dates.unique():
            _fx_pairs.add((d, "EUR"))
        _fx_cache = {(d, c): get_fx(d, c) for d, c in _fx_pairs}

        # Fill missing unit_price_pln: vectorized mask + vectorized FX lookup
        _needs_price = (items_df["unit_price_pln"] == 0) & (items_df["unit_price"] > 0)
        if _needs_price.any():
            _fx_rates_price = pd.Series(
                [_fx_cache.get((d, c), 1.0) for d, c in zip(_item_dates[_needs_price], _item_currencies[_needs_price])],
                index=items_df.index[_needs_price],
            )
            items_df.loc[_needs_price, "unit_price_pln"] = items_df.loc[_needs_price, "unit_price"] * _fx_rates_price

        # Fill missing unit_cost_pln from product costs: vectorized lookup
        _needs_cost = items_df["unit_cost_pln"] == 0
        if _needs_cost.any():
            _skus = items_df.loc[_needs_cost, "sku"].astype(str)
            _cost_pln = _skus.map(lambda s: _cost_cache.get(s, (0, 0))[0])
            _cost_eur = _skus.map(lambda s: _cost_cache.get(s, (0, 0))[1])

            # Apply PLN costs directly where available
            _has_pln = _cost_pln > 0
            if _has_pln.any():
                items_df.loc[_has_pln[_has_pln].index, "unit_cost_pln"] = _cost_pln[_has_pln].values

            # Convert EUR costs via FX for remaining items
            _needs_eur_mask = (~_has_pln) & (_cost_eur > 0)
            if _needs_eur_mask.any():
                _eur_dates = _item_dates[_needs_eur_mask.index[_needs_eur_mask]]
                _eur_fx = pd.Series(
                    [_fx_cache.get((d, "EUR"), 1.0) for d in _eur_dates],
                    index=_needs_eur_mask.index[_needs_eur_mask],
                )
                items_df.loc[_needs_eur_mask[_needs_eur_mask].index, "unit_cost_pln"] = (
                    _cost_eur[_needs_eur_mask].values * _eur_fx.values
                )

        items_df["line_cost_pln"] = items_df["unit_cost_pln"] * items_df["quantity"]
        items_df["line_revenue_pln"] = items_df["unit_price_pln"] * items_df["quantity"]

        # Add product image_url to items
        items_df["image_url"] = items_df["sku"].astype(str).map(
            lambda s: (prod_lookup.get(s) or {}).get("image_url", "")
        ).fillna("")
    else:
        items_df = pd.DataFrame(columns=["order_id", "sku", "name", "quantity",
                                          "unit_price", "unit_price_pln", "unit_cost_pln",
                                          "line_cost_pln", "line_revenue_pln", "asin",
                                          "currency", "product_id", "image_url"])

    # Aggregate items per order
    if not items_df.empty:
        items_agg = items_df.groupby("order_id").agg(
            item_count=("sku", "count"),
            unit_count=("quantity", "sum"),
            cogs_pln=("line_cost_pln", "sum"),
            items_revenue_pln=("line_revenue_pln", "sum"),
            first_sku=("sku", "first"),
            first_name=("name", "first"),
            first_image=("image_url", "first"),
        ).reset_index()
    else:
        items_agg = pd.DataFrame(columns=["order_id", "item_count", "unit_count",
                                           "cogs_pln", "items_revenue_pln",
                                           "first_sku", "first_name", "first_image"])

    # Merge items aggregation into orders
    orders = orders.merge(items_agg, left_on="id", right_on="order_id", how="left", suffixes=("", "_items"))
    for col in ["item_count", "unit_count"]:
        orders[col] = pd.to_numeric(orders[col], errors="coerce").fillna(0).astype(int)
    orders["cogs_pln"] = pd.to_numeric(orders["cogs_pln"], errors="coerce").fillna(0)
    orders["items_revenue_pln"] = pd.to_numeric(orders.get("items_revenue_pln", 0), errors="coerce").fillna(0)

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

    # ------------------------------------------------------------------
    # Filter out non-sale orders (FBA inbound shipments, cancelled)
    # ------------------------------------------------------------------
    _ext_ids = orders["external_id"].fillna("").astype(str)
    _statuses = orders["status"].fillna("").astype(str).str.lower()

    # FBA inbound shipments have external_id starting with S02- or FBA-
    _is_fba_inbound = _ext_ids.str.match(r'^(S02-|FBA-)', case=False, na=False)
    # Mark cancelled orders
    _is_cancelled = _statuses.isin(["cancelled", "canceled", "refunded"])

    # Flag non-sale orders (keep in dataset but zero out COGS to not pollute P&L)
    orders["is_sale"] = ~(_is_fba_inbound | _is_cancelled)
    # Zero out COGS for non-sale orders (inbound shipments, cancellations)
    orders.loc[~orders["is_sale"], "cogs_pln"] = 0

    # Pre-compute FX rates for all unique order (date, currency) pairs [vectorized]
    _order_dates = orders["order_date"].astype(str).str[:10]
    _order_curs = orders["currency"].fillna("EUR").astype(str)
    _order_fx_pairs = set(zip(_order_dates, _order_curs))
    _order_fx_cache = {(d, c): get_fx(d, c) for d, c in _order_fx_pairs}
    _order_fx_rates = pd.Series(
        [_order_fx_cache.get((d, c), 1.0) for d, c in zip(_order_dates, _order_curs)],
        index=orders.index,
    )

    # Revenue in PLN (brutto / gross)  [vectorized]
    _has_paid_pln = orders["total_paid_pln"] > 0
    orders["revenue_pln"] = orders["total_paid"] * _order_fx_rates
    orders.loc[_has_paid_pln, "revenue_pln"] = orders.loc[_has_paid_pln, "total_paid_pln"]

    # Fallback: use item prices sum when total_paid is 0 but items have prices
    _zero_rev = orders["revenue_pln"] <= 0
    _has_item_rev = orders["items_revenue_pln"] > 0
    orders.loc[_zero_rev & _has_item_rev, "revenue_pln"] = orders.loc[_zero_rev & _has_item_rev, "items_revenue_pln"]

    # Non-sale orders get zero revenue
    orders.loc[~orders["is_sale"], "revenue_pln"] = 0

    # FBA/FBM detection (needed early for shipping and fulfillment cost)  [vectorized]
    _is_amazon = orders["platform_name"].isin(amazon_platform_names)
    _is_fbm = orders["external_id"].fillna("").astype(str).apply(str.isdigit)
    orders["fulfillment"] = ""
    orders.loc[_is_amazon & ~_is_fbm, "fulfillment"] = "FBA"
    orders.loc[_is_amazon & _is_fbm, "fulfillment"] = "FBM"

    # ------------------------------------------------------------------
    # 1. VAT deduction: compute net revenue from gross  [vectorized]
    # ------------------------------------------------------------------
    EU_VAT_RATES = {
        "DE": 0.19, "FR": 0.20, "IT": 0.22, "ES": 0.21,
        "NL": 0.21, "SE": 0.25, "PL": 0.23, "BE": 0.21,
        "AT": 0.20, "GB": 0.20, "EE": 0.22, "FI": 0.255,
        "IE": 0.23, "PT": 0.23, "LU": 0.17, "CZ": 0.21,
    }
    _countries = orders["shipping_country"].fillna("").astype(str).str.upper().str.strip()
    orders["vat_rate"] = _countries.map(EU_VAT_RATES).fillna(0.23)
    orders["vat_amount_pln"] = orders["revenue_pln"] * orders["vat_rate"] / (1 + orders["vat_rate"])
    orders["revenue_net_pln"] = orders["revenue_pln"] - orders["vat_amount_pln"]

    # ------------------------------------------------------------------
    # 2. Fees in PLN  [vectorized]
    # ------------------------------------------------------------------
    # Layer 1: estimate from platform fee rate (default)
    _fee_rate = orders["platform_name"].map(platform_fee_rates).fillna(0)
    _fee_rate = _fee_rate.where(~_is_amazon, _is_fbm.map({True: AMAZON_FBM_FEE_RATE, False: AMAZON_FBA_FEE_RATE}))
    orders["fees_pln"] = orders["revenue_pln"] * _fee_rate
    # Layer 2: override with converted platform_fee where available
    _has_fee = orders["platform_fee"] > 0
    orders.loc[_has_fee, "fees_pln"] = orders.loc[_has_fee, "platform_fee"] * _order_fx_rates[_has_fee]
    # Layer 3: override with platform_fee_pln where available (highest priority)
    _has_fee_pln = orders["platform_fee_pln"] > 0
    orders.loc[_has_fee_pln, "fees_pln"] = orders.loc[_has_fee_pln, "platform_fee_pln"]

    # ------------------------------------------------------------------
    # 3. Shipping cost: DPD contract estimates for FBM  [vectorized]
    # ------------------------------------------------------------------
    DPD_RATES_EUR = {
        "DE": 2.86, "FR": 3.65, "IT": 3.72, "ES": 3.81,
        "NL": 2.92, "SE": 5.03, "AT": 3.18, "BE": 3.56,
        "PL": 0.0,  # domestic handled separately
    }
    DPD_SECURITY_FEE = 0.45   # EUR per package
    DPD_FUEL_SURCHARGE = 0.17  # 17%
    DPD_PL_DOMESTIC_PLN = 12.0

    # Start with seller_shipping_cost_pln where available (highest priority)
    orders["shipping_pln"] = 0.0
    _has_seller = orders["seller_shipping_cost_pln"] > 0
    orders.loc[_has_seller, "shipping_pln"] = orders.loc[_has_seller, "seller_shipping_cost_pln"]

    # For FBM orders WITHOUT seller_shipping_cost_pln, estimate via DPD rates
    _is_fbm_mask = orders["fulfillment"] == "FBM"
    _non_amazon_fbm = ~_is_amazon  # non-Amazon orders also need shipping estimate
    _needs_ship_estimate = (~_has_seller) & (_is_fbm_mask | _non_amazon_fbm)

    if _needs_ship_estimate.any():
        _ship_countries = _countries[_needs_ship_estimate]
        _is_pl = _ship_countries == "PL"

        # Non-PL: DPD rate + security + fuel surcharge, converted to PLN
        _dpd_base = _ship_countries.map(DPD_RATES_EUR).fillna(4.0)
        _dpd_total_eur = (_dpd_base + DPD_SECURITY_FEE) * (1 + DPD_FUEL_SURCHARGE)

        # Get EUR FX rates for shipping estimate
        _ship_dates = _order_dates[_needs_ship_estimate]
        _eur_fx_ship = pd.Series(
            [_order_fx_cache.get((d, "EUR"), get_fx(d, "EUR")) for d in _ship_dates],
            index=orders.index[_needs_ship_estimate],
        )
        _dpd_pln = _dpd_total_eur * _eur_fx_ship

        # PL domestic: flat 12 PLN
        _dpd_pln[_is_pl] = DPD_PL_DOMESTIC_PLN

        orders.loc[_needs_ship_estimate, "shipping_pln"] = _dpd_pln.values

    # FBA orders: shipping = 0 (already included in platform fees)
    _is_fba_mask = orders["fulfillment"] == "FBA"
    orders.loc[_is_fba_mask, "shipping_pln"] = 0.0

    # ------------------------------------------------------------------
    # 4. Exportivo 3PL cost: 5 PLN per FBM order  [vectorized]
    # ------------------------------------------------------------------
    orders["fulfillment_cost_pln"] = 0.0
    orders.loc[_is_fbm_mask | _non_amazon_fbm, "fulfillment_cost_pln"] = 5.0

    # ------------------------------------------------------------------
    # 5. PPC ad cost attribution  [vectorized]
    # ------------------------------------------------------------------
    orders["ppc_cost_pln"] = 0.0
    try:
        _ads = load_amazon_ad_spend(days=days)
        if not _ads.empty and "spend" in _ads.columns and "date" in _ads.columns:
            _eur_rate_latest = latest_fx.get("EUR", 4.30)
            _ads_daily = _ads.groupby("date").agg(_ad_spend=("spend", "sum")).reset_index()
            _ads_daily["_ad_spend_pln"] = _ads_daily["_ad_spend"] * _eur_rate_latest
            _ads_spend_by_date = dict(zip(_ads_daily["date"].astype(str), _ads_daily["_ad_spend_pln"]))

            # Compute daily revenue totals for proportional allocation
            _odate_str = _order_dates.values
            orders["_order_date_str"] = _odate_str
            _daily_rev = orders.groupby("_order_date_str")["revenue_pln"].transform("sum")
            _daily_rev = _daily_rev.replace(0, float("nan"))

            # Map daily ad spend to each order's date
            _order_ad_spend = orders["_order_date_str"].map(_ads_spend_by_date).fillna(0)

            # Proportional allocation: order's share = (order_revenue / daily_revenue) * daily_ad_spend
            orders["ppc_cost_pln"] = (_order_ad_spend * orders["revenue_pln"] / _daily_rev).fillna(0)
            orders.drop(columns=["_order_date_str"], inplace=True)
    except Exception:
        pass  # PPC data unavailable, keep zeros

    # ------------------------------------------------------------------
    # 6. FBA storage fee allocation  [vectorized]
    # ------------------------------------------------------------------
    orders["storage_fee_pln"] = 0.0
    try:
        _storage = load_amazon_storage_fees()
        if not _storage.empty and _is_fba_mask.any():
            _eur_rate_latest = latest_fx.get("EUR", 4.30)
            # Aggregate monthly storage fee per ASIN
            _st_agg = _storage.groupby(["month", "asin"]).agg(
                _total_fee=("estimated_storage_fee", "sum"),
            ).reset_index()
            _st_agg["_fee_pln"] = _st_agg["_total_fee"] * _eur_rate_latest

            # Match orders to storage months via order_date
            _fba_idx = orders.index[_is_fba_mask]
            if len(_fba_idx) > 0:
                _fba_dates = pd.to_datetime(orders.loc[_fba_idx, "order_date"], errors="coerce")
                _fba_months = _fba_dates.dt.to_period("M").astype(str)

                # Get ASINs from items for FBA orders
                if not items_df.empty and "asin" in items_df.columns:
                    _item_asin_map = items_df.groupby("order_id")["asin"].first()
                    _fba_asins = orders.loc[_fba_idx, "id"].map(_item_asin_map).fillna("")
                else:
                    _fba_asins = pd.Series("", index=_fba_idx)

                # Count FBA units sold per (month, asin) for allocation
                _fba_temp = pd.DataFrame({
                    "month": _fba_months.values,
                    "asin": _fba_asins.values,
                    "unit_count": orders.loc[_fba_idx, "unit_count"].values,
                }, index=_fba_idx)

                _fba_monthly_units = _fba_temp.groupby(["month", "asin"])["unit_count"].transform("sum")
                _fba_monthly_units = _fba_monthly_units.replace(0, float("nan"))

                # Build storage fee lookup: (month, asin) -> fee_pln
                _st_lookup = dict(zip(
                    zip(_st_agg["month"].astype(str), _st_agg["asin"].astype(str)),
                    _st_agg["_fee_pln"],
                ))

                _monthly_fee = pd.Series(
                    [_st_lookup.get((m, a), 0.0) for m, a in zip(_fba_temp["month"], _fba_temp["asin"])],
                    index=_fba_idx,
                )

                # Per-order storage = (order_units / monthly_units_for_asin) * monthly_storage_fee
                orders.loc[_fba_idx, "storage_fee_pln"] = (
                    _monthly_fee * _fba_temp["unit_count"] / _fba_monthly_units
                ).fillna(0).values
    except Exception:
        pass  # Storage data unavailable, keep zeros

    # ------------------------------------------------------------------
    # 7. Amazon ACCS currency conversion spread  [vectorized]
    # ------------------------------------------------------------------
    ACCS_SPREAD = 0.012  # ~1.2% spread charged by Amazon
    orders["fx_spread_pln"] = 0.0
    _is_amazon_nonpln = _is_amazon & (orders["currency"].fillna("PLN").astype(str).str.upper() != "PLN")
    orders.loc[_is_amazon_nonpln, "fx_spread_pln"] = orders.loc[_is_amazon_nonpln, "revenue_pln"] * ACCS_SPREAD

    # ------------------------------------------------------------------
    # 8. Total costs and profit  [vectorized]
    # ------------------------------------------------------------------
    orders["total_costs_pln"] = (
        orders["cogs_pln"]
        + orders["fees_pln"]
        + orders["shipping_pln"]
        + orders["fulfillment_cost_pln"]
        + orders["ppc_cost_pln"]
        + orders["storage_fee_pln"]
        + orders["fx_spread_pln"]
    )

    orders["profit_pln"] = orders["revenue_net_pln"] - orders["total_costs_pln"]

    # Margin based on net revenue (not brutto)
    orders["margin_pct"] = 0.0
    _has_rev = orders["revenue_net_pln"] > 0
    orders.loc[_has_rev, "margin_pct"] = orders.loc[_has_rev, "profit_pln"] / orders.loc[_has_rev, "revenue_net_pln"] * 100

    # Has COGS flag
    orders["has_cogs"] = orders["cogs_pln"] > 0

    # ROI  [vectorized]
    orders["roi_pct"] = 0.0
    _has_cogs_mask = orders["cogs_pln"] > 0
    orders.loc[_has_cogs_mask, "roi_pct"] = orders.loc[_has_cogs_mask, "profit_pln"] / orders.loc[_has_cogs_mask, "cogs_pln"] * 100

    # Fill missing item info
    orders["first_sku"] = orders["first_sku"].fillna("")
    orders["first_name"] = orders["first_name"].fillna("")

    # Sort
    orders = orders.sort_values("order_date", ascending=False).reset_index(drop=True)

    return orders, items_df


@st.cache_data(ttl=300)
def load_amazon_ad_spend(days=90):
    """Load Amazon advertising/PPC spend data."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _get(
        "amazon_ad_spend",
        {
            "select": "date,campaign_type,spend,sales,impressions,clicks,orders,acos,roas,currency",
            "date": f"gte.{cutoff}",
            "order": "date.asc",
        },
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for col in ["spend", "sales", "acos", "roas"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["impressions", "clicks", "orders"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=300)
def load_amazon_storage_fees():
    """Load Amazon FBA storage fee data (monthly).

    Returns DataFrame with columns: month, asin, estimated_storage_fee, currency
    """
    rows = _get(
        "amazon_storage_fees",
        {
            "select": "month,asin,fnsku,product_name,avg_qty,estimated_storage_fee,currency,product_size_tier",
            "order": "month.desc",
        },
        limit=5000,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["estimated_storage_fee"] = pd.to_numeric(df["estimated_storage_fee"], errors="coerce").fillna(0)
    df["avg_qty"] = pd.to_numeric(df["avg_qty"], errors="coerce").fillna(0).astype(int)
    return df


# ---------------------------------------------------------------------------
# Sprint 1: Analytics Quick Wins — new load_ functions
# ---------------------------------------------------------------------------

def _get_eur_rate(days=90):
    """Helper: get latest EUR->PLN rate from fx_rates."""
    fx = load_fx_rates(days=days)
    if not fx.empty and "currency" in fx.columns:
        eur_rows = fx[fx["currency"] == "EUR"]
        if not eur_rows.empty:
            _r = pd.to_numeric(eur_rows.iloc[-1].get("rate_pln", 4.30), errors="coerce")
            if not pd.isna(_r) and _r > 0:
                return float(_r)
    return 4.30


@st.cache_data(ttl=300)
def load_tacos_trend(days=90):
    """TACoS (Total Ad Cost of Sale) trend: ad_spend / total_revenue * 100."""
    ads = load_amazon_ad_spend(days=days)
    dm = load_daily_metrics(days=days)

    empty = pd.DataFrame(columns=["date", "ad_spend_pln", "revenue_pln", "tacos", "tacos_7d"])
    if ads.empty or dm.empty:
        return empty

    eur_rate = _get_eur_rate(days)

    ads_daily = ads.groupby("date").agg(ad_spend=("spend", "sum")).reset_index()
    ads_daily["ad_spend_pln"] = ads_daily["ad_spend"] * eur_rate

    rev_daily = dm.groupby("date").agg(revenue_pln=("revenue_pln", "sum")).reset_index()

    merged = rev_daily.merge(ads_daily[["date", "ad_spend_pln"]], on="date", how="outer").fillna(0)
    merged["tacos"] = merged.apply(
        lambda r: r["ad_spend_pln"] / r["revenue_pln"] * 100 if r["revenue_pln"] > 0 else 0, axis=1
    )
    merged = merged.sort_values("date").reset_index(drop=True)
    merged["tacos_7d"] = merged["tacos"].rolling(7, min_periods=1).mean()
    return merged


@st.cache_data(ttl=300)
def load_organic_paid_split(days=90):
    """Organic vs paid revenue split. Organic = Total Revenue - Ad Sales (PLN)."""
    ads = load_amazon_ad_spend(days=days)
    dm = load_daily_metrics(days=days)

    empty = pd.DataFrame(columns=["date", "revenue_pln", "organic_pln", "paid_pln", "organic_pct"])
    if dm.empty:
        return empty

    eur_rate = _get_eur_rate(days)

    rev_daily = dm.groupby("date").agg(revenue_pln=("revenue_pln", "sum")).reset_index()

    if not ads.empty:
        ads_daily = ads.groupby("date").agg(paid_sales=("sales", "sum")).reset_index()
        ads_daily["paid_pln"] = ads_daily["paid_sales"] * eur_rate
        merged = rev_daily.merge(ads_daily[["date", "paid_pln"]], on="date", how="left").fillna(0)
    else:
        merged = rev_daily.copy()
        merged["paid_pln"] = 0.0

    merged["organic_pln"] = (merged["revenue_pln"] - merged["paid_pln"]).clip(lower=0)
    merged["organic_pct"] = merged.apply(
        lambda r: r["organic_pln"] / r["revenue_pln"] * 100 if r["revenue_pln"] > 0 else 100, axis=1
    )
    return merged.sort_values("date").reset_index(drop=True)


@st.cache_data(ttl=300)
def load_buybox_history(days=90):
    """Buy Box Win Rate history from amazon_pricing snapshots.

    Won = our_price <= buy_box_price * 1.005 (within 0.5% tolerance).
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = _get(
        "amazon_pricing",
        {
            "select": "snapshot_date,asin,our_price,buy_box_price",
            "snapshot_date": f"gte.{cutoff}",
            "order": "snapshot_date.asc",
        },
        limit=50000,
    )
    pricing = pd.DataFrame(rows)

    empty = pd.DataFrame(columns=["snapshot_date", "total_asins", "won", "win_rate_pct", "win_rate_7d"])
    if pricing.empty:
        return empty

    pricing["our_price"] = pd.to_numeric(pricing["our_price"], errors="coerce")
    pricing["buy_box_price"] = pd.to_numeric(pricing["buy_box_price"], errors="coerce")

    valid = pricing[
        pricing["our_price"].notna() & (pricing["our_price"] > 0) &
        pricing["buy_box_price"].notna() & (pricing["buy_box_price"] > 0)
    ].copy()
    if valid.empty:
        return empty

    valid["won"] = valid["our_price"] <= valid["buy_box_price"] * 1.005

    result = valid.groupby("snapshot_date").agg(
        total_asins=("asin", "nunique"),
        won=("won", "sum"),
    ).reset_index()
    result["win_rate_pct"] = result.apply(
        lambda r: r["won"] / r["total_asins"] * 100 if r["total_asins"] > 0 else 0, axis=1
    )
    result = result.sort_values("snapshot_date").reset_index(drop=True)
    result["win_rate_7d"] = result["win_rate_pct"].rolling(7, min_periods=1).mean()
    return result


@st.cache_data(ttl=300)
def load_abc_xyz_analysis(days=90):
    """ABC/XYZ analysis on daily_metrics.

    ABC: Revenue — A (top 80%), B (80-95%), C (95-100%).
    XYZ: Demand variability via CV of weekly revenue — X (<0.5), Y (0.5-1.0), Z (>1.0).
    """
    dm = load_daily_metrics(days=days)

    empty = pd.DataFrame(columns=[
        "sku", "revenue", "revenue_pct", "cumulative_pct", "abc_class",
        "cv", "xyz_class", "segment", "units", "orders",
    ])
    if dm.empty:
        return empty

    # --- ABC ---
    sku_rev = dm.groupby("sku").agg(
        revenue=("revenue_pln", "sum"),
        units=("units", "sum"),
        orders=("orders_count", "sum"),
    ).reset_index()

    total_rev = sku_rev["revenue"].sum()
    if total_rev <= 0:
        return empty

    sku_rev = sku_rev.sort_values("revenue", ascending=False).reset_index(drop=True)
    sku_rev["revenue_pct"] = sku_rev["revenue"] / total_rev * 100
    sku_rev["cumulative_pct"] = sku_rev["revenue_pct"].cumsum()
    sku_rev["abc_class"] = sku_rev["cumulative_pct"].apply(
        lambda c: "A" if c <= 80 else ("B" if c <= 95 else "C")
    )

    # --- XYZ ---
    dm_c = dm.copy()
    dm_c["date_parsed"] = pd.to_datetime(dm_c["date"])
    dm_c["year_week"] = dm_c["date_parsed"].dt.strftime("%G-W%V")

    weekly = dm_c.groupby(["sku", "year_week"]).agg(
        weekly_rev=("revenue_pln", "sum"),
    ).reset_index()

    cv_data = weekly.groupby("sku").agg(
        mean_rev=("weekly_rev", "mean"),
        std_rev=("weekly_rev", "std"),
    ).reset_index()
    cv_data["std_rev"] = cv_data["std_rev"].fillna(0)
    cv_data["cv"] = cv_data.apply(
        lambda r: r["std_rev"] / r["mean_rev"] if r["mean_rev"] > 0 else 999, axis=1
    )
    cv_data["xyz_class"] = cv_data["cv"].apply(
        lambda v: "X" if v < 0.5 else ("Y" if v <= 1.0 else "Z")
    )

    # Merge
    result = sku_rev.merge(cv_data[["sku", "cv", "xyz_class"]], on="sku", how="left")
    result["cv"] = result["cv"].fillna(999)
    result["xyz_class"] = result["xyz_class"].fillna("Z")
    result["segment"] = result["abc_class"] + result["xyz_class"]
    return result


@st.cache_data(ttl=300)
def load_inventory_velocity(days=30):
    """Inventory velocity: sell-through rate and days of inventory (DOI).

    Sell-through = units_sold / (units_sold + current_stock).
    DOI = current_stock / avg_daily_sales (capped at 999).
    """
    inv = load_amazon_inventory()
    dm = load_daily_metrics(days=days)

    empty = pd.DataFrame(columns=[
        "sku", "product_name", "current_stock", "avg_daily_sales",
        "sell_through_rate", "days_of_inventory",
    ])
    if inv.empty or dm.empty:
        return empty

    inv["fulfillable_qty"] = pd.to_numeric(inv["fulfillable_qty"], errors="coerce").fillna(0).astype(int)

    latest = inv["snapshot_date"].max()
    inv_latest = inv[inv["snapshot_date"] == latest].copy()
    inv_agg = inv_latest.groupby("sku").agg(
        current_stock=("fulfillable_qty", "sum"),
        product_name=("product_name", "first"),
    ).reset_index()

    sales = dm.groupby("sku").agg(total_units=("units", "sum")).reset_index()
    sales["avg_daily_sales"] = sales["total_units"] / max(days, 1)

    result = inv_agg.merge(sales[["sku", "total_units", "avg_daily_sales"]], on="sku", how="left")
    result["total_units"] = result["total_units"].fillna(0)
    result["avg_daily_sales"] = result["avg_daily_sales"].fillna(0)

    denom = result["total_units"] + result["current_stock"]
    result["sell_through_rate"] = (result["total_units"] / denom * 100).where(denom > 0, 0)
    result["days_of_inventory"] = (
        result["current_stock"] / result["avg_daily_sales"]
    ).where(result["avg_daily_sales"] > 0, 999).clip(upper=999)

    result = result[["sku", "product_name", "current_stock", "avg_daily_sales",
                      "sell_through_rate", "days_of_inventory"]].copy()
    return result.sort_values("days_of_inventory").reset_index(drop=True)


@st.cache_data(ttl=300)
def load_marketplace_pnl(days=90):
    """P&L breakdown per marketplace."""
    dm = load_daily_metrics(days=days)
    platforms = load_platforms()

    empty = pd.DataFrame(columns=[
        "marketplace", "revenue_pln", "cogs", "platform_fees", "shipping_cost",
        "gross_profit", "margin_pct", "orders_count", "units",
    ])
    if dm.empty:
        return empty

    dm["marketplace"] = dm["platform_id"].map(
        lambda x: platforms.get(x, {}).get("code", f"#{x}")
    )

    agg_dict = {
        "revenue_pln": "sum", "cogs": "sum", "fees": "sum",
        "profit": "sum", "orders_count": "sum", "units": "sum",
    }
    if "shipping_cost" in dm.columns:
        agg_dict["shipping_cost"] = "sum"

    agg = dm.groupby("marketplace").agg(agg_dict).reset_index()
    if "shipping_cost" not in agg.columns:
        agg["shipping_cost"] = 0.0

    agg.rename(columns={"fees": "platform_fees", "profit": "gross_profit"}, inplace=True)
    agg["margin_pct"] = agg.apply(
        lambda r: r["gross_profit"] / r["revenue_pln"] * 100 if r["revenue_pln"] > 0 else 0, axis=1
    )
    return agg.sort_values("revenue_pln", ascending=False).reset_index(drop=True)


def get_marketplace_names():
    """Get marketplace ID to display name mapping."""
    try:
        from etl.config import MARKETPLACE_TO_PLATFORM
        mkt_names = {v: v.replace("amazon_", "").upper() for v in MARKETPLACE_TO_PLATFORM.values()}
        return {k: mkt_names.get(v, v) for k, v in MARKETPLACE_TO_PLATFORM.items()}
    except ImportError:
        return {}


# --- VAT ---

# Marketplace ID -> (country code, country name, VAT rate %)
MARKETPLACE_VAT_MAP = {
    "A1PA6795UKMFR9": ("DE", "Germany", 19.0),
    "A13V1IB3VIYZZH": ("FR", "France", 20.0),
    "APJ6JRA9NG5V4":  ("IT", "Italy", 22.0),
    "A1RKKUPIHCS9HS": ("ES", "Spain", 21.0),
    "A1805IZSGTT6HS": ("NL", "Netherlands", 21.0),
    "A2NODRKZP88ZB9": ("SE", "Sweden", 25.0),
    "A1C3SOZRARQ6R3": ("PL", "Poland", 23.0),
    "AMEN7PMS3EDWL":  ("BE", "Belgium", 21.0),
}

# Platform code -> (country code, country name, VAT rate %)
PLATFORM_VAT_MAP = {
    "amazon_de": ("DE", "Germany", 19.0),
    "amazon_fr": ("FR", "France", 20.0),
    "amazon_it": ("IT", "Italy", 22.0),
    "amazon_es": ("ES", "Spain", 21.0),
    "amazon_nl": ("NL", "Netherlands", 21.0),
    "amazon_se": ("SE", "Sweden", 25.0),
    "amazon_pl": ("PL", "Poland", 23.0),
    "amazon_be": ("BE", "Belgium", 21.0),
    "allegro":   ("PL", "Poland", 23.0),
}


@st.cache_data(ttl=300)
def load_vat_summary(days=90):
    """Load daily_metrics grouped by country with VAT calculations.

    Maps platform_id -> platform code -> country -> VAT rate.
    Returns a DataFrame with columns:
        country, country_name, revenue_pln (netto), vat_rate, estimated_vat,
        orders_count, units
    And a monthly DataFrame for trend charts.
    """
    dm = load_daily_metrics(days=days)
    platforms = load_platforms()

    if dm.empty:
        empty = pd.DataFrame(columns=[
            "country", "country_name", "revenue_pln", "vat_rate",
            "estimated_vat", "orders_count", "units",
        ])
        return empty, pd.DataFrame(columns=["month", "country", "revenue_pln", "estimated_vat"])

    # Map platform_id -> platform code
    dm["platform_code"] = dm["platform_id"].map(
        lambda x: platforms.get(x, {}).get("code", "")
    )

    # Map platform code -> country info
    dm["country"] = dm["platform_code"].map(
        lambda x: PLATFORM_VAT_MAP.get(x, (None, None, None))[0]
    )
    dm["country_name"] = dm["platform_code"].map(
        lambda x: PLATFORM_VAT_MAP.get(x, (None, None, None))[1]
    )
    dm["vat_rate"] = dm["platform_code"].map(
        lambda x: PLATFORM_VAT_MAP.get(x, (None, None, 0))[2]
    )

    # Filter only rows with known country (EU marketplaces)
    eu = dm[dm["country"].notna()].copy()
    if eu.empty:
        empty = pd.DataFrame(columns=[
            "country", "country_name", "revenue_pln", "vat_rate",
            "estimated_vat", "orders_count", "units",
        ])
        return empty, pd.DataFrame(columns=["month", "country", "revenue_pln", "estimated_vat"])

    # Revenue in daily_metrics is gross (includes VAT). Netto = gross / (1 + rate)
    eu["revenue_netto"] = eu["revenue_pln"] / (1 + eu["vat_rate"] / 100)
    eu["estimated_vat"] = eu["revenue_pln"] - eu["revenue_netto"]

    # Aggregate by country
    by_country = eu.groupby(["country", "country_name", "vat_rate"]).agg(
        revenue_pln=("revenue_netto", "sum"),
        estimated_vat=("estimated_vat", "sum"),
        orders_count=("orders_count", "sum"),
        units=("units", "sum"),
    ).reset_index()
    by_country = by_country.sort_values("revenue_pln", ascending=False)

    # Monthly trend
    eu["month"] = pd.to_datetime(eu["date"]).dt.to_period("M").astype(str)
    monthly = eu.groupby(["month", "country"]).agg(
        revenue_pln=("revenue_netto", "sum"),
        estimated_vat=("estimated_vat", "sum"),
    ).reset_index()
    monthly = monthly.sort_values("month")

    return by_country, monthly
