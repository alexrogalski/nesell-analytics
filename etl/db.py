"""Database helper: Supabase REST API client (no direct PostgreSQL needed)."""
import requests, json
from . import config

_BASE = config.SUPABASE_URL
_HEADERS = {
    "apikey": config.SUPABASE_KEY,
    "Authorization": f"Bearer {config.SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}


def _url(table):
    return f"{_BASE}/rest/v1/{table}"


def _post(table, data, on_conflict=None):
    """Insert or upsert rows via PostgREST."""
    headers = dict(_HEADERS)
    if on_conflict:
        headers["Prefer"] = f"return=representation,resolution=merge-duplicates"
    params = {}
    if on_conflict:
        params["on_conflict"] = on_conflict
    resp = requests.post(_url(table), headers=headers, json=data, params=params)
    if resp.status_code not in (200, 201):
        raise Exception(f"DB {table} insert error {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def _get(table, params=None):
    """Select rows via PostgREST."""
    resp = requests.get(_url(table), headers=_HEADERS, params=params or {})
    if resp.status_code != 200:
        raise Exception(f"DB {table} select error {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def _patch(table, match_params, data):
    """Update rows matching filter."""
    headers = dict(_HEADERS)
    resp = requests.patch(_url(table), headers=headers, json=data, params=match_params)
    if resp.status_code not in (200, 204):
        raise Exception(f"DB {table} patch error {resp.status_code}: {resp.text[:200]}")
    return resp.json() if resp.text else []


# --- Public API (same interface as before, but conn is ignored/unused) ---

def get_conn():
    """Compatibility stub. Returns None — we use REST API."""
    # Verify connection works
    try:
        _get("platforms", {"select": "id", "limit": "1"})
        print("  [DB] Connected to Supabase REST API")
    except Exception as e:
        raise Exception(f"Cannot connect to Supabase: {e}")
    return None


def get_platform_map(conn):
    """Return {platform_code: id} mapping."""
    rows = _get("platforms", {"select": "code,id"})
    return {r["code"]: r["id"] for r in rows}


def upsert_orders(conn, orders):
    """Upsert orders into DB."""
    if not orders:
        return 0
    # PostgREST upsert with on_conflict
    rows = []
    for o in orders:
        rows.append({
            "external_id": o["external_id"],
            "platform_id": o["platform_id"],
            "platform_order_id": o.get("platform_order_id"),
            "order_date": o["order_date"],
            "status": o.get("status"),
            "buyer_email": o.get("buyer_email"),
            "shipping_country": o.get("shipping_country"),
            "shipping_cost": o.get("shipping_cost", 0),
            "total_paid": o["total_paid"],
            "currency": o["currency"],
            "total_paid_pln": o.get("total_paid_pln"),
            "platform_fee": o.get("platform_fee", 0),
            "platform_fee_pln": o.get("platform_fee_pln"),
            "raw_data": o.get("raw_data"),
        })
    # Batch in chunks of 500 (PostgREST limit)
    total = 0
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        _post("orders", chunk, on_conflict="external_id,platform_id")
        total += len(chunk)
    return total


def upsert_order_items(conn, items):
    """Insert order items (no upsert — append only)."""
    if not items:
        return 0
    rows = []
    for it in items:
        rows.append({
            "order_id": it["order_id"],
            "sku": it.get("sku"),
            "name": it.get("name"),
            "quantity": it["quantity"],
            "unit_price": it["unit_price"],
            "currency": it["currency"],
            "unit_price_pln": it.get("unit_price_pln"),
            "unit_cost": it.get("unit_cost"),
            "unit_cost_pln": it.get("unit_cost_pln"),
            "asin": it.get("asin"),
        })
    total = 0
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        _post("order_items", chunk)
        total += len(chunk)
    return total


def upsert_products(conn, products):
    """Upsert products from catalog."""
    if not products:
        return 0
    rows = []
    for p in products:
        rows.append({
            "sku": p["sku"],
            "name": p["name"],
            "brand": p.get("brand"),
            "source": p.get("source"),
            "category": p.get("category"),
            "cost_pln": p.get("cost_pln"),
            "weight_g": p.get("weight_g"),
            "is_parent": p.get("is_parent", False),
            "parent_sku": p.get("parent_sku"),
            "ean": p.get("ean"),
            "active": p.get("active", True),
        })
    total = 0
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        _post("products", chunk, on_conflict="sku")
        total += len(chunk)
    return total


def upsert_fx_rate(conn, date, currency, rate_pln):
    """Upsert single FX rate."""
    _post("fx_rates", [{"date": str(date), "currency": currency, "rate_pln": float(rate_pln)}],
          on_conflict="date,currency")


def get_fx_rate(conn, date, currency):
    """Get FX rate for a date. Falls back to most recent."""
    if currency == "PLN":
        return 1.0
    rows = _get("fx_rates", {
        "select": "rate_pln",
        "currency": f"eq.{currency}",
        "date": f"lte.{date}",
        "order": "date.desc",
        "limit": "1",
    })
    return float(rows[0]["rate_pln"]) if rows else None


def upsert_daily_metrics(conn, metrics):
    """Upsert daily aggregated metrics."""
    if not metrics:
        return 0
    rows = []
    for m in metrics:
        rows.append({
            "date": str(m["date"]),
            "platform_id": m["platform_id"],
            "sku": m["sku"],
            "orders_count": m["orders_count"],
            "units_sold": m["units_sold"],
            "revenue": float(m["revenue"]),
            "revenue_pln": float(m["revenue_pln"]),
            "cogs": float(m["cogs"]),
            "platform_fees": float(m["platform_fees"]),
            "shipping_cost": float(m["shipping_cost"]),
            "gross_profit": float(m["gross_profit"]),
            "margin_pct": float(m["margin_pct"]),
        })
    total = 0
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        _post("daily_metrics", chunk, on_conflict="date,platform_id,sku")
        total += len(chunk)
    return total


def get_order_id_by_external(conn, external_id, platform_id):
    """Get internal order ID by external ID."""
    rows = _get("orders", {
        "select": "id",
        "external_id": f"eq.{external_id}",
        "platform_id": f"eq.{platform_id}",
        "limit": "1",
    })
    return rows[0]["id"] if rows else None


def run_rpc(conn, function_name, params=None):
    """Call a PostgreSQL function via PostgREST RPC."""
    resp = requests.post(
        f"{_BASE}/rest/v1/rpc/{function_name}",
        headers=_HEADERS,
        json=params or {}
    )
    if resp.status_code not in (200, 204):
        raise Exception(f"RPC {function_name} error: {resp.text[:200]}")
    return resp.json() if resp.text else None
