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


# --- Public API ---

def verify_connection():
    """Verify Supabase REST API connection works."""
    try:
        _get("platforms", {"select": "id", "limit": "1"})
        print("  [DB] Connected to Supabase REST API")
    except Exception as e:
        raise Exception(f"Cannot connect to Supabase: {e}")


def get_platform_map():
    """Return {platform_code: id} mapping."""
    rows = _get("platforms", {"select": "code,id"})
    return {r["code"]: r["id"] for r in rows}


def upsert_orders(orders):
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
            "notes": o.get("notes"),
            "raw_data": o.get("raw_data"),
        })
    # Batch in chunks of 500 (PostgREST limit)
    total = 0
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        _post("orders", chunk, on_conflict="external_id,platform_id")
        total += len(chunk)
    return total


def upsert_order_items(items):
    """Upsert order items (merge duplicates by summing quantities)."""
    if not items:
        return 0
    # Merge by (order_id, sku) — sum quantities for duplicates
    seen = {}
    for it in items:
        key = (it["order_id"], it.get("sku"))
        if key in seen:
            # Duplicate found — sum quantities, use most complete data
            existing = seen[key]
            existing["quantity"] += it["quantity"]
            # Prefer non-empty values from new item
            if it.get("name") and not existing.get("name"):
                existing["name"] = it["name"]
            if it.get("asin") and not existing.get("asin"):
                existing["asin"] = it.get("asin")
        else:
            seen[key] = {
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
            }
    rows = list(seen.values())
    total = 0
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        _post("order_items", chunk, on_conflict="order_id,sku")
        total += len(chunk)
    return total


def upsert_products(products):
    """Upsert products from catalog (deduplicated by SKU within batch)."""
    if not products:
        return 0
    # Deduplicate by SKU — keep last occurrence (later entries overwrite earlier)
    seen = {}
    for p in products:
        sku = p.get("sku", "")
        if not sku:
            continue
        row = {
            "sku": sku,
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
            "image_url": p.get("image_url"),
        }
        # Prefer entries with better data (non-empty name, non-null cost)
        existing = seen.get(sku)
        if existing:
            # Keep the one with a real name (not barcode), or with cost
            existing_name = existing.get("name", "")
            new_name = row.get("name", "")
            if (not existing_name or existing_name == sku or existing_name.isdigit()) and new_name and new_name != sku:
                seen[sku] = row
            elif not existing.get("cost_pln") and row.get("cost_pln"):
                seen[sku] = row
        else:
            seen[sku] = row

    rows = list(seen.values())
    total = 0
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        _post("products", chunk, on_conflict="sku")
        total += len(chunk)
    return total


def upsert_fx_rate(date, currency, rate_pln):
    """Upsert single FX rate."""
    _post("fx_rates", [{"date": str(date), "currency": currency, "rate_pln": float(rate_pln)}],
          on_conflict="date,currency")


def get_fx_rate(date, currency):
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


def upsert_daily_metrics(metrics):
    """Upsert daily aggregated metrics.

    Includes vat_pln and revenue_net_pln columns when present in the input.
    Falls back gracefully if the DB columns don't exist yet (pre-migration).
    """
    if not metrics:
        return 0

    # Check if new columns are present in the data
    has_vat = any("vat_pln" in m for m in metrics)

    rows = []
    for m in metrics:
        row = {
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
        }
        if has_vat:
            row["vat_pln"] = float(m.get("vat_pln", 0))
            row["revenue_net_pln"] = float(m.get("revenue_net_pln", 0))
        rows.append(row)

    total = 0
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        try:
            _post("daily_metrics", chunk, on_conflict="date,platform_id,sku")
        except Exception as e:
            # If new columns don't exist yet, retry without them
            if has_vat and ("vat_pln" in str(e) or "revenue_net_pln" in str(e)):
                print("  [WARN] daily_metrics missing vat_pln/revenue_net_pln columns, writing without them")
                for row in chunk:
                    row.pop("vat_pln", None)
                    row.pop("revenue_net_pln", None)
                _post("daily_metrics", chunk, on_conflict="date,platform_id,sku")
            else:
                raise
        total += len(chunk)
    return total


def count_order_items(order_id):
    """Count existing order items for a given order_id."""
    resp = requests.get(
        _url("order_items"),
        headers=_HEADERS,
        params={
            "select": "id",
            "order_id": f"eq.{order_id}",
            "limit": "1",
        },
    )
    if resp.status_code != 200:
        return 0
    return len(resp.json())


def get_order_id_by_external(external_id, platform_id):
    """Get internal order ID by external ID."""
    rows = _get("orders", {
        "select": "id",
        "external_id": f"eq.{external_id}",
        "platform_id": f"eq.{platform_id}",
        "limit": "1",
    })
    return rows[0]["id"] if rows else None


def run_rpc(function_name, params=None):
    """Call a PostgreSQL function via PostgREST RPC."""
    resp = requests.post(
        f"{_BASE}/rest/v1/rpc/{function_name}",
        headers=_HEADERS,
        json=params or {}
    )
    if resp.status_code not in (200, 204):
        raise Exception(f"RPC {function_name} error: {resp.text[:200]}")
    return resp.json() if resp.text else None


# ── Amazon data tables ───────────────────────────────────────────────

def upsert_amazon_traffic(records):
    """Upsert Amazon traffic data (sessions, page views, Buy Box %)."""
    if not records:
        return 0
    # Deduplicate by (date, asin, marketplace_id) within batch
    seen = {}
    for r in records:
        key = (r.get("date"), r.get("asin"), r.get("marketplace_id"))
        seen[key] = r
    deduped = list(seen.values())
    total = 0
    for i in range(0, len(deduped), 500):
        chunk = deduped[i:i+500]
        _post("amazon_traffic", chunk, on_conflict="date,asin,marketplace_id")
        total += len(chunk)
    return total


def upsert_amazon_inventory(records):
    """Upsert FBA inventory snapshots."""
    if not records:
        return 0
    from datetime import date
    today = str(date.today())
    for r in records:
        r.setdefault("snapshot_date", today)
    total = 0
    for i in range(0, len(records), 500):
        chunk = records[i:i+500]
        _post("amazon_inventory", chunk, on_conflict="snapshot_date,sku")
        total += len(chunk)
    return total


def upsert_amazon_storage_fees(records):
    """Upsert FBA storage fee data."""
    if not records:
        return 0
    total = 0
    for i in range(0, len(records), 500):
        chunk = records[i:i+500]
        _post("amazon_storage_fees", chunk, on_conflict="month,asin")
        total += len(chunk)
    return total


def upsert_amazon_fba_fees(records):
    """Upsert estimated FBA fees per SKU (deduplicate within batch)."""
    if not records:
        return 0
    # Deduplicate by SKU within batch (keep last occurrence)
    seen = {}
    for r in records:
        seen[r.get("sku", "")] = r
    deduped = list(seen.values())
    total = 0
    for i in range(0, len(deduped), 500):
        chunk = deduped[i:i+500]
        _post("amazon_fba_fees", chunk, on_conflict="sku")
        total += len(chunk)
    return total


def upsert_amazon_returns(records):
    """Upsert Amazon return records (deduplicated by return_date, order_id, sku, quantity)."""
    if not records:
        return 0
    # Deduplicate within batch by (return_date, order_id, sku, quantity)
    seen = {}
    for r in records:
        key = (r.get("return_date"), r.get("order_id", ""), r.get("sku"), r.get("quantity", 1))
        seen[key] = r
    deduped = list(seen.values())
    total = 0
    for i in range(0, len(deduped), 500):
        chunk = deduped[i:i+500]
        _post("amazon_returns", chunk, on_conflict="return_date,order_id,sku,quantity")
        total += len(chunk)
    return total


def upsert_amazon_reimbursements(records):
    """Upsert Amazon reimbursement records (deduplicate within batch)."""
    if not records:
        return 0
    # Deduplicate by (reimbursement_id, sku) within batch
    seen = {}
    for r in records:
        key = (r.get("reimbursement_id", ""), r.get("sku", ""))
        seen[key] = r
    deduped = list(seen.values())
    total = 0
    for i in range(0, len(deduped), 500):
        chunk = deduped[i:i+500]
        _post("amazon_reimbursements", chunk, on_conflict="reimbursement_id,sku")
        total += len(chunk)
    return total


def upsert_amazon_bsr(records):
    """Upsert BSR snapshots."""
    if not records:
        return 0
    from datetime import date
    today = str(date.today())
    for r in records:
        r.setdefault("snapshot_date", today)
    total = 0
    for i in range(0, len(records), 500):
        chunk = records[i:i+500]
        _post("amazon_bsr", chunk, on_conflict="snapshot_date,asin,marketplace_id,category_id")
        total += len(chunk)
    return total


def upsert_amazon_pricing(records):
    """Upsert competitive pricing snapshots."""
    if not records:
        return 0
    from datetime import date
    today = str(date.today())
    for r in records:
        r.setdefault("snapshot_date", today)
    total = 0
    for i in range(0, len(records), 500):
        chunk = records[i:i+500]
        _post("amazon_pricing", chunk, on_conflict="snapshot_date,asin,marketplace_id")
        total += len(chunk)
    return total


def upsert_amazon_settlements(records):
    """Insert settlement records (append-only)."""
    if not records:
        return 0
    total = 0
    for i in range(0, len(records), 500):
        chunk = records[i:i+500]
        _post("amazon_settlements", chunk)
        total += len(chunk)
    return total


def upsert_amazon_ad_spend(records):
    """Upsert Amazon advertising/PPC spend data."""
    if not records:
        return 0
    total = 0
    for i in range(0, len(records), 500):
        chunk = records[i:i+500]
        _post("amazon_ad_spend", chunk, on_conflict="date,campaign_name,marketplace_id")
        total += len(chunk)
    return total


def upsert_amazon_restock(records):
    """Upsert Amazon restock recommendation snapshots (per snapshot_date, sku)."""
    if not records:
        return 0
    # Deduplicate within batch by (snapshot_date, sku) — keep last occurrence
    seen = {}
    for r in records:
        key = (r.get("snapshot_date"), r.get("sku", ""))
        seen[key] = r
    deduped = list(seen.values())
    total = 0
    for i in range(0, len(deduped), 500):
        chunk = deduped[i:i+500]
        _post("amazon_restock", chunk, on_conflict="snapshot_date,sku")
        total += len(chunk)
    return total


def upsert_amazon_aged_inventory(records):
    """Upsert FBA aged inventory snapshots (per snapshot_date, sku)."""
    if not records:
        return 0
    # Deduplicate within batch by (snapshot_date, sku) — keep last occurrence
    seen = {}
    for r in records:
        key = (r.get("snapshot_date"), r.get("sku", ""))
        seen[key] = r
    deduped = list(seen.values())
    total = 0
    for i in range(0, len(deduped), 500):
        chunk = deduped[i:i+500]
        _post("amazon_aged_inventory", chunk, on_conflict="snapshot_date,sku")
        total += len(chunk)
    return total
