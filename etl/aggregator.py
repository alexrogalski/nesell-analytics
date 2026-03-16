"""Aggregator: compute daily metrics from raw orders via REST API."""
from datetime import date, timedelta
from collections import defaultdict
from . import db, fx_rates

# ---------------------------------------------------------------------------
# SKU normalization: map order-item SKUs to canonical product SKUs
# ---------------------------------------------------------------------------

# Valid size suffixes for stripping (dash or underscore separated)
_SIZE_SUFFIXES = frozenset({
    'XS', 'S', 'M', 'L', 'XL', 'XXL', '2XL', '3XL', 'OS', 'OSFA',
    '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46',
    '4', '6', '8', '10', '12', '14', '16',
})


def normalize_sku(sku: str, cost_map: dict[str, float]) -> str:
    """Try to find the canonical product SKU matching this order item SKU.

    Applies a fallback chain of transformations until a match with cost > 0
    is found in the products cost_map. Returns the original SKU if no match.
    """
    if not sku or sku == 'unknown':
        return sku

    # 0. Trim whitespace (some SKUs have leading spaces from API)
    clean = sku.strip()
    if clean != sku and clean in cost_map and cost_map[clean] > 0:
        return clean

    # Direct match (most common path)
    if clean in cost_map and cost_map[clean] > 0:
        return clean

    # 1. Strip trailing dashes: 194954124766--- -> 194954124766
    nodash = clean.rstrip('-')
    if nodash != clean and nodash in cost_map and cost_map[nodash] > 0:
        return nodash

    # 2. Strip leading zeros: 0885178375941 -> 885178375941
    nozero = clean.lstrip('0')
    if nozero != clean and nozero in cost_map and cost_map[nozero] > 0:
        return nozero

    # 3. Both: leading zeros + trailing dashes: 0027131043287--- -> 27131043287
    both = clean.lstrip('0').rstrip('-')
    if both != clean and both != nozero and both != nodash:
        if both in cost_map and cost_map[both] > 0:
            return both

    # 4. Strip size suffix with dash: SX7664-010-S -> SX7664-010
    parts = clean.rsplit('-', 1)
    if len(parts) == 2 and parts[1].upper() in _SIZE_SUFFIXES:
        base = parts[0]
        if base in cost_map and cost_map[base] > 0:
            return base

    # 5. Strip size suffix with underscore: SX7667-100_M -> SX7667-100
    parts2 = clean.rsplit('_', 1)
    if len(parts2) == 2 and parts2[1].upper() in _SIZE_SUFFIXES:
        base2 = parts2[0]
        if base2 in cost_map and cost_map[base2] > 0:
            return base2

    # 6. FBA- prefix removal: FBA-6843674_7856 -> 6843674_7856
    if clean.startswith('FBA-'):
        stripped = clean[4:]
        if stripped in cost_map and cost_map[stripped] > 0:
            return stripped
        # Also try PFT format: FBA-6843674_7856 -> PFT-6843674-7856
        pft = 'PFT-' + stripped.replace('_', '-')
        if pft in cost_map and cost_map[pft] > 0:
            return pft

    # 7. Printful underscore -> PFT: 6843674_7854 -> PFT-6843674-7854
    #    Handles both "6843674_7854" and " 6843674_7854" (already stripped)
    if '_' in clean and clean.replace('_', '').isdigit():
        pft = 'PFT-' + clean.replace('_', '-')
        if pft in cost_map and cost_map[pft] > 0:
            return pft

    # 8. EAN/barcode lookup: check if any product SKU contains this value
    #    Only for numeric SKUs >= 8 digits (EAN-8, EAN-13, UPC)
    if clean.isdigit() and len(clean) >= 8:
        for prod_sku, cost in cost_map.items():
            if cost > 0 and clean in prod_sku and prod_sku != clean:
                return prod_sku

    return sku  # No match found, return original

# Real average fee rates calculated from 593 orders with actual Amazon Finances data.
# FBA orders have referral (~15%) + FBA fulfillment (~18-19%) + other (~1%) = ~34%.
# FBM orders (from Baselinker) only have referral (~15%), no FBA fulfillment fees.
AMAZON_FBA_FEE_PCT = {
    "EUR": 34.73,  # 584 FBA orders: 5,641.68 / 16,246.49
    "SEK": 34.73,  # Same as EUR (Amazon SE charges in EUR effectively)
    "GBP": 34.73,  # Same tier as EUR
    "PLN": 18.67,  # 9 orders: 177.98 / 953.21 (amazon_pl, lower FBA fees)
}
AMAZON_FBA_DEFAULT_FEE_PCT = 34.0  # Weighted average fallback for FBA

# FBM (Fulfilled by Merchant) orders only have referral fee, no FBA fulfillment
AMAZON_FBM_FEE_PCT = 15.45  # Referral fee only (~15.45%)

# Amazon platform IDs (1-9)
AMAZON_PLATFORM_IDS = set(range(1, 10))


def _get_all(table, params):
    """Paginated GET — fetch all rows (PostgREST default limit is 1000)."""
    all_rows = []
    offset = 0
    batch_size = 1000
    while True:
        p = {**params, "limit": str(batch_size), "offset": str(offset)}
        rows = db._get(table, p)
        all_rows.extend(rows)
        if len(rows) < batch_size:
            break
        offset += batch_size
    return all_rows


def aggregate_daily(conn, days_back: int = 90):
    """Compute daily_metrics from orders + order_items + product costs.

    Uses real per-order fees (from orders.platform_fee) when available,
    falls back to flat fee_pct from platforms table.
    """
    cutoff = str(date.today() - timedelta(days=days_back))

    # Get all non-cancelled orders in period (paginated)
    # seller_shipping_cost = actual courier cost (from shipping_costs module)
    # shipping_cost = buyer's delivery_price (revenue side, from Baselinker)
    orders = _get_all("orders", {
        "select": "id,external_id,platform_id,order_date,currency,status,platform_fee,total_paid,shipping_cost,seller_shipping_cost,seller_shipping_cost_pln",
        "order_date": f"gte.{cutoff}",
        "status": "neq.cancelled",
        "order": "order_date.desc",
    })
    if not orders:
        print("  No orders found for aggregation")
        return 0

    # Exclude FBA inbound/MCF shipments (S02-prefix orders and Non-Amazon channel)
    # NOTE: Stale rows from before this filter was added may need manual cleanup
    # via DELETE from daily_metrics where sku matches S02-related SKUs with 0 revenue.
    # The etl/amazon.py ingestion filter now prevents new S02 rows from being created.
    before_filter = len(orders)
    orders = [
        o for o in orders
        if not (o.get("external_id") or "").startswith("S02")
    ]
    excluded = before_filter - len(orders)
    if excluded:
        print(f"  Excluded {excluded} FBA inbound/MCF shipments (S02-prefix)")

    print(f"  Found {len(orders)} orders in period")

    order_ids = [o["id"] for o in orders]
    order_map = {o["id"]: o for o in orders}

    # Build per-order fee map (real fees from Finances API)
    order_fee_map = {}
    order_shipping_map = {}
    seller_shipping_count = 0
    for o in orders:
        fee = float(o.get("platform_fee", 0) or 0)
        order_fee_map[o["id"]] = fee
        # Prefer seller_shipping_cost_pln (actual DPD cost) over shipping_cost (buyer's delivery_price)
        # For P&L: shipping_cost column = what buyer paid (revenue, already in total_paid)
        # seller_shipping_cost = actual courier cost to seller (expense)
        seller_cost_pln = float(o.get("seller_shipping_cost_pln", 0) or 0)
        if seller_cost_pln > 0:
            # Use seller cost directly in PLN (already converted by shipping_costs module)
            order_shipping_map[o["id"]] = seller_cost_pln
            seller_shipping_count += 1
        else:
            # Fallback: use buyer's delivery_price as rough proxy for FBM shipping cost
            order_shipping_map[o["id"]] = float(o.get("shipping_cost", 0) or 0)

    # Count how many have real fees vs zero
    real_fees = sum(1 for f in order_fee_map.values() if f > 0)
    print(f"  Orders with real fees: {real_fees}/{len(orders)}")
    print(f"  Orders with seller shipping cost: {seller_shipping_count}/{len(orders)}")

    # Get all order items for these orders (batch by 100 IDs)
    all_items = []
    for i in range(0, len(order_ids), 100):
        batch = order_ids[i:i+100]
        ids_str = ",".join(str(x) for x in batch)
        items = _get_all("order_items", {
            "select": "order_id,sku,quantity,unit_price,currency",
            "order_id": f"in.({ids_str})",
        })
        all_items.extend(items)

    print(f"  Found {len(all_items)} order items")

    # Get product costs (paginated)
    products = _get_all("products", {"select": "sku,cost_pln"})
    cost_map = {p["sku"]: float(p["cost_pln"] or 0) for p in products}

    # Build SKU normalization cache for all order-item SKUs
    # IMPORTANT: use the same key transformation as the agg dict (lstrip zeros, rstrip dashes)
    # so that sku_norm_map.get(sku) always finds the right normalized SKU.
    raw_skus = set()
    for item in all_items:
        s = (item.get("sku") or "").strip()
        if s:
            agg_key = s.lstrip('0').rstrip('-').strip() or s
            raw_skus.add(agg_key)
    sku_norm_map = {}
    normalized_count = 0
    for raw in raw_skus:
        norm = normalize_sku(raw, cost_map)
        sku_norm_map[raw] = norm
        if norm != raw and norm != raw.strip():
            normalized_count += 1
    if normalized_count:
        print(f"  SKU normalization: {normalized_count} SKUs remapped to products with COGS")

    # Get platform fee rates (fallback for orders without real fees)
    platforms = db._get("platforms", {"select": "id,fee_pct"})
    fee_pct_map = {p["id"]: float(p["fee_pct"] or 0) for p in platforms}

    # Load returns data to deduct from P&L
    returns = _get_all("amazon_returns", {
        "select": "return_date,sku,quantity",
        "return_date": f"gte.{cutoff}",
    })
    # Group returns by (date, sku)
    returns_map = defaultdict(int)
    for r in returns:
        rd = (r.get("return_date", "")[:10], r.get("sku", ""))
        returns_map[rd] += int(r.get("quantity", 1) or 1)
    if returns:
        print(f"  Found {len(returns)} returns ({sum(returns_map.values())} units)")

    # Build set of FBM order IDs (Baselinker-sourced, numeric external_id = FBM)
    fbm_order_ids = set()
    fba_order_ids = set()
    for o in orders:
        eid = o.get("external_id", "")
        if o.get("platform_id", 0) in AMAZON_PLATFORM_IDS:
            if eid.isdigit():
                fbm_order_ids.add(o["id"])
            else:
                fba_order_ids.add(o["id"])
    if fbm_order_ids or fba_order_ids:
        print(f"  Amazon orders: {len(fba_order_ids)} FBA + {len(fbm_order_ids)} FBM")

    # Aggregate: group by (date, platform_id, sku)
    agg = defaultdict(lambda: {
        "orders": set(), "units": 0, "revenue": 0.0, "currency": "EUR",
        "real_fees": 0.0,  # sum of real per-order fees allocated to this group
        "shipping_pln": 0.0,   # sum of seller shipping costs in PLN (already converted)
        "shipping_orig": 0.0,  # sum of buyer delivery_price in original currency (fallback)
        "has_seller_cost": False,  # whether any order in group has seller_shipping_cost
        "fba_units": 0,    # units from FBA orders (for fee fallback selection)
        "fbm_units": 0,    # units from FBM orders (for fee fallback selection)
    })

    # First pass: count items per order (for fee allocation)
    # Only count items with a valid unit_price to avoid fee-without-revenue artifacts.
    items_per_order = defaultdict(int)
    for item in all_items:
        if item.get("unit_price") is not None and float(item["unit_price"] or 0) > 0:
            items_per_order[item["order_id"]] += int(item["quantity"] or 1)

    for item in all_items:
        order = order_map.get(item["order_id"])
        if not order:
            continue
        day = order["order_date"][:10]  # YYYY-MM-DD
        plat_id = order["platform_id"]
        raw_sku = (item.get("sku") or "").strip()
        if not raw_sku:
            continue  # skip items with no SKU entirely

        # Skip items with no price: including their fees would create fee-without-revenue rows.
        # These occur when unit_price_pln is NULL in order_items (FX rate missing at import time).
        if item.get("unit_price") is None or float(item.get("unit_price") or 0) == 0:
            continue

        # Normalize SKU: strip leading zeros, trailing dashes/special chars
        sku = raw_sku.lstrip('0').rstrip('-').strip()
        if not sku:
            sku = raw_sku  # fallback if stripping removed everything (e.g. "000")
        key = (day, plat_id, sku)
        qty = int(item["quantity"] or 1)

        agg[key]["orders"].add(item["order_id"])
        agg[key]["units"] += qty
        agg[key]["revenue"] += float(item["unit_price"]) * qty
        agg[key]["currency"] = item.get("currency") or order.get("currency", "EUR")

        # Track FBA vs FBM units for fee fallback
        if item["order_id"] in fba_order_ids:
            agg[key]["fba_units"] += qty
        elif item["order_id"] in fbm_order_ids:
            agg[key]["fbm_units"] += qty

        # Allocate real order fee proportionally by quantity
        order_fee = order_fee_map.get(item["order_id"], 0)
        if order_fee > 0:
            total_items_in_order = items_per_order.get(item["order_id"], 1)
            fee_share = order_fee * qty / total_items_in_order
            agg[key]["real_fees"] += fee_share

        # Allocate shipping cost proportionally by quantity
        # order_shipping_map contains seller_shipping_cost_pln (already in PLN) if available,
        # otherwise buyer's delivery_price (in original currency) as fallback
        order_shipping = order_shipping_map.get(item["order_id"], 0)
        seller_cost_pln = float(order_map[item["order_id"]].get("seller_shipping_cost_pln", 0) or 0)
        if order_shipping > 0:
            total_items_in_order = items_per_order.get(item["order_id"], 1)
            ship_share = order_shipping * qty / total_items_in_order
            if seller_cost_pln > 0:
                agg[key]["shipping_pln"] += ship_share
                agg[key]["has_seller_cost"] = True
            else:
                agg[key]["shipping_orig"] += ship_share

    # Build metrics
    metrics = []
    for (day, plat_id, sku), data in agg.items():
        currency = data["currency"]
        revenue = data["revenue"]

        # FX conversion
        fx = fx_rates.convert_to_pln(conn, 1.0, currency, day)
        rate = fx if fx else 1.0
        revenue_pln = round(revenue * rate, 2) if currency != "PLN" else revenue

        # Fees: use real fees if available, else use FBA/FBM-aware fallback
        if data["real_fees"] > 0:
            # Real fees are in original currency — convert to PLN
            fees = round(data["real_fees"] * rate, 2) if currency != "PLN" else round(data["real_fees"], 2)
        elif plat_id in AMAZON_PLATFORM_IDS:
            # Determine blended fee rate based on FBA vs FBM mix
            total_amz_units = data["fba_units"] + data["fbm_units"]
            if total_amz_units > 0:
                fba_ratio = data["fba_units"] / total_amz_units
                fba_pct = AMAZON_FBA_FEE_PCT.get(currency, AMAZON_FBA_DEFAULT_FEE_PCT)
                blended_pct = fba_ratio * fba_pct + (1 - fba_ratio) * AMAZON_FBM_FEE_PCT
            else:
                # Unknown fulfillment type, use FBA rate as conservative estimate
                blended_pct = AMAZON_FBA_FEE_PCT.get(currency, AMAZON_FBA_DEFAULT_FEE_PCT)
            fees = round(revenue_pln * blended_pct / 100, 2)
        else:
            # Non-Amazon platforms: use configured flat rate
            fees = round(revenue_pln * fee_pct_map.get(plat_id, 0) / 100, 2)

        # Deduct returns (reduce units & revenue)
        returned_units = returns_map.get((day, sku), 0)
        net_units = max(data["units"] - returned_units, 0)

        # Costs (use normalized SKU for COGS lookup)
        norm_sku = sku_norm_map.get(sku, sku)
        cost_per_unit = cost_map.get(norm_sku, 0)
        cogs = round(cost_per_unit * net_units, 2)
        # Revenue adjusted for returns (proportional reduction)
        if returned_units > 0 and data["units"] > 0:
            return_ratio = returned_units / data["units"]
            revenue_pln = round(revenue_pln * (1 - return_ratio), 2)
            revenue = round(revenue * (1 - return_ratio), 2)
        # Shipping costs:
        # shipping_pln = seller's actual DPD cost (already in PLN from shipping_costs module)
        # shipping_orig = buyer's delivery_price fallback (needs FX conversion)
        shipping = round(data["shipping_pln"], 2)
        if data["shipping_orig"] > 0:
            fallback_ship = round(data["shipping_orig"] * rate, 2) if currency != "PLN" else round(data["shipping_orig"], 2)
            shipping += fallback_ship
        gross_profit = round(revenue_pln - cogs - fees - shipping, 2)
        margin = round(gross_profit / revenue_pln * 100, 1) if revenue_pln > 0 else 0
        # Clamp to DB precision NUMERIC(5,2) — max ±999.99
        margin = max(-999.9, min(999.9, margin))

        metrics.append({
            "date": day,
            "platform_id": plat_id,
            "sku": sku,
            "orders_count": len(data["orders"]),
            "units_sold": net_units,
            "revenue": min(revenue, 99_999_999.99),
            "revenue_pln": min(revenue_pln, 99_999_999.99),
            "cogs": min(cogs, 99_999_999.99),
            "platform_fees": min(fees, 99_999_999.99),
            "shipping_cost": min(shipping, 99_999_999.99),
            "gross_profit": max(-99_999_999.99, min(gross_profit, 99_999_999.99)),
            "margin_pct": margin,
        })

    count = db.upsert_daily_metrics(conn, metrics)
    print(f"  Aggregated {count} daily metrics from {len(all_items)} items")
    return count
