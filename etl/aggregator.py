"""Aggregator: compute daily metrics from raw orders via REST API."""
from datetime import date, timedelta
from collections import defaultdict
from . import db, fx_rates


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
    orders = _get_all("orders", {
        "select": "id,platform_id,order_date,currency,status,platform_fee,total_paid",
        "order_date": f"gte.{cutoff}",
        "status": "neq.cancelled",
        "order": "order_date.desc",
    })
    if not orders:
        print("  No orders found for aggregation")
        return 0

    print(f"  Found {len(orders)} orders in period")

    order_ids = [o["id"] for o in orders]
    order_map = {o["id"]: o for o in orders}

    # Build per-order fee map (real fees from Finances API)
    order_fee_map = {}
    for o in orders:
        fee = float(o.get("platform_fee", 0) or 0)
        order_fee_map[o["id"]] = fee

    # Count how many have real fees vs zero
    real_fees = sum(1 for f in order_fee_map.values() if f > 0)
    print(f"  Orders with real fees: {real_fees}/{len(orders)}")

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

    # Aggregate: group by (date, platform_id, sku)
    agg = defaultdict(lambda: {
        "orders": set(), "units": 0, "revenue": 0.0, "currency": "EUR",
        "real_fees": 0.0,  # sum of real per-order fees allocated to this group
    })

    # First pass: count items per order (for fee allocation)
    items_per_order = defaultdict(int)
    for item in all_items:
        items_per_order[item["order_id"]] += int(item["quantity"] or 1)

    for item in all_items:
        order = order_map.get(item["order_id"])
        if not order:
            continue
        day = order["order_date"][:10]  # YYYY-MM-DD
        plat_id = order["platform_id"]
        sku = item.get("sku") or "unknown"
        key = (day, plat_id, sku)
        qty = int(item["quantity"] or 1)

        agg[key]["orders"].add(item["order_id"])
        agg[key]["units"] += qty
        agg[key]["revenue"] += float(item["unit_price"] or 0) * qty
        agg[key]["currency"] = item.get("currency") or order.get("currency", "EUR")

        # Allocate real order fee proportionally by quantity
        order_fee = order_fee_map.get(item["order_id"], 0)
        if order_fee > 0:
            total_items_in_order = items_per_order.get(item["order_id"], 1)
            fee_share = order_fee * qty / total_items_in_order
            agg[key]["real_fees"] += fee_share

    # Build metrics
    metrics = []
    for (day, plat_id, sku), data in agg.items():
        currency = data["currency"]
        revenue = data["revenue"]

        # FX conversion
        fx = fx_rates.convert_to_pln(conn, 1.0, currency, day)
        rate = fx if fx else 1.0
        revenue_pln = round(revenue * rate, 2) if currency != "PLN" else revenue

        # Fees: use real fees if available, else fallback to flat %
        if data["real_fees"] > 0:
            # Real fees are in original currency — convert to PLN
            fees = round(data["real_fees"] * rate, 2) if currency != "PLN" else round(data["real_fees"], 2)
        else:
            fees = round(revenue_pln * fee_pct_map.get(plat_id, 0) / 100, 2)

        # Deduct returns (reduce units & revenue)
        returned_units = returns_map.get((day, sku), 0)
        net_units = max(data["units"] - returned_units, 0)

        # Costs
        cost_per_unit = cost_map.get(sku, 0)
        cogs = round(cost_per_unit * net_units, 2)
        # Revenue adjusted for returns (proportional reduction)
        if returned_units > 0 and data["units"] > 0:
            return_ratio = returned_units / data["units"]
            revenue_pln = round(revenue_pln * (1 - return_ratio), 2)
            revenue = round(revenue * (1 - return_ratio), 2)
        shipping = 0
        gross_profit = round(revenue_pln - cogs - fees - shipping, 2)
        margin = round(gross_profit / revenue_pln * 100, 1) if revenue_pln > 0 else 0

        metrics.append({
            "date": day,
            "platform_id": plat_id,
            "sku": sku,
            "orders_count": len(data["orders"]),
            "units_sold": net_units,
            "revenue": revenue,
            "revenue_pln": revenue_pln,
            "cogs": cogs,
            "platform_fees": fees,
            "shipping_cost": shipping,
            "gross_profit": gross_profit,
            "margin_pct": margin,
        })

    count = db.upsert_daily_metrics(conn, metrics)
    print(f"  Aggregated {count} daily metrics from {len(all_items)} items")
    return count
