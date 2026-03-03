"""Aggregator: compute daily metrics from raw orders via REST API."""
from datetime import date, timedelta
from collections import defaultdict
from . import db, fx_rates


def aggregate_daily(conn, days_back: int = 90):
    """Compute daily_metrics from orders + order_items + product costs."""
    cutoff = str(date.today() - timedelta(days=days_back))

    # Get all non-cancelled orders in period
    orders = db._get("orders", {
        "select": "id,platform_id,order_date,currency,status",
        "order_date": f"gte.{cutoff}",
        "status": "neq.cancelled",
        "order": "order_date.desc",
    })
    if not orders:
        print("  No orders found for aggregation")
        return 0

    order_ids = [o["id"] for o in orders]
    order_map = {o["id"]: o for o in orders}

    # Get all order items for these orders (batch)
    all_items = []
    for i in range(0, len(order_ids), 100):
        batch = order_ids[i:i+100]
        ids_str = ",".join(str(x) for x in batch)
        items = db._get("order_items", {
            "select": "order_id,sku,quantity,unit_price,currency",
            "order_id": f"in.({ids_str})",
        })
        all_items.extend(items)

    # Get product costs
    products = db._get("products", {"select": "sku,cost_pln"})
    cost_map = {p["sku"]: float(p["cost_pln"] or 0) for p in products}

    # Get platform fee rates
    platforms = db._get("platforms", {"select": "id,fee_pct"})
    fee_map = {p["id"]: float(p["fee_pct"] or 0) for p in platforms}

    # Aggregate: group by (date, platform_id, sku)
    agg = defaultdict(lambda: {
        "orders": set(), "units": 0, "revenue": 0.0, "currency": "EUR"
    })

    for item in all_items:
        order = order_map.get(item["order_id"])
        if not order:
            continue
        day = order["order_date"][:10]  # YYYY-MM-DD
        plat_id = order["platform_id"]
        sku = item.get("sku") or "unknown"
        key = (day, plat_id, sku)

        agg[key]["orders"].add(item["order_id"])
        agg[key]["units"] += int(item["quantity"] or 1)
        agg[key]["revenue"] += float(item["unit_price"] or 0) * int(item["quantity"] or 1)
        agg[key]["currency"] = item.get("currency") or order.get("currency", "EUR")

    # Build metrics
    metrics = []
    for (day, plat_id, sku), data in agg.items():
        currency = data["currency"]
        revenue = data["revenue"]

        # FX conversion
        fx = fx_rates.convert_to_pln(conn, 1.0, currency, day)
        rate = fx if fx else 1.0
        revenue_pln = round(revenue * rate, 2) if currency != "PLN" else revenue

        # Costs
        cost_per_unit = cost_map.get(sku, 0)
        cogs = round(cost_per_unit * data["units"], 2)
        fees = round(revenue_pln * fee_map.get(plat_id, 0) / 100, 2)
        shipping = 0
        gross_profit = round(revenue_pln - cogs - fees - shipping, 2)
        margin = round(gross_profit / revenue_pln * 100, 1) if revenue_pln > 0 else 0

        metrics.append({
            "date": day,
            "platform_id": plat_id,
            "sku": sku,
            "orders_count": len(data["orders"]),
            "units_sold": data["units"],
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
