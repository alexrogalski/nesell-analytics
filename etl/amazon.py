"""Amazon SP-API ETL: pull orders and enrich with fees."""
import requests, time, json
from datetime import datetime, timedelta
from urllib.parse import quote
from . import config, db


def _headers():
    token = config.get_amazon_token()
    return {
        "x-amz-access-token": token,
        "Content-Type": "application/json",
    }


def _get(path, params=None):
    """GET request to Amazon SP-API with retry."""
    url = f"{config.AMZ_API_BASE}{path}"
    for attempt in range(3):
        resp = requests.get(url, headers=_headers(), params=params)
        if resp.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        return resp.json()
    return resp.json()


def sync_orders(conn, days_back: int = 90):
    """Pull Amazon orders via SP-API Orders API."""
    platform_map = db.get_platform_map(conn)
    after = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")

    marketplace_ids = ",".join(config.MARKETPLACE_TO_PLATFORM.keys())
    all_orders = []
    next_token = None

    while True:
        params = {
            "MarketplaceIds": marketplace_ids,
            "CreatedAfter": after,
            "MaxResultsPerPage": 100,
        }
        if next_token:
            params = {"MarketplaceIds": marketplace_ids, "NextToken": next_token}

        data = _get("/orders/v0/orders", params)
        payload = data.get("payload", {})
        orders = payload.get("Orders", [])
        all_orders.extend(orders)

        next_token = payload.get("NextToken")
        print(f"  Amazon orders batch: {len(orders)} (total {len(all_orders)})")
        if not next_token:
            break
        time.sleep(1)

    # Transform
    db_orders = []
    for o in all_orders:
        mkt_id = o.get("MarketplaceId", "")
        plat_code = config.MARKETPLACE_TO_PLATFORM.get(mkt_id, "amazon_de")
        plat_id = platform_map.get(plat_code)
        if not plat_id:
            continue

        status = o.get("OrderStatus", "").lower()
        if status == "canceled":
            status = "cancelled"
        elif status == "shipped":
            status = "shipped"
        elif status in ("pending", "unshipped"):
            status = "pending"

        currency = o.get("OrderTotal", {}).get("CurrencyCode", "EUR")
        total = float(o.get("OrderTotal", {}).get("Amount", 0))

        db_orders.append({
            "external_id": o["AmazonOrderId"],
            "platform_id": plat_id,
            "platform_order_id": o["AmazonOrderId"],
            "order_date": o.get("PurchaseDate", ""),
            "status": status,
            "buyer_email": o.get("BuyerInfo", {}).get("BuyerEmail", ""),
            "shipping_country": o.get("ShippingAddress", {}).get("CountryCode", ""),
            "shipping_cost": 0,
            "total_paid": total,
            "currency": currency,
            "total_paid_pln": None,
            "platform_fee": 0,
            "platform_fee_pln": 0,
            "raw_data": o,
        })

    count = db.upsert_orders(conn, db_orders)
    print(f"  Upserted {count} Amazon orders")

    # Fetch order items for each order
    _sync_order_items(conn, all_orders, platform_map)
    return count


def _sync_order_items(conn, raw_orders, platform_map):
    """Fetch order items from SP-API and upsert."""
    total_items = 0
    hdrs = _headers()

    for o in raw_orders:
        order_id_amz = o["AmazonOrderId"]
        mkt_id = o.get("MarketplaceId", "")
        plat_code = config.MARKETPLACE_TO_PLATFORM.get(mkt_id, "amazon_de")
        plat_id = platform_map.get(plat_code)
        if not plat_id:
            continue

        internal_order_id = db.get_order_id_by_external(conn, order_id_amz, plat_id)
        if not internal_order_id:
            continue

        # Fetch items
        data = _get(f"/orders/v0/orders/{order_id_amz}/orderItems")
        items_data = data.get("payload", {}).get("OrderItems", [])

        items = []
        for it in items_data:
            currency = it.get("ItemPrice", {}).get("CurrencyCode", "EUR")
            price = float(it.get("ItemPrice", {}).get("Amount", 0))
            qty = int(it.get("QuantityOrdered", 1))
            unit_price = round(price / qty, 2) if qty > 0 else price

            items.append({
                "order_id": internal_order_id,
                "sku": it.get("SellerSKU", ""),
                "name": it.get("Title", ""),
                "quantity": qty,
                "unit_price": unit_price,
                "currency": currency,
                "unit_price_pln": None,
                "unit_cost": None,
                "unit_cost_pln": None,
                "asin": it.get("ASIN", ""),
            })

        if items:
            db.upsert_order_items(conn, items)
            total_items += len(items)

        time.sleep(0.5)  # rate limit

    print(f"  Upserted {total_items} Amazon order items")
    return total_items
