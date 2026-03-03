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
    """GET request to Amazon SP-API with retry and generous backoff."""
    url = f"{config.AMZ_API_BASE}{path}"
    for attempt in range(8):
        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"    [ConnectionError] retrying in {wait}s (attempt {attempt+1}/8)")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            # Aggressive backoff: 5, 10, 20, 40, 60, 60, 60, 60
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [429] rate limited, waiting {wait}s (attempt {attempt+1}/8)...")
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            # Token expired — refresh and retry
            print(f"    [403] token may be expired, refreshing (attempt {attempt+1}/8)...")
            time.sleep(3)
            continue
        if resp.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [{resp.status_code}] server error, retrying in {wait}s (attempt {attempt+1}/8)")
            time.sleep(wait)
            continue
        return resp.json()
    print(f"    [WARN] All 8 attempts failed for {path}")
    return {}


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
            "FulfillmentChannels": "AFN",  # FBA only — FBM already in Baselinker
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
        time.sleep(2)  # respect rate limits on getOrders

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
    """Fetch order items from SP-API — skip orders that already have items."""
    total_items = 0
    skipped = 0

    for idx, o in enumerate(raw_orders):
        order_id_amz = o["AmazonOrderId"]
        mkt_id = o.get("MarketplaceId", "")
        plat_code = config.MARKETPLACE_TO_PLATFORM.get(mkt_id, "amazon_de")
        plat_id = platform_map.get(plat_code)
        if not plat_id:
            continue

        internal_order_id = db.get_order_id_by_external(conn, order_id_amz, plat_id)
        if not internal_order_id:
            continue

        # Check if items already exist for this order — avoid duplicates
        existing_count = db.count_order_items(conn, internal_order_id)
        if existing_count > 0:
            skipped += 1
            continue

        # Fetch items from Amazon API
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

        # Progress every 50 orders
        if (idx + 1) % 50 == 0:
            print(f"    Items progress: {idx+1}/{len(raw_orders)} orders processed, {total_items} items inserted, {skipped} skipped (already had items)")

        time.sleep(2)  # 2s between getOrderItems calls to respect rate limits

    print(f"  Inserted {total_items} Amazon order items ({skipped} orders skipped — already had items)")
    return total_items
