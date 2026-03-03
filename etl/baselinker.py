"""Baselinker ETL: pull orders and products into DB."""
import requests, time, json, re
from datetime import datetime, timedelta
from . import config, db


def bl_api(method: str, params: dict = None) -> dict:
    """Call Baselinker API with rate limit retry."""
    for attempt in range(5):
        resp = requests.post(config.BASELINKER_URL, data={
            "token": config.BASELINKER_TOKEN,
            "method": method,
            "parameters": json.dumps(params or {})
        })
        data = resp.json()
        if data.get("status") == "ERROR":
            msg = data.get("error_message", "")
            # Rate limit — extract wait time and retry
            if "limit exceeded" in msg.lower() or "blocked until" in msg.lower():
                # Parse "blocked until 2026-03-03 11:20:20"
                match = re.search(r"blocked until (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", msg)
                if match:
                    blocked_until = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
                    wait = max((blocked_until - datetime.now()).total_seconds() + 5, 30)
                else:
                    wait = 60 * (attempt + 1)
                print(f"  [Rate limit] Waiting {wait:.0f}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
                continue
            raise Exception(f"Baselinker {method}: {msg}")
        return data
    raise Exception(f"Baselinker {method}: rate limit exceeded after 5 retries")


def _detect_platform(order: dict, platform_map: dict) -> int | None:
    """Detect platform_id from Baselinker order source."""
    src = (order.get("order_source") or "").lower()

    # Amazon — detect by marketplace from extra fields
    if "amazon" in src:
        extra = order.get("extra_field_1", "") or ""
        for mkt_id, plat_code in config.MARKETPLACE_TO_PLATFORM.items():
            if mkt_id in extra:
                return platform_map.get(plat_code)
        return platform_map.get("amazon_de")

    if "allegro" in src:
        return platform_map.get("allegro")
    if "temu" in src:
        return platform_map.get("temu")
    if "empik" in src:
        return platform_map.get("empik")

    return platform_map.get("baselinker_other")


def _transform_order(o: dict, platform_map: dict) -> dict | None:
    """Transform single Baselinker order to DB format."""
    plat_id = _detect_platform(o, platform_map)
    if not plat_id:
        return None

    currency = o.get("currency", "PLN")
    total = float(o.get("payment_done", 0) or 0)

    return {
        "external_id": str(o["order_id"]),
        "platform_id": plat_id,
        "platform_order_id": o.get("order_source_external_id", ""),
        "order_date": datetime.fromtimestamp(o.get("date_confirmed", 0)).isoformat(),
        "status": _map_bl_status(o.get("order_status_id")),
        "buyer_email": o.get("email", ""),
        "shipping_country": o.get("delivery_country_code", ""),
        "shipping_cost": float(o.get("delivery_price", 0) or 0),
        "total_paid": total,
        "currency": currency,
        "total_paid_pln": total if currency == "PLN" else None,
        "platform_fee": 0,
        "platform_fee_pln": 0,
        "raw_data": None,  # skip raw_data to save space
    }


def _extract_items(o: dict, platform_map: dict) -> list:
    """Extract order items from a raw Baselinker order."""
    items = []
    for prod in o.get("products", []):
        items.append({
            "bl_order_id": str(o["order_id"]),
            "platform_id": _detect_platform(o, platform_map),
            "sku": prod.get("sku", ""),
            "name": prod.get("name", ""),
            "quantity": int(prod.get("quantity", 1)),
            "unit_price": float(prod.get("price_brutto", 0)),
            "currency": o.get("currency", "PLN"),
        })
    return items


BATCH_SIZE = 5000


def sync_orders(conn, days_back: int = 90):
    """Pull orders from Baselinker for last N days. Saves in batches."""
    platform_map = db.get_platform_map(conn)
    since = int((datetime.now() - timedelta(days=days_back)).timestamp())

    total_fetched = 0
    total_saved = 0
    batch_orders = []
    batch_items = []
    page = 0
    cursor_date = since  # pagination: date_confirmed_from

    while True:
        params = {
            "date_confirmed_from": cursor_date,
            "get_unconfirmed_orders": False,
            "include_custom_extra_fields": False,
        }

        data = bl_api("getOrders", params)
        orders = data.get("orders", [])
        if not orders:
            break

        # Pagination: advance cursor to last order's date_confirmed + 1
        last_date = max(o.get("date_confirmed", 0) for o in orders)
        if last_date <= cursor_date and len(orders) >= 100:
            # Same timestamp — use id_from to break ties
            last_id = max(o.get("order_id", 0) for o in orders)
            cursor_date = last_date
            # Re-fetch with id_from to skip already-seen orders
            params2 = {**params, "id_from": last_id}
            data2 = bl_api("getOrders", params2)
            extra = data2.get("orders", [])
            if extra and extra[0]["order_id"] != orders[0]["order_id"]:
                orders = extra
                last_date = max(o.get("date_confirmed", 0) for o in orders)
            else:
                cursor_date = last_date + 1
        else:
            cursor_date = last_date + 1

        page += 1
        total_fetched += len(orders)

        # Transform
        for o in orders:
            db_order = _transform_order(o, platform_map)
            if db_order:
                batch_orders.append(db_order)
                batch_items.extend(_extract_items(o, platform_map))

        # Save batch
        if len(batch_orders) >= BATCH_SIZE:
            saved = _save_batch(conn, batch_orders, batch_items, platform_map)
            total_saved += saved
            print(f"  [Batch] Saved {saved} orders (total saved: {total_saved}, fetched: {total_fetched})")
            batch_orders = []
            batch_items = []

        if page % 50 == 0:
            print(f"  Page {page}: {total_fetched} orders fetched, {total_saved} saved")

        time.sleep(0.3)

    # Save remaining
    if batch_orders:
        saved = _save_batch(conn, batch_orders, batch_items, platform_map)
        total_saved += saved

    print(f"  DONE: {total_saved} orders saved from {total_fetched} fetched")
    return total_saved


def _save_batch(conn, db_orders, raw_items, platform_map):
    """Save a batch of orders + their items."""
    # Deduplicate orders by (external_id, platform_id)
    seen = set()
    unique_orders = []
    for o in db_orders:
        key = (o["external_id"], o["platform_id"])
        if key not in seen:
            seen.add(key)
            unique_orders.append(o)
    count = db.upsert_orders(conn, unique_orders)

    # Now save items — need to look up order IDs
    items_to_save = []
    for it in raw_items:
        if not it.get("platform_id"):
            continue
        order_id = db.get_order_id_by_external(conn, it["bl_order_id"], it["platform_id"])
        if order_id:
            items_to_save.append({
                "order_id": order_id,
                "sku": it["sku"],
                "name": it["name"],
                "quantity": it["quantity"],
                "unit_price": it["unit_price"],
                "currency": it["currency"],
                "unit_price_pln": None,
                "unit_cost": None,
                "unit_cost_pln": None,
                "asin": None,
            })

    if items_to_save:
        db.upsert_order_items(conn, items_to_save)

    return count


def _map_bl_status(status_id) -> str:
    """Map Baselinker status ID to our status."""
    return "confirmed"


def sync_products(conn, inventory_id: int = 52954):
    """Pull products from Baselinker inventory."""
    all_ids = []
    page = 1
    while True:
        data = bl_api("getInventoryProductsList", {
            "inventory_id": inventory_id,
            "page": page,
        })
        products = data.get("products", {})
        if not products:
            break
        all_ids.extend(products.keys())
        page += 1
        print(f"  Product list page {page - 1}: {len(products)} IDs")
        time.sleep(0.3)

    print(f"  Total product IDs: {len(all_ids)}")

    all_products = []
    for i in range(0, len(all_ids), 100):
        batch_ids = all_ids[i:i+100]
        data = bl_api("getInventoryProductsData", {
            "inventory_id": inventory_id,
            "products": batch_ids,
        })
        products = data.get("products", {})

        for pid, p in products.items():
            sku = p.get("sku", "")
            if not sku:
                continue
            name = ""
            tf = p.get("text_fields", {})
            if isinstance(tf, dict):
                name = tf.get("name", "") or tf.get("name|pl", "")

            all_products.append({
                "sku": sku,
                "name": name,
                "brand": "nesell",
                "source": "printful" if sku.startswith("PFT-") else "unknown",
                "cost_pln": float(p.get("average_cost", 0) or 0) or None,
                "weight_g": int(float(p.get("weight", 0) or 0) * 1000) or None,
                "is_parent": "-" not in sku.split("PFT-")[-1] if sku.startswith("PFT-") else False,
                "parent_sku": None,
                "ean": p.get("ean", ""),
                "active": True,
            })

            for vid, v in p.get("variants", {}).items():
                vsku = v.get("sku", "")
                if not vsku:
                    continue
                all_products.append({
                    "sku": vsku,
                    "name": v.get("name", ""),
                    "brand": "nesell",
                    "source": "printful" if vsku.startswith("PFT-") else "unknown",
                    "cost_pln": float(v.get("average_cost", 0) or 0) or None,
                    "weight_g": None,
                    "is_parent": False,
                    "parent_sku": sku,
                    "ean": v.get("ean", ""),
                    "active": True,
                })

        print(f"  Batch {i//100 + 1}: {len(products)} products fetched")
        time.sleep(0.3)

    count = db.upsert_products(conn, all_products)
    print(f"  Upserted {count} products")
    return count
