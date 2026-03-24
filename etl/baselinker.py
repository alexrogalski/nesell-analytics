"""Baselinker ETL: pull orders and products into DB."""
import requests, time, json, re
from datetime import datetime, timedelta
from . import config, db


def bl_api(method: str, params: dict = None) -> dict:
    """Call Baselinker API with rate limit retry."""
    for attempt in range(5):
        try:
            resp = requests.post(config.BASELINKER_URL, data={
                "token": config.BASELINKER_TOKEN,
                "method": method,
                "parameters": json.dumps(params or {})
            }, timeout=120)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            wait = 15 * (attempt + 1)
            print(f"  [Network error] {e.__class__.__name__} on {method}, retrying in {wait}s (attempt {attempt+1}/5)...")
            time.sleep(wait)
            continue
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
    """Transform single Baselinker order to DB format.

    Extracts real marketplace commission from Baselinker's commission field
    (available when include_commission_data=True in getOrders).
    Commission structure: {"net": "12.50", "gross": "15.38", "currency": "PLN"}
    We use the GROSS value as the platform_fee (what the seller actually pays).
    """
    plat_id = _detect_platform(o, platform_map)
    if not plat_id:
        return None

    currency = o.get("currency", "PLN")
    total = float(o.get("payment_done", 0) or 0)

    # Extract real commission from marketplace (Allegro, Empik, Temu, etc.)
    # Baselinker provides marketplace referral commission via include_commission_data.
    # For Amazon FBA orders (external_id contains dashes like "303-xxx-xxx"):
    #   SKIP commission here — amazon_fees.py provides more complete data
    #   (referral + FBA + other fees) via a separate PATCH call.
    # For Amazon FBM orders (external_id is numeric Baselinker ID):
    #   WRITE commission from Baselinker — amazon_fees.py can't match these
    #   by their numeric external_id.
    platform_fee = 0.0
    src = (o.get("order_source") or "").lower()
    ext_id = str(o["order_id"])
    source_ext_id = o.get("order_source_external_id", "") or ""

    if "amazon" in src:
        # Amazon FBA: external_id will be Amazon order ID with dashes — skip fees
        # Amazon FBM: external_id is numeric Baselinker ID — write Baselinker commission
        is_amazon_format = bool(source_ext_id and "-" in source_ext_id)
        if not is_amazon_format:
            # FBM order — write Baselinker commission
            commission = o.get("commission")
            if commission and isinstance(commission, dict):
                platform_fee = float(commission.get("gross", 0) or 0)
    else:
        commission = o.get("commission")
        if commission and isinstance(commission, dict):
            platform_fee = float(commission.get("gross", 0) or 0)

    # For Amazon FBM orders, store the actual Amazon order ID in platform_order_id
    # so amazon_fees.py can potentially match them in the future
    platform_order_id = source_ext_id

    # Store raw BL status_id in notes for future analysis/debugging
    bl_status_id = o.get("order_status_id")
    notes = f"bl_status_id={bl_status_id}" if bl_status_id is not None else None

    return {
        "external_id": ext_id,
        "platform_id": plat_id,
        "platform_order_id": platform_order_id,
        "order_date": datetime.fromtimestamp(o.get("date_confirmed", 0)).isoformat(),
        "status": _map_bl_status(bl_status_id),
        "buyer_email": o.get("email", ""),
        "shipping_country": o.get("delivery_country_code", ""),
        "shipping_cost": float(o.get("delivery_price", 0) or 0),
        "total_paid": total,
        "currency": currency,
        "total_paid_pln": total if currency == "PLN" else None,
        "platform_fee": platform_fee,
        "platform_fee_pln": 0,
        "notes": notes,
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


def sync_orders(days_back: int = 90):
    """Pull orders from Baselinker for last N days. Saves in batches."""
    platform_map = db.get_platform_map()
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
            "include_commission_data": True,
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
            saved = _save_batch(batch_orders, batch_items, platform_map)
            total_saved += saved
            print(f"  [Batch] Saved {saved} orders (total saved: {total_saved}, fetched: {total_fetched})")
            batch_orders = []
            batch_items = []

        if page % 50 == 0:
            print(f"  Page {page}: {total_fetched} orders fetched, {total_saved} saved")

        time.sleep(0.3)

    # Save remaining
    if batch_orders:
        saved = _save_batch(batch_orders, batch_items, platform_map)
        total_saved += saved

    print(f"  DONE: {total_saved} orders saved from {total_fetched} fetched")
    return total_saved


def _save_batch(db_orders, raw_items, platform_map):
    """Save a batch of orders + their items."""
    # Deduplicate orders by (external_id, platform_id)
    seen = set()
    unique_orders = []
    for o in db_orders:
        key = (o["external_id"], o["platform_id"])
        if key not in seen:
            seen.add(key)
            unique_orders.append(o)
    count = db.upsert_orders(unique_orders)

    # Now save items — need to look up order IDs
    items_to_save = []
    for it in raw_items:
        if not it.get("platform_id"):
            continue
        order_id = db.get_order_id_by_external(it["bl_order_id"], it["platform_id"])
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
        db.upsert_order_items(items_to_save)

    return count


def _map_bl_status(status_id) -> str:
    """Map Baselinker status ID to our status.

    Baselinker uses custom status IDs per account (configured in panel settings).
    Common conventions: low IDs = new/processing, higher IDs = shipped/delivered/cancelled.
    We store the raw status_id in the order for analysis, and map known patterns here.

    The exact mapping depends on the Baselinker account configuration.
    Safe approach: default to "confirmed" but detect cancellation-range statuses.
    """
    if status_id is None:
        return "confirmed"
    try:
        sid = int(status_id)
    except (TypeError, ValueError):
        return "confirmed"

    # Baselinker standard status_id ranges (common across most accounts):
    # These are panel-specific, but IDs in the high range often mean cancelled/returned.
    # Known cancelled/returned status IDs from Baselinker's default templates:
    # 5 = cancelled, 6 = returned (if using default numeric IDs)
    # Some accounts use 100000+ range for custom statuses.
    # We store the raw ID and flag only clearly cancellation-like ones.
    if sid == 5:
        return "cancelled"
    if sid == 6:
        return "returned"

    return "confirmed"


def _detect_source(sku: str, inventory_id: int) -> str:
    """Detect product source from SKU pattern and inventory."""
    if sku.startswith("PFT-"):
        return "printful"
    if inventory_id == 30229:
        return "wholesale"
    return "unknown"


def _fetch_inventory_products(inventory_id: int, source_label: str) -> list:
    """Fetch all products from a single Baselinker inventory.

    Returns list of product dicts ready for DB upsert.
    """
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
        print(f"  [{source_label}] Product list page {page - 1}: {len(products)} IDs")
        time.sleep(0.3)

    print(f"  [{source_label}] Total product IDs: {len(all_ids)}")

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

            # Extract main product image (key "1" is the primary image)
            images = p.get("images", {})
            image_url = None
            if isinstance(images, dict) and images:
                image_url = images.get("1") or next(iter(images.values()), None)

            all_products.append({
                "sku": sku,
                "name": name,
                "brand": "nesell",
                "source": _detect_source(sku, inventory_id),
                "cost_pln": float(p.get("average_cost", 0) or 0) or None,
                "weight_g": int(float(p.get("weight", 0) or 0) * 1000) or None,
                "is_parent": "-" not in sku.split("PFT-")[-1] if sku.startswith("PFT-") else False,
                "parent_sku": None,
                "ean": p.get("ean", ""),
                "image_url": image_url,
                "active": True,
            })

            for vid, v in p.get("variants", {}).items():
                vsku = v.get("sku", "")
                if not vsku:
                    continue
                # Variants may have their own images; fall back to parent image
                v_images = v.get("images", {})
                v_image_url = None
                if isinstance(v_images, dict) and v_images:
                    v_image_url = v_images.get("1") or next(iter(v_images.values()), None)
                if not v_image_url:
                    v_image_url = image_url  # inherit parent image

                all_products.append({
                    "sku": vsku,
                    "name": v.get("name", ""),
                    "brand": "nesell",
                    "source": _detect_source(vsku, inventory_id),
                    "cost_pln": float(v.get("average_cost", 0) or 0) or None,
                    "weight_g": None,
                    "is_parent": False,
                    "parent_sku": sku,
                    "ean": v.get("ean", ""),
                    "image_url": v_image_url,
                    "active": True,
                })

        print(f"  [{source_label}] Batch {i//100 + 1}: {len(products)} products fetched")
        time.sleep(0.3)

    return all_products


def _enrich_names_from_orders(products: list) -> int:
    """Enrich product names that are just EAN/barcodes using order_items.

    Queries order_items for real marketplace listing titles and updates
    products where name == sku (i.e. name is just a barcode).
    Returns number of products enriched.
    """
    # Find products with barcode-like names (name equals sku, or name is all digits)
    barcode_skus = set()
    for p in products:
        name = (p.get("name") or "").strip()
        sku = p.get("sku", "")
        if not name or name == sku or (name.isdigit() and len(name) >= 8):
            barcode_skus.add(sku)

    if not barcode_skus:
        return 0

    # Query order_items for real product names
    # Fetch all order_items that have a name and match our barcode SKUs
    enriched = 0
    sku_names = {}

    # Paginated fetch of order_items with names
    offset = 0
    batch_size = 1000
    while True:
        items = db._get("order_items", {
            "select": "sku,name",
            "name": "neq.",
            "limit": str(batch_size),
            "offset": str(offset),
        })
        for item in items:
            item_sku = item.get("sku", "")
            item_name = (item.get("name") or "").strip()
            if item_sku and item_name and item_sku in barcode_skus:
                # Prefer longer names (more descriptive marketplace titles)
                existing = sku_names.get(item_sku, "")
                if len(item_name) > len(existing):
                    sku_names[item_sku] = item_name
        if len(items) < batch_size:
            break
        offset += batch_size

    # Update the products list in-place
    for p in products:
        sku = p.get("sku", "")
        if sku in sku_names:
            p["name"] = sku_names[sku]
            enriched += 1

    # Also update existing products in DB that have barcode names
    for sku, name in sku_names.items():
        try:
            db._patch("products", {"sku": f"eq.{sku}"}, {"name": name})
        except Exception:
            pass  # Skip individual update failures

    return enriched


def sync_products():
    """Pull products from both Baselinker inventories (Printful + wholesale).

    Syncs from:
    - inventory_id=52954 (Printful) first
    - inventory_id=30229 (test-exportivo / wholesale) second

    Wholesale products do not overwrite Printful products that already exist.
    After syncing, enriches barcode-only names from order_items marketplace titles.
    """
    # 1. Sync Printful inventory first (higher priority)
    printful_products = _fetch_inventory_products(52954, "Printful")
    print(f"  Printful: {len(printful_products)} products fetched")

    # 2. Sync wholesale inventory (test-exportivo)
    wholesale_products = _fetch_inventory_products(30229, "Wholesale")
    print(f"  Wholesale: {len(wholesale_products)} products fetched")

    # 3. Merge: Printful products take priority (upsert Printful first, then wholesale)
    # Track Printful SKUs so we don't overwrite them with worse wholesale data
    printful_skus = {p["sku"] for p in printful_products}

    # Filter wholesale: skip products that already exist in Printful inventory
    # (they'll be upserted from Printful with better data)
    wholesale_new = [p for p in wholesale_products if p["sku"] not in printful_skus]
    skipped = len(wholesale_products) - len(wholesale_new)
    if skipped:
        print(f"  Wholesale: {skipped} SKUs skipped (already in Printful inventory)")

    all_products = printful_products + wholesale_new
    print(f"  Total unique products to upsert: {len(all_products)}")

    # 4. Enrich barcode-only names from order_items
    enriched = _enrich_names_from_orders(all_products)
    if enriched:
        print(f"  Enriched {enriched} product names from order_items (replaced barcodes)")

    # 5. Upsert all products
    count = db.upsert_products(all_products)
    print(f"  Upserted {count} products")
    return count
