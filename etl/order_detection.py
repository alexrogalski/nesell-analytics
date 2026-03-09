"""Detect new Amazon orders in Baselinker that need Printful fulfillment.

Scans Baselinker orders from Amazon source, filters for PFT-* SKU items,
and provides helpers to extract shipping addresses in Printful format
and mark orders as processing.
"""
import re
import time
import json
import logging
import requests
from datetime import datetime

from . import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Baselinker API helper (standalone — no DB dependency)
# ---------------------------------------------------------------------------

def _bl_api(token: str, method: str, params: dict | None = None) -> dict:
    """Call Baselinker API with rate-limit retry.

    Mirrors the pattern from baselinker.py but accepts token explicitly
    so the module can be used independently of the ETL pipeline.
    """
    for attempt in range(5):
        resp = requests.post(config.BASELINKER_URL, data={
            "token": token,
            "method": method,
            "parameters": json.dumps(params or {}),
        })
        data = resp.json()

        if data.get("status") == "ERROR":
            msg = data.get("error_message", "")
            if "limit exceeded" in msg.lower() or "blocked until" in msg.lower():
                match = re.search(
                    r"blocked until (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", msg,
                )
                if match:
                    blocked_until = datetime.strptime(
                        match.group(1), "%Y-%m-%d %H:%M:%S",
                    )
                    wait = max(
                        (blocked_until - datetime.now()).total_seconds() + 5, 30,
                    )
                else:
                    wait = 60 * (attempt + 1)
                logger.warning(
                    "Rate limit hit — waiting %.0fs (attempt %d/5)", wait, attempt + 1,
                )
                time.sleep(wait)
                continue
            raise RuntimeError(f"Baselinker {method}: {msg}")
        return data

    raise RuntimeError(f"Baselinker {method}: rate limit exceeded after 5 retries")


# ---------------------------------------------------------------------------
# 1. Fetch new Amazon orders containing Printful items
# ---------------------------------------------------------------------------

def get_new_amazon_orders(
    token: str,
    since_timestamp: int,
    status_id: int | None = None,
) -> list[dict]:
    """Fetch Amazon-source orders from Baselinker that contain PFT-* SKUs.

    Args:
        token: Baselinker API token.
        since_timestamp: Unix timestamp — only orders confirmed after this date.
        status_id: Optional Baselinker status ID to filter by (e.g. "new" status).
                   If None, all statuses are returned and caller must filter.

    Returns:
        List of raw Baselinker order dicts, each guaranteed to contain at least
        one product whose SKU starts with ``PFT-``.
    """
    all_orders: list[dict] = []
    cursor_date = since_timestamp

    while True:
        params: dict = {
            "date_confirmed_from": cursor_date,
            "get_unconfirmed_orders": False,
            "include_custom_extra_fields": False,
        }
        if status_id is not None:
            params["status_id"] = status_id

        data = _bl_api(token, "getOrders", params)
        orders = data.get("orders", [])
        if not orders:
            break

        for order in orders:
            source = (order.get("order_source") or "").lower()
            if "amazon" not in source:
                continue

            # Check if any product has a PFT-* SKU
            has_printful = any(
                (p.get("sku") or "").upper().startswith("PFT-")
                for p in order.get("products", [])
            )
            if has_printful:
                all_orders.append(order)

        # Pagination: advance cursor past last order's date_confirmed
        last_date = max(o.get("date_confirmed", 0) for o in orders)
        if last_date <= cursor_date and len(orders) >= 100:
            # Tie-break on order_id when timestamps collide
            last_id = max(o.get("order_id", 0) for o in orders)
            params2 = {**params, "id_from": last_id}
            data2 = _bl_api(token, "getOrders", params2)
            extra = data2.get("orders", [])
            if extra and extra[0]["order_id"] != orders[0]["order_id"]:
                last_date = max(o.get("date_confirmed", 0) for o in extra)
                cursor_date = last_date + 1
                # Process extra page too
                for order in extra:
                    source = (order.get("order_source") or "").lower()
                    if "amazon" not in source:
                        continue
                    has_printful = any(
                        (p.get("sku") or "").upper().startswith("PFT-")
                        for p in order.get("products", [])
                    )
                    if has_printful:
                        all_orders.append(order)
            else:
                cursor_date = last_date + 1
        else:
            cursor_date = last_date + 1

        time.sleep(0.3)

    logger.info(
        "Fetched %d Amazon orders with PFT-* items (since %s)",
        len(all_orders),
        datetime.fromtimestamp(since_timestamp).isoformat(),
    )
    return all_orders


# ---------------------------------------------------------------------------
# 2. Filter Printful items from an order
# ---------------------------------------------------------------------------

def filter_printful_items(order: dict) -> list[dict]:
    """Extract only the PFT-* SKU line items from a Baselinker order.

    Args:
        order: Raw Baselinker order dict (as returned by getOrders).

    Returns:
        List of product dicts (Baselinker format) whose SKU starts with PFT-.
        Each dict contains: sku, name, quantity, price_brutto, variant_id,
        product_id, attributes, ean, weight.
    """
    items: list[dict] = []
    for product in order.get("products", []):
        sku = (product.get("sku") or "").upper()
        if not sku.startswith("PFT-"):
            continue
        items.append({
            "order_product_id": product.get("order_product_id"),
            "product_id": product.get("product_id"),
            "variant_id": product.get("variant_id"),
            "sku": product.get("sku", ""),
            "name": product.get("name", ""),
            "quantity": int(product.get("quantity", 1)),
            "price_brutto": float(product.get("price_brutto", 0)),
            "attributes": product.get("attributes", ""),
            "ean": product.get("ean", ""),
            "weight": float(product.get("weight", 0) or 0),
        })
    return items


# ---------------------------------------------------------------------------
# 3. Extract shipping address in Printful format
# ---------------------------------------------------------------------------

_COUNTRY_STATE_MAP: dict[str, str] = {
    # Common EU countries without states — Printful accepts empty state_code
}


def extract_shipping_address(order: dict) -> dict:
    """Parse Baselinker delivery address into Printful recipient format.

    Printful expects:
        name, address1, address2, city, state_code, country_code, zip,
        phone, email

    Baselinker provides:
        delivery_fullname, delivery_company, delivery_address,
        delivery_city, delivery_state, delivery_country_code,
        delivery_postcode, phone, email

    Args:
        order: Raw Baselinker order dict.

    Returns:
        Dict ready to be used as Printful ``recipient`` object.
    """
    fullname = (order.get("delivery_fullname") or "").strip()
    company = (order.get("delivery_company") or "").strip()

    # Printful wants "name" — use fullname, append company if present
    name = fullname
    if company and company.lower() != fullname.lower():
        name = f"{fullname} ({company})" if fullname else company

    address_raw = (order.get("delivery_address") or "").strip()
    # Split multi-line address into address1 / address2
    address_lines = [ln.strip() for ln in address_raw.split("\n") if ln.strip()]
    address1 = address_lines[0] if address_lines else ""
    address2 = " ".join(address_lines[1:]) if len(address_lines) > 1 else ""

    country_code = (order.get("delivery_country_code") or "").upper()
    state_code = (order.get("delivery_state") or "").strip()

    # For US/CA Printful requires 2-letter state codes; most EU countries
    # don't use state_code — pass whatever BL provides.
    if len(state_code) > 2 and country_code in ("US", "CA"):
        # Attempt abbreviation (fallback: pass as-is)
        state_code = state_code[:2].upper()

    return {
        "name": name or "Customer",
        "company": company,
        "address1": address1,
        "address2": address2,
        "city": (order.get("delivery_city") or "").strip(),
        "state_code": state_code,
        "country_code": country_code,
        "zip": (order.get("delivery_postcode") or "").strip(),
        "phone": (order.get("phone") or "").strip(),
        "email": (order.get("email") or "").strip(),
    }


# ---------------------------------------------------------------------------
# 4. Mark order as processing in Baselinker
# ---------------------------------------------------------------------------

def mark_order_processing(token: str, order_id: int, status_id: int) -> None:
    """Update a Baselinker order's status to 'processing'.

    Args:
        token: Baselinker API token.
        order_id: Baselinker order ID.
        status_id: Target Baselinker status ID (the one representing
                   "processing" / "sent to Printful").
    """
    _bl_api(token, "setOrderStatus", {
        "order_id": order_id,
        "status_id": status_id,
    })
    logger.info("Order %d status changed to %d (processing)", order_id, status_id)


# ---------------------------------------------------------------------------
# Convenience: run detection standalone
# ---------------------------------------------------------------------------

def detect_pending_printful_orders(
    since_timestamp: int,
    status_id: int | None = None,
) -> list[dict]:
    """High-level helper using default token from config.

    Returns list of dicts with keys:
        order_id, order_source_external_id, date_confirmed, currency,
        printful_items, shipping_address
    """
    token = config.BASELINKER_TOKEN
    if not token:
        raise RuntimeError("BASELINKER_API_TOKEN not configured in ~/.keys/baselinker.env")

    raw_orders = get_new_amazon_orders(token, since_timestamp, status_id)

    results: list[dict] = []
    for order in raw_orders:
        pf_items = filter_printful_items(order)
        if not pf_items:
            continue
        results.append({
            "order_id": order["order_id"],
            "order_source_external_id": order.get("order_source_external_id", ""),
            "date_confirmed": order.get("date_confirmed", 0),
            "currency": order.get("currency", "EUR"),
            "delivery_price": float(order.get("delivery_price", 0) or 0),
            "printful_items": pf_items,
            "shipping_address": extract_shipping_address(order),
        })

    logger.info("Detected %d orders pending Printful fulfillment", len(results))
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from datetime import timedelta

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    days_back = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    since = int((datetime.now() - timedelta(days=days_back)).timestamp())

    print(f"Scanning Amazon orders from last {days_back} days for PFT-* SKUs...")
    pending = detect_pending_printful_orders(since)

    if not pending:
        print("No pending Printful orders found.")
    else:
        print(f"\nFound {len(pending)} orders needing Printful fulfillment:\n")
        for o in pending:
            addr = o["shipping_address"]
            items_str = ", ".join(
                f'{it["sku"]} x{it["quantity"]}' for it in o["printful_items"]
            )
            print(
                f"  #{o['order_id']} | Amazon: {o['order_source_external_id']} | "
                f"{addr['country_code']} {addr['city']} | {items_str}"
            )
