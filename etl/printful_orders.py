"""Create and manage Printful orders from Amazon/Baselinker order data.

Printful API v1 endpoints (v2 order endpoints not fully available):
- POST /orders                  — create draft order
- POST /orders/{id}/confirm     — confirm draft -> production
- GET  /orders/{id}             — get order details + tracking
- POST /shipping/rates          — estimate shipping cost
- DELETE /orders/{id}           — cancel order (draft/pending only)

SKU format: PFT-{template_id}-{variant_id}
Store ID: 15269225 (Baselinker native store)
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .config import PRINTFUL_STORE_ID, PRINTFUL_V1_TOKEN

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Base URL
# ---------------------------------------------------------------------------
PRINTFUL_API_BASE = "https://api.printful.com"

# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class PrintfulAPIError(Exception):
    """Base exception for Printful API errors."""

    def __init__(self, message: str, status_code: int | None = None, response_body: dict | None = None):
        self.status_code = status_code
        self.response_body = response_body or {}
        super().__init__(message)


class PrintfulAuthError(PrintfulAPIError):
    """401 Unauthorized — bad or expired token."""
    pass


class PrintfulValidationError(PrintfulAPIError):
    """400 Bad Request — invalid payload (missing fields, bad variant_id, etc.)."""
    pass


class PrintfulRateLimitError(PrintfulAPIError):
    """429 Too Many Requests — rate limit exceeded."""

    def __init__(self, message: str, retry_after: int = 60, **kwargs: Any):
        self.retry_after = retry_after
        super().__init__(message, **kwargs)


class PrintfulNotFoundError(PrintfulAPIError):
    """404 Not Found — order or resource does not exist."""
    pass


class PrintfulConflictError(PrintfulAPIError):
    """409 Conflict — order cannot be modified in its current state."""
    pass


# ---------------------------------------------------------------------------
# Internal HTTP helpers
# ---------------------------------------------------------------------------

_MAX_RETRIES = 4
_BASE_BACKOFF = 2  # seconds, doubles each retry


def _build_headers(token: str, store_id: int | str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-PF-Store-Id": str(store_id),
        "Content-Type": "application/json",
    }


def _raise_for_status(resp: requests.Response) -> None:
    """Translate HTTP errors into typed exceptions."""
    if resp.status_code in (200, 201):
        return

    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text[:500]}

    msg = body.get("result", body.get("error", {}).get("message", resp.text[:300]))

    if resp.status_code == 401:
        raise PrintfulAuthError(f"Authentication failed: {msg}", status_code=401, response_body=body)
    if resp.status_code == 400:
        raise PrintfulValidationError(f"Validation error: {msg}", status_code=400, response_body=body)
    if resp.status_code == 404:
        raise PrintfulNotFoundError(f"Not found: {msg}", status_code=404, response_body=body)
    if resp.status_code == 409:
        raise PrintfulConflictError(f"Conflict: {msg}", status_code=409, response_body=body)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        raise PrintfulRateLimitError(
            f"Rate limited (retry after {retry_after}s)",
            retry_after=retry_after,
            status_code=429,
            response_body=body,
        )
    raise PrintfulAPIError(f"HTTP {resp.status_code}: {msg}", status_code=resp.status_code, response_body=body)


def _request(
    method: str,
    path: str,
    token: str,
    store_id: int | str,
    json_body: dict | None = None,
    params: dict | None = None,
) -> dict:
    """Execute an HTTP request with exponential backoff on 429s.

    Returns the parsed JSON response body.
    Raises typed exceptions for non-retryable errors immediately.
    """
    headers = _build_headers(token, store_id)
    url = f"{PRINTFUL_API_BASE}{path}"

    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.request(
                method,
                url,
                headers=headers,
                json=json_body,
                params=params,
                timeout=30,
            )
        except requests.RequestException as exc:
            if attempt == _MAX_RETRIES - 1:
                raise PrintfulAPIError(f"Network error after {_MAX_RETRIES} attempts: {exc}") from exc
            wait = _BASE_BACKOFF * (2 ** attempt)
            logger.warning("Network error on %s %s (attempt %d/%d), retrying in %ds: %s",
                           method, path, attempt + 1, _MAX_RETRIES, wait, exc)
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", _BASE_BACKOFF * (2 ** attempt)))
            if attempt == _MAX_RETRIES - 1:
                _raise_for_status(resp)  # raises PrintfulRateLimitError
            logger.warning("Rate limited on %s %s (attempt %d/%d), waiting %ds",
                           method, path, attempt + 1, _MAX_RETRIES, retry_after)
            time.sleep(retry_after)
            continue

        _raise_for_status(resp)
        return resp.json()

    # Should not reach here, but just in case
    raise PrintfulAPIError(f"Exhausted {_MAX_RETRIES} retries for {method} {path}")


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------


def create_printful_order(
    token: str,
    store_id: int,
    order_data: dict,
) -> dict:
    """Create a new draft order in Printful.

    Args:
        token: Printful API token (v1).
        store_id: Printful store ID (e.g. 15269225).
        order_data: Dict with keys:
            - external_id (str): Baselinker order ID or other external reference.
            - recipient (dict): Shipping address with fields:
                name, address1, city, country_code, zip
                Optional: company, address2, state_code, phone, email
            - items (list[dict]): Each item has:
                variant_id (int): Printful catalog variant ID
                quantity (int): Number of units
                files (list[dict], optional): Print files [{type, url}]
            - shipping (str, optional): Shipping method (e.g. "STANDARD").

    Returns:
        dict with keys: id, external_id, status, shipping, shipping_service_name,
        created, updated, recipient, items, costs, etc.

    Raises:
        PrintfulAuthError: Bad or expired token (401).
        PrintfulValidationError: Invalid payload (400).
        PrintfulRateLimitError: Rate limit exceeded after retries (429).
        PrintfulAPIError: Other API errors.
    """
    # Validate required fields
    if "recipient" not in order_data:
        raise PrintfulValidationError("order_data must contain 'recipient'", status_code=None)
    if "items" not in order_data or not order_data["items"]:
        raise PrintfulValidationError("order_data must contain non-empty 'items' list", status_code=None)

    recipient = order_data["recipient"]
    for field in ("name", "address1", "city", "country_code", "zip"):
        if not recipient.get(field):
            raise PrintfulValidationError(
                f"recipient.{field} is required", status_code=None
            )

    # Build the Printful order payload
    payload: dict[str, Any] = {
        "recipient": {
            "name": recipient["name"],
            "address1": recipient["address1"],
            "city": recipient["city"],
            "country_code": recipient["country_code"],
            "zip": recipient["zip"],
        },
        "items": [],
    }

    # Optional recipient fields
    for opt_field in ("company", "address2", "state_code", "phone", "email"):
        if recipient.get(opt_field):
            payload["recipient"][opt_field] = recipient[opt_field]

    # External ID
    if order_data.get("external_id"):
        payload["external_id"] = str(order_data["external_id"])

    # Shipping method
    if order_data.get("shipping"):
        payload["shipping"] = order_data["shipping"]

    # Items
    for item in order_data["items"]:
        if not item.get("variant_id"):
            raise PrintfulValidationError("Each item must have a 'variant_id'", status_code=None)
        pf_item: dict[str, Any] = {
            "variant_id": int(item["variant_id"]),
            "quantity": int(item.get("quantity", 1)),
        }
        if item.get("files"):
            pf_item["files"] = item["files"]
        payload["items"].append(pf_item)

    logger.info("Creating Printful order (external_id=%s, items=%d)",
                order_data.get("external_id"), len(payload["items"]))

    data = _request("POST", "/orders", token, store_id, json_body=payload)

    result = data.get("result", data)
    order_id = result.get("id")
    status = result.get("status", "unknown")

    logger.info("Printful order created: id=%s, status=%s", order_id, status)
    return result


def confirm_printful_order(
    token: str,
    store_id: int,
    order_id: int,
) -> dict:
    """Confirm a draft order — moves it to production/fulfillment.

    This is irreversible: once confirmed, the order will be printed and shipped.
    Only draft orders can be confirmed.

    Args:
        token: Printful API token.
        store_id: Printful store ID.
        order_id: Printful internal order ID (from create_printful_order).

    Returns:
        dict with updated order data including new status.

    Raises:
        PrintfulNotFoundError: Order does not exist (404).
        PrintfulConflictError: Order is not in draft state (409).
        PrintfulAPIError: Other errors.
    """
    logger.info("Confirming Printful order %d", order_id)

    data = _request("POST", f"/orders/{order_id}/confirm", token, store_id)
    result = data.get("result", data)
    status = result.get("status", "unknown")

    logger.info("Printful order %d confirmed, new status=%s", order_id, status)
    return result


def get_printful_order_status(
    token: str,
    store_id: int,
    order_id: int,
) -> dict:
    """Get order details including fulfillment status and tracking info.

    Args:
        token: Printful API token.
        store_id: Printful store ID.
        order_id: Printful internal order ID.

    Returns:
        dict with keys:
            - status (str): draft, pending, canceled, onhold, inprocess, partial, fulfilled
            - tracking_number (str | None): Carrier tracking number if shipped.
            - tracking_url (str | None): Tracking URL if available.
            - carrier (str | None): Carrier name (e.g. "DHL", "DPD").
            - order (dict): Full order data from Printful.

    Raises:
        PrintfulNotFoundError: Order does not exist (404).
    """
    logger.info("Fetching status for Printful order %d", order_id)

    data = _request("GET", f"/orders/{order_id}", token, store_id)
    result = data.get("result", data)

    # Extract shipment/tracking info from the order
    shipments = result.get("shipments", [])
    tracking_number = None
    tracking_url = None
    carrier = None

    if shipments:
        # Use the most recent shipment
        latest = shipments[-1]
        tracking_number = latest.get("tracking_number")
        tracking_url = latest.get("tracking_url")
        carrier = latest.get("carrier")

    status = result.get("status", "unknown")

    logger.info("Printful order %d: status=%s, tracking=%s", order_id, status, tracking_number)

    return {
        "status": status,
        "tracking_number": tracking_number,
        "tracking_url": tracking_url,
        "carrier": carrier,
        "order": result,
    }


def calculate_shipping(
    token: str,
    store_id: int,
    recipient: dict,
    items: list[dict],
) -> dict:
    """Estimate shipping cost before creating an order.

    Args:
        token: Printful API token.
        store_id: Printful store ID.
        recipient: Shipping address dict with at least:
            address1, city, country_code, zip
        items: List of dicts, each with:
            variant_id (int): Printful catalog variant ID
            quantity (int): Number of units

    Returns:
        dict with keys:
            - shipping_method (str): Name of the cheapest method (e.g. "STANDARD").
            - cost (str): Shipping cost as string (e.g. "3.99").
            - currency (str): Currency code (e.g. "EUR", "USD").
            - all_methods (list[dict]): All available methods with id, name, rate, currency,
              minDeliveryDays, maxDeliveryDays.

    Raises:
        PrintfulValidationError: Invalid recipient or items.
        PrintfulAPIError: Other errors.
    """
    payload = {
        "recipient": {
            "address1": recipient.get("address1", ""),
            "city": recipient.get("city", ""),
            "country_code": recipient.get("country_code", ""),
            "zip": recipient.get("zip", ""),
        },
        "items": [
            {
                "variant_id": int(item["variant_id"]),
                "quantity": int(item.get("quantity", 1)),
            }
            for item in items
        ],
    }

    # Optional recipient fields for more accurate rates
    for opt_field in ("state_code", "phone"):
        if recipient.get(opt_field):
            payload["recipient"][opt_field] = recipient[opt_field]

    logger.info("Calculating shipping for %d items to %s",
                len(items), recipient.get("country_code"))

    data = _request("POST", "/shipping/rates", token, store_id, json_body=payload)
    rates = data.get("result", [])

    if not rates:
        return {
            "shipping_method": None,
            "cost": None,
            "currency": None,
            "all_methods": [],
        }

    # Sort by rate (cheapest first)
    sorted_rates = sorted(rates, key=lambda r: float(r.get("rate", "9999")))
    cheapest = sorted_rates[0]

    all_methods = [
        {
            "id": r.get("id", ""),
            "name": r.get("name", ""),
            "rate": r.get("rate", ""),
            "currency": r.get("currency", ""),
            "min_delivery_days": r.get("minDeliveryDays"),
            "max_delivery_days": r.get("maxDeliveryDays"),
        }
        for r in sorted_rates
    ]

    return {
        "shipping_method": cheapest.get("id", cheapest.get("name", "")),
        "cost": cheapest.get("rate", ""),
        "currency": cheapest.get("currency", "USD"),
        "all_methods": all_methods,
    }


def cancel_printful_order(
    token: str,
    store_id: int,
    order_id: int,
) -> bool:
    """Cancel a Printful order.

    Only orders in draft or pending status can be cancelled.
    Orders already in production cannot be cancelled via API.

    Args:
        token: Printful API token.
        store_id: Printful store ID.
        order_id: Printful internal order ID.

    Returns:
        True if cancellation was successful.

    Raises:
        PrintfulNotFoundError: Order does not exist (404).
        PrintfulConflictError: Order is already in production and cannot be cancelled (409).
        PrintfulAPIError: Other errors.
    """
    logger.info("Cancelling Printful order %d", order_id)

    data = _request("DELETE", f"/orders/{order_id}", token, store_id)
    result = data.get("result", data)
    status = result.get("status", "")

    logger.info("Printful order %d cancelled, status=%s", order_id, status)
    return True


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


def parse_pft_sku(sku: str) -> tuple[str | None, int | None]:
    """Parse a PFT-{template_id}-{variant_id} SKU.

    Returns:
        (template_id, catalog_variant_id) or (template_id, None) for parent SKUs.
    """
    parts = sku.split("-")
    if len(parts) == 3 and parts[0] == "PFT":
        try:
            return parts[1], int(parts[2])
        except ValueError:
            return parts[1], None
    elif len(parts) == 2 and parts[0] == "PFT":
        return parts[1], None
    return None, None


def build_order_from_baselinker(
    bl_order: dict,
    sku_to_variant: dict[str, int] | None = None,
) -> dict:
    """Convert a Baselinker order dict into the format expected by create_printful_order.

    Args:
        bl_order: Baselinker order data (from getOrders API) with keys:
            order_id, delivery_fullname, delivery_address, delivery_city,
            delivery_postcode, delivery_country_code, delivery_company,
            phone, email, products (list of {sku, quantity, ...})
        sku_to_variant: Optional mapping from SKU to Printful catalog variant ID.
            If not provided, variant_id is extracted from PFT-xxx-{variant_id} SKU.

    Returns:
        dict ready to pass to create_printful_order().

    Raises:
        PrintfulValidationError: If a SKU cannot be mapped to a variant_id.
    """
    recipient = {
        "name": bl_order.get("delivery_fullname", ""),
        "address1": bl_order.get("delivery_address", ""),
        "city": bl_order.get("delivery_city", ""),
        "country_code": bl_order.get("delivery_country_code", ""),
        "zip": bl_order.get("delivery_postcode", ""),
    }

    if bl_order.get("delivery_company"):
        recipient["company"] = bl_order["delivery_company"]
    if bl_order.get("phone"):
        recipient["phone"] = bl_order["phone"]
    if bl_order.get("email"):
        recipient["email"] = bl_order["email"]

    items = []
    for product in bl_order.get("products", []):
        sku = product.get("sku", "")
        quantity = int(product.get("quantity", 1))

        # Resolve variant_id
        variant_id = None
        if sku_to_variant and sku in sku_to_variant:
            variant_id = sku_to_variant[sku]
        else:
            _, vid = parse_pft_sku(sku)
            variant_id = vid

        if variant_id is None:
            raise PrintfulValidationError(
                f"Cannot resolve Printful variant_id for SKU '{sku}'. "
                "Only PFT-* SKUs are supported for Printful fulfillment.",
                status_code=None,
            )

        items.append({
            "variant_id": variant_id,
            "quantity": quantity,
        })

    return {
        "external_id": str(bl_order.get("order_id", "")),
        "recipient": recipient,
        "items": items,
    }


def create_order_from_baselinker(
    token: str,
    store_id: int,
    bl_order: dict,
    sku_to_variant: dict[str, int] | None = None,
    auto_confirm: bool = False,
) -> dict:
    """End-to-end: convert a Baselinker order and create it in Printful.

    Args:
        token: Printful API token.
        store_id: Printful store ID.
        bl_order: Baselinker order data dict.
        sku_to_variant: Optional SKU -> variant_id mapping.
        auto_confirm: If True, immediately confirm the draft order.

    Returns:
        dict with keys: order (Printful order data), confirmed (bool).
    """
    order_data = build_order_from_baselinker(bl_order, sku_to_variant)
    order = create_printful_order(token, store_id, order_data)

    confirmed = False
    if auto_confirm and order.get("id"):
        order = confirm_printful_order(token, store_id, order["id"])
        confirmed = True

    return {"order": order, "confirmed": confirmed}


# ---------------------------------------------------------------------------
# CLI entry point (for testing)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    token = PRINTFUL_V1_TOKEN
    store_id = int(PRINTFUL_STORE_ID)

    if not token:
        print("ERROR: PRINTFUL_API_TOKEN not found in ~/.keys/printful.env")
        sys.exit(1)

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m etl.printful_orders shipping <country_code> <variant_id> [quantity]")
        print("  python -m etl.printful_orders status <order_id>")
        print("  python -m etl.printful_orders create <json_file>")
        print("  python -m etl.printful_orders cancel <order_id>")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "shipping":
        if len(sys.argv) < 4:
            print("Usage: ... shipping <country_code> <variant_id> [quantity]")
            sys.exit(1)
        cc = sys.argv[2]
        vid = int(sys.argv[3])
        qty = int(sys.argv[4]) if len(sys.argv) > 4 else 1
        result = calculate_shipping(
            token, store_id,
            recipient={"address1": "Test", "city": "Test", "country_code": cc, "zip": "00-001"},
            items=[{"variant_id": vid, "quantity": qty}],
        )
        print(json.dumps(result, indent=2))

    elif cmd == "status":
        if len(sys.argv) < 3:
            print("Usage: ... status <order_id>")
            sys.exit(1)
        oid = int(sys.argv[2])
        result = get_printful_order_status(token, store_id, oid)
        print(json.dumps(result, indent=2, default=str))

    elif cmd == "create":
        if len(sys.argv) < 3:
            print("Usage: ... create <json_file>")
            sys.exit(1)
        with open(sys.argv[2]) as f:
            order_data = json.load(f)
        result = create_printful_order(token, store_id, order_data)
        print(json.dumps(result, indent=2, default=str))

    elif cmd == "cancel":
        if len(sys.argv) < 3:
            print("Usage: ... cancel <order_id>")
            sys.exit(1)
        oid = int(sys.argv[2])
        ok = cancel_printful_order(token, store_id, oid)
        print(f"Cancelled: {ok}")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
