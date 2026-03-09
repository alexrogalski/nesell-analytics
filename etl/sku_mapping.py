"""SKU mapping: map Amazon/Baselinker PFT-* SKUs to Printful variant IDs.

SKU format: PFT-{template_id}-{variant_id} (child) or PFT-{template_id} (parent)
- template_id: Printful product template ID (integer)
- variant_id: Printful catalog variant ID (integer)

Usage:
    from etl.sku_mapping import (
        is_printful_sku, parse_printful_sku,
        get_sku_mapping, validate_variant_availability,
    )

    mapping = get_sku_mapping(config.BASELINKER_TOKEN)
    for sku, info in mapping.items():
        print(f"{sku}: template={info['template_id']}, variant={info['variant_id']}")
"""

import re
import json
import time
import requests
from typing import Any

from . import config

# ---------------------------------------------------------------------------
# SKU parsing helpers
# ---------------------------------------------------------------------------

_PFT_PATTERN = re.compile(r"^PFT-(\d+)(?:-(\d+))?$")


def is_printful_sku(sku: str) -> bool:
    """Return True if *sku* matches the PFT-{template}-{variant} convention."""
    return bool(_PFT_PATTERN.match(sku))


def parse_printful_sku(sku: str) -> tuple[int, int]:
    """Extract (template_id, variant_id) from a PFT child SKU.

    Raises ValueError for parent SKUs (no variant part) or non-PFT SKUs.
    """
    m = _PFT_PATTERN.match(sku)
    if not m:
        raise ValueError(f"Not a Printful SKU: {sku}")
    template_id_str, variant_id_str = m.group(1), m.group(2)
    if variant_id_str is None:
        raise ValueError(f"Parent SKU has no variant_id: {sku}")
    return int(template_id_str), int(variant_id_str)


def parse_printful_sku_safe(sku: str) -> tuple[int | None, int | None]:
    """Like parse_printful_sku but returns (template_id, None) for parents
    and (None, None) for non-PFT SKUs instead of raising."""
    m = _PFT_PATTERN.match(sku)
    if not m:
        return None, None
    template_id = int(m.group(1))
    variant_id = int(m.group(2)) if m.group(2) else None
    return template_id, variant_id


# ---------------------------------------------------------------------------
# Baselinker API helper (reuses pattern from baselinker.py)
# ---------------------------------------------------------------------------

def _bl_api(token: str, method: str, params: dict | None = None) -> dict:
    """Call Baselinker API with rate-limit retry.

    Follows the same retry pattern as baselinker.bl_api but accepts an
    explicit token so callers do not depend on config.BASELINKER_TOKEN.
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
                    from datetime import datetime
                    blocked_until = datetime.strptime(
                        match.group(1), "%Y-%m-%d %H:%M:%S",
                    )
                    wait = max(
                        (blocked_until - datetime.now()).total_seconds() + 5, 30,
                    )
                else:
                    wait = 60 * (attempt + 1)
                print(f"  [Rate limit] Waiting {wait:.0f}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
                continue
            raise RuntimeError(f"Baselinker {method}: {msg}")
        return data
    raise RuntimeError(f"Baselinker {method}: rate limit exceeded after 5 retries")


# ---------------------------------------------------------------------------
# Printful API helper (reuses pattern from printful_costs.py)
# ---------------------------------------------------------------------------

def _pf_get(token: str, path: str, params: dict | None = None) -> dict | None:
    """GET request to Printful v1 API with retry on 429."""
    headers = {
        "Authorization": f"Bearer {token}",
        "X-PF-Store-Id": config.PRINTFUL_STORE_ID,
        "Content-Type": "application/json",
    }
    for _ in range(3):
        r = requests.get(
            f"https://api.printful.com{path}",
            headers=headers,
            params=params or {},
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  [Rate limit] Waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code == 200:
            return r.json()
        print(f"  [Printful] GET {path} -> {r.status_code}: {r.text[:200]}")
        return None
    return None


# ---------------------------------------------------------------------------
# SKU mapping from Baselinker inventory
# ---------------------------------------------------------------------------

def get_sku_mapping(
    baselinker_token: str,
    inventory_id: int = 52954,
) -> dict[str, dict[str, Any]]:
    """Fetch all PFT-* products from a Baselinker inventory and build a mapping.

    Returns a dict keyed by SKU::

        {
            "PFT-90034427-7853": {
                "template_id": 90034427,
                "variant_id": 7853,
                "name": "...",
                "price": 24.99,
                "bl_product_id": "123456",
                "is_parent": False,
            },
            "PFT-90034427": {
                "template_id": 90034427,
                "variant_id": None,
                "name": "...",
                "price": 0.0,
                "bl_product_id": "123456",
                "is_parent": True,
            },
        }
    """
    # Step 1: paginate product list to collect all product IDs
    all_ids: list[str] = []
    page = 1
    while True:
        data = _bl_api(baselinker_token, "getInventoryProductsList", {
            "inventory_id": inventory_id,
            "page": page,
        })
        products = data.get("products", {})
        if not products:
            break
        all_ids.extend(products.keys())
        page += 1
        time.sleep(0.3)

    print(f"  [sku_mapping] Total product IDs in inventory: {len(all_ids)}")

    # Step 2: fetch product data in batches and filter PFT-* SKUs
    mapping: dict[str, dict[str, Any]] = {}

    for i in range(0, len(all_ids), 100):
        batch_ids = all_ids[i : i + 100]
        data = _bl_api(baselinker_token, "getInventoryProductsData", {
            "inventory_id": inventory_id,
            "products": batch_ids,
        })
        products = data.get("products", {})

        for pid, p in products.items():
            sku = p.get("sku", "")
            if not is_printful_sku(sku):
                continue

            template_id, variant_id = parse_printful_sku_safe(sku)
            name = ""
            tf = p.get("text_fields", {})
            if isinstance(tf, dict):
                name = tf.get("name", "") or tf.get("name|pl", "")

            # Get price from price groups (use first available)
            prices = p.get("prices", {})
            price = 0.0
            if prices:
                first_price = next(iter(prices.values()), 0)
                price = float(first_price) if first_price else 0.0

            mapping[sku] = {
                "template_id": template_id,
                "variant_id": variant_id,
                "name": name,
                "price": price,
                "bl_product_id": pid,
                "is_parent": variant_id is None,
            }

            # Also process variants of this product
            for vid, v in p.get("variants", {}).items():
                vsku = v.get("sku", "")
                if not is_printful_sku(vsku):
                    continue
                v_template_id, v_variant_id = parse_printful_sku_safe(vsku)

                v_prices = v.get("prices", {})
                v_price = 0.0
                if v_prices:
                    first_v_price = next(iter(v_prices.values()), 0)
                    v_price = float(first_v_price) if first_v_price else 0.0

                mapping[vsku] = {
                    "template_id": v_template_id,
                    "variant_id": v_variant_id,
                    "name": v.get("name", ""),
                    "price": v_price,
                    "bl_product_id": pid,
                    "is_parent": v_variant_id is None,
                }

        time.sleep(0.3)

    parents = sum(1 for v in mapping.values() if v["is_parent"])
    children = len(mapping) - parents
    print(f"  [sku_mapping] Mapped {len(mapping)} PFT SKUs ({parents} parents, {children} variants)")
    return mapping


# ---------------------------------------------------------------------------
# Printful catalog availability check
# ---------------------------------------------------------------------------

def validate_variant_availability(
    printful_token: str,
    variant_id: int,
) -> bool:
    """Check whether a Printful catalog variant is available for ordering.

    Queries the Printful v1 ``/products/variant/{id}`` endpoint.
    Returns True if the variant exists and its ``in_stock`` flag is truthy,
    False otherwise (not found, discontinued, out of stock, API error).
    """
    data = _pf_get(printful_token, f"/products/variant/{variant_id}")
    if not data or not data.get("result"):
        return False

    variant = data["result"].get("variant", {})
    # The API returns availability_status and in_stock fields
    in_stock = variant.get("in_stock", False)
    availability = variant.get("availability_status", [])

    # If in_stock is explicitly set, trust it
    if isinstance(in_stock, bool):
        return in_stock

    # Fallback: check if any region has stock
    if isinstance(availability, list):
        return any(
            region.get("status") == "active"
            for region in availability
        )

    return bool(in_stock)


# ---------------------------------------------------------------------------
# Bulk availability check (convenience wrapper)
# ---------------------------------------------------------------------------

def validate_mapping_availability(
    printful_token: str,
    mapping: dict[str, dict[str, Any]],
    delay: float = 0.25,
) -> dict[str, bool]:
    """Check availability for all variant SKUs in a mapping.

    Returns {sku: is_available} for child SKUs only (parents have no variant_id).
    """
    results: dict[str, bool] = {}
    variant_skus = [
        (sku, info["variant_id"])
        for sku, info in mapping.items()
        if info["variant_id"] is not None
    ]

    total = len(variant_skus)
    for i, (sku, vid) in enumerate(variant_skus):
        results[sku] = validate_variant_availability(printful_token, vid)
        if (i + 1) % 20 == 0:
            print(f"  [availability] Checked {i+1}/{total} variants...")
        time.sleep(delay)

    available = sum(1 for v in results.values() if v)
    print(f"  [availability] {available}/{total} variants in stock")
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Quick CLI test: build mapping and print summary."""
    print("=" * 60)
    print("SKU Mapping: PFT-* -> Printful variant IDs")
    print("=" * 60)

    token = config.BASELINKER_TOKEN
    if not token:
        print("ERROR: BASELINKER_API_TOKEN not found in ~/.keys/baselinker.env")
        return

    mapping = get_sku_mapping(token)

    # Group by template
    templates: dict[int, list[str]] = {}
    for sku, info in sorted(mapping.items()):
        tid = info["template_id"]
        if tid not in templates:
            templates[tid] = []
        templates[tid].append(sku)

    print(f"\n{len(templates)} templates, {len(mapping)} total SKUs:\n")
    for tid in sorted(templates):
        skus = templates[tid]
        parent = [s for s in skus if mapping[s]["is_parent"]]
        children = [s for s in skus if not mapping[s]["is_parent"]]
        parent_name = mapping[parent[0]]["name"] if parent else "(no parent)"
        print(f"  Template {tid}: {parent_name}")
        print(f"    Parent: {parent[0] if parent else 'MISSING'}")
        print(f"    Variants: {len(children)}")
        if children[:3]:
            for c in children[:3]:
                info = mapping[c]
                print(f"      {c} -> variant {info['variant_id']}, price {info['price']}")
            if len(children) > 3:
                print(f"      ... and {len(children) - 3} more")
        print()


if __name__ == "__main__":
    main()
