#!/usr/bin/env python3.11
"""Printful Availability Guard — deactivates Amazon listings for unavailable Printful variants.

Checks Printful catalog availability via v2 API, cross-references with Baselinker
inventory, and sets quantity=0 on Amazon for variants marked "not fulfillable".
Restores to 999 when variants become available again.

Usage:
    python3.11 -m etl.availability_guard --check     # dry run
    python3.11 -m etl.availability_guard --enforce    # check + deactivate/reactivate
    python3.11 -m etl.availability_guard --report     # show current state
"""

import argparse
import json
import os
import requests
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────

KEYS_DIR = Path.home() / ".keys"
STATE_FILE = Path(__file__).parent.parent / "data" / "printful_availability.json"

# Printful catalog products to monitor (product_id -> technique)
PRODUCT_CATALOG = {
    206: {"name": "Classic Dad Hat (Yupoong 6245CM)", "technique": "EMBROIDERY"},
    # Add more products here as needed:
    # 252: {"name": "Trucker Cap", "technique": "EMBROIDERY"},
}

# Variant ID -> color name (for readable output)
VARIANT_COLORS = {
    7853: "White", 7854: "Black", 7855: "Khaki", 7856: "Light Blue",
    7857: "Navy", 7858: "Pink", 7859: "Stone", 8745: "Spruce",
    9794: "Green Camo", 12735: "Cranberry", 12736: "Dark Grey",
}

SELLER_ID = "A1IZH6PW7A624A"
EU_MARKETPLACES = {
    "DE": "A1PA6795UKMFR9", "FR": "A13V1IB3VIYZZH", "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS", "NL": "A1805IZSGTT6HS", "SE": "A2NODRKZP88ZB9",
    "PL": "A1C3SOZRARQ6R3", "BE": "AMEN7PMS3EDWL",
}
BL_INVENTORY_ID = 52954
BL_WAREHOUSE_ID = "bl_79555"


def load_env(path):
    vals = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip().replace("\r", "")
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip()
    return vals


# Load credentials
_pf = load_env(KEYS_DIR / "printful.env")
PF_TOKEN = _pf.get("PRINTFUL_API_TOKEN_V2", _pf.get("PRINTFUL_API_TOKEN", ""))
PF_STORE_ID = _pf.get("PRINTFUL_STORE_ID", "15269225")

_bl = load_env(KEYS_DIR / "baselinker.env")
BL_TOKEN = _bl.get("BASELINKER_API_TOKEN", "")

_amz_path = KEYS_DIR / "amazon-sp-api.json"
AMZ_CREDS = json.loads(_amz_path.read_text()) if _amz_path.exists() else {}


# ── API Helpers ─────────────────────────────────────────────────────────────

def pf_get(path, params=None):
    """GET Printful v2 API with retry."""
    headers = {
        "Authorization": f"Bearer {PF_TOKEN}",
        "X-PF-Store-Id": PF_STORE_ID,
    }
    for attempt in range(3):
        r = requests.get(f"https://api.printful.com{path}", headers=headers, params=params or {})
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


def bl_api(method, params):
    """Call Baselinker API."""
    r = requests.post(
        "https://api.baselinker.com/connector.php",
        data={"token": BL_TOKEN, "method": method, "parameters": json.dumps(params)},
    )
    data = r.json()
    if data.get("status") != "SUCCESS":
        print(f"  [BL] {method} error: {data.get('error_message', data)}")
        return None
    return data


def get_amz_access_token():
    """Get Amazon SP-API access token via refresh token."""
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": AMZ_CREDS.get("refresh_token", ""),
        "client_id": AMZ_CREDS.get("client_id", ""),
        "client_secret": AMZ_CREDS.get("client_secret", ""),
    })
    if r.status_code == 200:
        return r.json()["access_token"]
    print(f"  [Amazon] Token refresh failed: {r.status_code} {r.text[:200]}")
    return None


def amz_patch_listing(sku, marketplace_id, quantity, access_token):
    """PATCH Amazon listing quantity."""
    url = f"https://sellingpartnerapi-eu.amazon.com/listings/2021-08-01/items/{SELLER_ID}/{sku}"
    r = requests.patch(
        url,
        params={"marketplaceIds": marketplace_id},
        headers={
            "x-amz-access-token": access_token,
            "Content-Type": "application/json",
        },
        json={
            "productType": "HAT",
            "patches": [{
                "op": "replace",
                "path": "/attributes/fulfillment_availability",
                "value": [{
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": quantity,
                    "marketplace_id": marketplace_id,
                }],
            }],
        },
    )
    return r.status_code in (200, 202), r.status_code


# ── State Management ────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_check": None, "variants": {}}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["last_check"] = datetime.now(timezone.utc).isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── Core Logic ──────────────────────────────────────────────────────────────

def check_printful_availability():
    """Check all monitored products, return {variant_id: "in stock"|"not fulfillable"}."""
    availability = {}

    for product_id, info in PRODUCT_CATALOG.items():
        technique = info["technique"]
        print(f"\n  Checking {info['name']} (product {product_id}, {technique})...")

        data = pf_get(
            f"/v2/catalog-products/{product_id}/availability",
            {"techniques": technique},
        )
        if not data or "data" not in data:
            print(f"    WARNING: Could not fetch availability for product {product_id}")
            continue

        for item in data["data"]:
            vid = item["catalog_variant_id"]
            status = "unknown"
            for tech in item.get("techniques", []):
                for region in tech.get("selling_regions", []):
                    if region["name"] == "europe":
                        status = region["availability"]
                        break

            color = VARIANT_COLORS.get(vid, f"variant_{vid}")
            availability[vid] = status
            marker = "OK" if status == "in stock" else "UNAVAILABLE"
            print(f"    {color} ({vid}): {marker}")

    return availability


def find_affected_skus(unavailable_variant_ids):
    """Find all Baselinker SKUs containing unavailable variant IDs."""
    if not unavailable_variant_ids:
        return {}

    print(f"\n  Scanning Baselinker inventory for affected SKUs...")
    affected = {}  # variant_id -> [sku1, sku2, ...]

    # Get all products from Printful inventory
    page = 1
    all_products = {}
    while True:
        data = bl_api("getInventoryProductsList", {
            "inventory_id": BL_INVENTORY_ID,
            "filter_sort": "id",
            "page": page,
        })
        if not data or not data.get("products"):
            break
        products = data["products"]
        if not products:
            break
        all_products.update(products)
        page += 1
        time.sleep(0.3)

    print(f"  Found {len(all_products)} products in Baselinker inventory")

    # Check each product's SKU for affected variant IDs
    for prod_id, prod_info in all_products.items():
        sku = prod_info.get("sku", "")
        for vid in unavailable_variant_ids:
            vid_str = str(vid)
            # Match SKU pattern: PFT-{template}-{variant_id} or PFT-MEGA-*-{variant_id}
            if sku.endswith(f"-{vid_str}"):
                if vid not in affected:
                    affected[vid] = []
                affected[vid].append(sku)

    return affected


def enforce_availability(availability, dry_run=True):
    """Deactivate unavailable variants, reactivate restored ones."""
    state = load_state()

    # Identify unavailable variants
    unavailable = {vid for vid, status in availability.items() if status != "in stock"}
    available = {vid for vid, status in availability.items() if status == "in stock"}

    # Previously deactivated variants that are now available (need reactivation)
    to_reactivate = set()
    for vid_str, info in state.get("variants", {}).items():
        vid = int(vid_str)
        if vid in available and info.get("status") == "not_fulfillable":
            to_reactivate.add(vid)

    # New unavailable variants (need deactivation)
    previously_deactivated = {int(v) for v in state.get("variants", {}).keys()
                              if state["variants"][v].get("status") == "not_fulfillable"}
    to_deactivate = unavailable - previously_deactivated

    # Also check if there are unavailable variants we haven't deactivated yet
    to_deactivate = unavailable  # always enforce all unavailable

    if not to_deactivate and not to_reactivate:
        print("\n  No changes needed.")
        return

    # Find affected SKUs for deactivation
    affected_deactivate = find_affected_skus(to_deactivate) if to_deactivate else {}
    affected_reactivate = find_affected_skus(to_reactivate) if to_reactivate else {}

    # Get Amazon access token
    amz_token = None
    if not dry_run and (affected_deactivate or affected_reactivate):
        amz_token = get_amz_access_token()
        if not amz_token:
            print("  ERROR: Cannot get Amazon access token. Skipping Amazon updates.")

    # Process deactivations
    for vid, skus in affected_deactivate.items():
        color = VARIANT_COLORS.get(vid, f"variant_{vid}")
        print(f"\n  DEACTIVATE: {color} ({vid}) -> {len(skus)} SKUs")

        for sku in skus:
            if dry_run:
                print(f"    [DRY RUN] Would set qty=0 for {sku}")
            else:
                # Amazon: set quantity 0 on DE marketplace (primary)
                if amz_token:
                    for market_code, market_id in EU_MARKETPLACES.items():
                        ok, status_code = amz_patch_listing(sku, market_id, 0, amz_token)
                        marker = "OK" if ok else f"FAIL({status_code})"
                        print(f"    Amazon {market_code}: qty=0 for {sku} [{marker}]")
                        time.sleep(0.5)  # rate limit

                print(f"    Deactivated: {sku}")

        # Update state
        state.setdefault("variants", {})[str(vid)] = {
            "status": "not_fulfillable",
            "deactivated_skus": skus,
            "since": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "color": color,
        }

    # Process reactivations
    for vid, skus in affected_reactivate.items():
        color = VARIANT_COLORS.get(vid, f"variant_{vid}")
        print(f"\n  REACTIVATE: {color} ({vid}) -> {len(skus)} SKUs")

        for sku in skus:
            if dry_run:
                print(f"    [DRY RUN] Would set qty=999 for {sku}")
            else:
                if amz_token:
                    for market_code, market_id in EU_MARKETPLACES.items():
                        ok, status_code = amz_patch_listing(sku, market_id, 999, amz_token)
                        marker = "OK" if ok else f"FAIL({status_code})"
                        print(f"    Amazon {market_code}: qty=999 for {sku} [{marker}]")
                        time.sleep(0.5)

                print(f"    Reactivated: {sku}")

        # Remove from state
        state["variants"].pop(str(vid), None)

    if not dry_run:
        save_state(state)
        print(f"\n  State saved to {STATE_FILE}")


def show_report():
    """Show current availability state."""
    state = load_state()
    print(f"\nLast check: {state.get('last_check', 'never')}")
    variants = state.get("variants", {})
    if not variants:
        print("No deactivated variants.")
        return

    print(f"\nDeactivated variants ({len(variants)}):")
    for vid, info in variants.items():
        color = info.get("color", vid)
        skus = info.get("deactivated_skus", [])
        since = info.get("since", "?")
        print(f"  {color} ({vid}): {len(skus)} SKUs deactivated since {since}")
        for sku in skus:
            print(f"    - {sku}")


# ── Entry Points ────────────────────────────────────────────────────────────

def run_guard(dry_run=True):
    """Main entry point for ETL integration."""
    print("=" * 60)
    print(f"Printful Availability Guard ({'DRY RUN' if dry_run else 'ENFORCE'})")
    print("=" * 60)

    print("\n[1/2] Checking Printful availability...")
    availability = check_printful_availability()

    unavailable_count = sum(1 for s in availability.values() if s != "in stock")
    total = len(availability)
    print(f"\n  Summary: {total - unavailable_count}/{total} variants available, {unavailable_count} unavailable")

    print("\n[2/2] Enforcing availability...")
    enforce_availability(availability, dry_run=dry_run)

    print(f"\n{'=' * 60}")
    print("Done.")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(description="Printful Availability Guard")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="Dry run: check and report only")
    group.add_argument("--enforce", action="store_true", help="Check and deactivate/reactivate")
    group.add_argument("--report", action="store_true", help="Show current state")
    args = parser.parse_args()

    if args.report:
        show_report()
    elif args.check:
        run_guard(dry_run=True)
    elif args.enforce:
        run_guard(dry_run=False)


if __name__ == "__main__":
    main()
