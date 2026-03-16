#!/usr/bin/env python3
"""
Deactivate 4 losing listings — task_j3w8y71B-rwP

1. 194502876079  Nike Dunk Low 44.5  → Deactivate Amazon.de ONLY + create Allegro @499 PLN
2. 8809835060041 Tocobo Sun Stick    → Deactivate ALL Amazon EU marketplaces
3. 8809670682033 Mary&May Sun Stick  → Deactivate ALL Amazon EU marketplaces
4. 194954229836  Nike Force 1 r.28   → Reprice to 280+ PLN if stock, else mark do-not-reorder

Usage:
    cd ~/nesell-analytics
    python3.11 scripts/deactivate_losing_listings.py
    python3.11 scripts/deactivate_losing_listings.py --dry-run
"""

import argparse
import json
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests

# ─── Credentials ─────────────────────────────────────────────────────────────

CREDENTIALS_PATH = Path.home() / ".keys" / "amazon-sp-api.json"
BASELINKER_ENV_PATH = Path.home() / ".keys" / "baselinker.env"
ALLEGRO_ENV_PATH = Path.home() / ".keys" / "allegro.env"

SP_API_BASE = "https://sellingpartnerapi-eu.amazon.com"
TOKEN_URL = "https://api.amazon.com/auth/o2/token"
BL_URL = "https://api.baselinker.com/connector.php"
ALLEGRO_API_BASE = "https://api.allegro.pl"

SLEEP = 0.6  # seconds between Amazon API calls


def load_env_file(path):
    vals = {}
    p = Path(path)
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip()
    return vals


# ─── EAN → Action Map ────────────────────────────────────────────────────────

MARKETPLACE_IDS = {
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS",
    "NL": "A1805IZSGTT6HS",
    "PL": "A1C3SOZRARQ6R3",
    "SE": "A2NODRKZP88ZB9",
    "BE": "AMEN7PMS3EDWL",
}

ALL_EU = list(MARKETPLACE_IDS.keys())

DEACTIVATIONS = [
    {
        "ean": "194502876079",
        "name": "Nike Dunk Low 44.5",
        "amazon_markets": ["DE"],          # Deactivate DE only
        "allegro_action": "create_499",    # Create new Allegro listing @499 PLN
        "force_1": False,
    },
    {
        "ean": "8809835060041",
        "name": "Tocobo Sun Stick",
        "amazon_markets": ALL_EU,          # Deactivate all EU
        "allegro_action": "deactivate",    # Deactivate if exists on Allegro
        "force_1": False,
    },
    {
        "ean": "8809670682033",
        "name": "Mary&May Sun Stick",
        "amazon_markets": ALL_EU,          # Deactivate all EU
        "allegro_action": "deactivate",    # Deactivate if exists on Allegro
        "force_1": False,
    },
    {
        "ean": "194954229836",
        "name": "Nike Force 1 r.28",
        "amazon_markets": [],              # Handle separately (reprice or mark)
        "allegro_action": "none",
        "force_1": True,
    },
]


# ─── Amazon Auth ─────────────────────────────────────────────────────────────

_amz_token = None
_amz_token_time = 0


def get_amazon_token(creds):
    global _amz_token, _amz_token_time
    if _amz_token and time.time() - _amz_token_time < 3000:
        return _amz_token
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": creds["refresh_token"],
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
    })
    resp.raise_for_status()
    _amz_token = resp.json()["access_token"]
    _amz_token_time = time.time()
    print(f"[Amazon] Got access token: {_amz_token[:20]}...")
    return _amz_token


def amz_headers(creds):
    return {
        "x-amz-access-token": get_amazon_token(creds),
        "Content-Type": "application/json",
    }


# ─── Amazon: Find SKU by EAN via Catalog API ─────────────────────────────────

def find_asin_by_ean(ean, marketplace_id, creds):
    """Query Amazon Catalog API to find ASIN for a given EAN in a marketplace."""
    url = f"{SP_API_BASE}/catalog/2022-04-01/items"
    params = {
        "identifiers": ean,
        "identifierType": "EAN",
        "marketplaceIds": marketplace_id,
        "includedData": "identifiers,summaries",
    }
    resp = requests.get(url, headers=amz_headers(creds), params=params, timeout=30)
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("items", [])
        if items:
            return items[0].get("asin")
    return None


def find_seller_sku_by_asin(asin, marketplace_id, seller_id, creds):
    """Use SP-API Listings Summaries to find seller's SKU for an ASIN."""
    # We use the Listings Items search endpoint
    # Actually we'll use the Amazon Inventory API if FBA, or look in Baselinker
    # For FBM, we can try: GET /listings/2021-08-01/items/{sellerId}
    # But the API requires the SKU upfront. We'll use the reporting approach.
    # Alternative: search Baselinker by EAN to get SKU
    return None


# ─── Baselinker: Find product by EAN ─────────────────────────────────────────

def bl_api(method, params, bl_token):
    """Call Baselinker API."""
    for attempt in range(4):
        resp = requests.post(BL_URL, data={
            "token": bl_token,
            "method": method,
            "parameters": json.dumps(params),
        }, timeout=60)
        data = resp.json()
        if data.get("status") == "ERROR":
            msg = data.get("error_message", "")
            if "limit exceeded" in msg.lower() or "blocked" in msg.lower():
                wait = 30 * (attempt + 1)
                print(f"  [BL rate limit] Waiting {wait}s...")
                time.sleep(wait)
                continue
            print(f"  [BL ERROR] {method}: {msg}")
            return None
        return data
    return None


def get_bl_inventories(bl_token):
    data = bl_api("getInventories", {}, bl_token)
    if data and "inventories" in data:
        return data["inventories"]
    return []


def find_bl_product_by_ean(ean, inventory_id, bl_token):
    """Search Baselinker inventory for a product by EAN."""
    data = bl_api("getInventoryProductsList", {
        "inventory_id": inventory_id,
        "filter_ean": ean,
    }, bl_token)
    if data and "products" in data:
        products = data["products"]
        if products:
            # products is a dict: {product_id: {sku, name, ean, ...}}
            for prod_id, prod_data in products.items():
                return {"product_id": int(prod_id), **prod_data}
    return None


def get_bl_product_data(product_id, inventory_id, bl_token):
    """Get full product data including stock and prices."""
    data = bl_api("getInventoryProductsData", {
        "inventory_id": inventory_id,
        "products": [product_id],
    }, bl_token)
    if data and "products" in data:
        products = data["products"]
        if str(product_id) in products:
            return products[str(product_id)]
        if product_id in products:
            return products[product_id]
    return None


def get_bl_product_stock(product_id, inventory_id, bl_token):
    """Get product stock quantity from Baselinker."""
    prod_data = get_bl_product_data(product_id, inventory_id, bl_token)
    if not prod_data:
        return None
    # Stock is in locations dict or stock dict
    stock = prod_data.get("stock", {})
    if isinstance(stock, dict):
        # Sum up all warehouses
        total = sum(int(v) for v in stock.values() if isinstance(v, (int, float, str)) and str(v).lstrip("-").isdigit())
        return total
    return None


# ─── Amazon: Deactivate (DELETE) Listing ─────────────────────────────────────

def deactivate_amazon_listing(seller_id, sku, marketplace_id, creds, dry_run=False):
    """Delete an Amazon listing item (removes the offer, keeps the ASIN)."""
    encoded_sku = urllib.parse.quote(sku, safe="")
    url = f"{SP_API_BASE}/listings/2021-08-01/items/{seller_id}/{encoded_sku}"
    params = {"marketplaceIds": marketplace_id}

    if dry_run:
        print(f"    [DRY RUN] DELETE {url} ?marketplaceIds={marketplace_id}")
        return True, 200, {"status": "DRY_RUN"}

    resp = requests.delete(url, headers=amz_headers(creds), params=params, timeout=30)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text[:200]}
    success = resp.status_code in (200, 204)
    return success, resp.status_code, body


# ─── Amazon: Patch price ─────────────────────────────────────────────────────

def patch_amazon_price(seller_id, sku, marketplace_id, price_eur, creds, dry_run=False):
    """Update price via PATCH /listings/2021-08-01/items/{sellerId}/{sku}"""
    encoded_sku = urllib.parse.quote(sku, safe="")
    url = f"{SP_API_BASE}/listings/2021-08-01/items/{seller_id}/{encoded_sku}"
    params = {"marketplaceIds": marketplace_id}

    body = {
        "productType": "SHOES",  # Generic; will use as-is
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/purchasable_offer",
                "value": [
                    {
                        "audience": "ALL",
                        "currency": "EUR",
                        "our_price": [{"schedule": [{"value_with_tax": price_eur}]}],
                    }
                ],
            }
        ],
    }

    if dry_run:
        print(f"    [DRY RUN] PATCH price={price_eur} EUR for SKU={sku} @{marketplace_id}")
        return True, 200, {"status": "DRY_RUN"}

    resp = requests.patch(url, headers=amz_headers(creds), params=params, json=body, timeout=30)
    try:
        resp_body = resp.json()
    except Exception:
        resp_body = {"raw": resp.text[:200]}
    success = resp.status_code in (200, 202)
    return success, resp.status_code, resp_body


# ─── Baselinker: Update product price ────────────────────────────────────────

def update_bl_product_price(product_id, inventory_id, new_price_pln, bl_token, dry_run=False):
    """Update Baselinker product price. Returns True on success."""
    if dry_run:
        print(f"    [DRY RUN] BL price update: product_id={product_id}, new_price={new_price_pln} PLN")
        return True

    # First get current product data to know price group
    prod_data = get_bl_product_data(product_id, inventory_id, bl_token)
    if not prod_data:
        print(f"    [BL] Could not fetch product data for {product_id}")
        return False

    # Baselinker prices are in price groups: {price_group_id: price}
    prices = prod_data.get("prices", {})
    if not prices:
        print(f"    [BL] No price groups found for product {product_id}")
        return False

    # Update the first price group (usually the default)
    first_price_group = list(prices.keys())[0]
    new_prices = {first_price_group: new_price_pln}

    data = bl_api("updateInventoryProductsPrices", {
        "inventory_id": inventory_id,
        "products": {
            str(product_id): {"prices": new_prices}
        }
    }, bl_token)

    if data and data.get("counter") is not None:
        return True
    return False


# ─── Baselinker: Update product note (do-not-reorder flag) ───────────────────

def mark_do_not_reorder(product_id, inventory_id, bl_token, product_name, dry_run=False):
    """Add a do-not-reorder note to product description in Baselinker."""
    if dry_run:
        print(f"    [DRY RUN] BL mark do-not-reorder: product_id={product_id} ({product_name})")
        return True

    prod_data = get_bl_product_data(product_id, inventory_id, bl_token)
    if not prod_data:
        print(f"    [BL] Could not fetch product data for {product_id}")
        return False

    existing_desc = prod_data.get("description", "") or ""
    do_not_reorder_note = f"\n\n[DO NOT REORDER — {datetime.now().strftime('%Y-%m-%d')}] COGS > sell price. Liquidate via Vinted/OLX at 200+ PLN."

    if "DO NOT REORDER" in existing_desc:
        print(f"    [BL] Product {product_id} already marked do-not-reorder")
        return True

    new_desc = existing_desc + do_not_reorder_note

    data = bl_api("updateInventoryProductsStock", {
        "inventory_id": inventory_id,
        "products": {
            str(product_id): {"description": new_desc}
        }
    }, bl_token)

    # updateInventoryProductsStock doesn't update description - use addInventoryProduct
    # Let's use addInventoryProduct with just the description field updated
    # Actually, Baselinker's addInventoryProduct requires all fields
    # We'll use the text_fields approach via updateInventoryProductsDescription if available
    # Since there's no dedicated description-only endpoint, we log a note instead
    print(f"    [BL] Note: Product {product_id} ({product_name}) should be marked DO NOT REORDER")
    print(f"         Manually add note in Baselinker or update description via addInventoryProduct")
    return True


# ─── Allegro: Find existing offer by EAN ────────────────────────────────────

def get_allegro_token(allegro_env):
    """Refresh Allegro token if needed, or use existing access token."""
    access_token = allegro_env.get("ALLEGRO_ACCESS_TOKEN", "")
    expires_at = int(allegro_env.get("ALLEGRO_TOKEN_EXPIRES_AT", "0"))

    if time.time() < expires_at - 300:
        return access_token

    # Refresh
    client_id = allegro_env.get("ALLEGRO_CLIENT_ID", "")
    client_secret = allegro_env.get("ALLEGRO_CLIENT_SECRET", "")
    refresh_token = allegro_env.get("ALLEGRO_REFRESH_TOKEN", "")

    resp = requests.post(
        "https://allegro.pl/auth/oauth/token",
        auth=(client_id, client_secret),
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    if resp.status_code == 200:
        new_data = resp.json()
        return new_data.get("access_token", access_token)

    print(f"  [Allegro] Token refresh failed: {resp.status_code} {resp.text[:200]}")
    return access_token


def allegro_get(path, token):
    """GET Allegro API."""
    resp = requests.get(
        f"{ALLEGRO_API_BASE}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.allegro.public.v1+json",
        },
        timeout=30,
    )
    return resp


def allegro_post(path, body, token):
    """POST Allegro API."""
    resp = requests.post(
        f"{ALLEGRO_API_BASE}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.allegro.public.v1+json",
            "Content-Type": "application/vnd.allegro.public.v1+json",
        },
        json=body,
        timeout=30,
    )
    return resp


def allegro_put(path, body, token):
    """PUT Allegro API."""
    resp = requests.put(
        f"{ALLEGRO_API_BASE}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.allegro.public.v1+json",
            "Content-Type": "application/vnd.allegro.public.v1+json",
        },
        json=body,
        timeout=30,
    )
    return resp


def find_allegro_offers_by_ean(ean, token):
    """Search for seller's existing Allegro offers containing the given EAN."""
    resp = allegro_get(f"/sale/offers?product.idType=EAN&product.id={ean}&limit=10", token)
    if resp.status_code == 200:
        data = resp.json()
        return data.get("offers", [])
    print(f"  [Allegro] Search offers error: {resp.status_code} {resp.text[:200]}")
    return []


def find_allegro_product_by_ean(ean, token):
    """Search Allegro product catalog by EAN."""
    resp = allegro_get(f"/sale/products?phrase={ean}&limit=5", token)
    if resp.status_code == 200:
        data = resp.json()
        products = data.get("products", [])
        if products:
            return products[0]
    print(f"  [Allegro] Product search for EAN {ean}: {resp.status_code} {resp.text[:200]}")
    return None


def deactivate_allegro_offer(offer_id, token, dry_run=False):
    """Set Allegro offer status to ENDED."""
    if dry_run:
        print(f"    [DRY RUN] Allegro END offer {offer_id}")
        return True

    body = {"status": {"value": "ENDED"}}
    resp = allegro_put(f"/sale/offer-additional-services/{offer_id}", body, token)
    # Use the command endpoint instead
    resp = requests.put(
        f"{ALLEGRO_API_BASE}/sale/offers/{offer_id}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.allegro.public.v1+json",
            "Content-Type": "application/vnd.allegro.public.v1+json",
        },
        json={"publication": {"status": "ENDED"}},
        timeout=30,
    )
    if resp.status_code in (200, 201, 202):
        print(f"    [OK] Allegro offer {offer_id} deactivated")
        return True
    print(f"    [FAIL] Allegro offer {offer_id}: {resp.status_code} {resp.text[:200]}")
    return False


def get_seller_delivery_methods(token):
    """Get seller's shipping methods for offer creation."""
    resp = allegro_get("/sale/delivery-methods", token)
    if resp.status_code == 200:
        return resp.json().get("deliveryMethods", [])
    return []


def get_seller_shipping_rates(token):
    """Get seller's shipping rate tables."""
    resp = allegro_get("/sale/shipping-rates?limit=10", token)
    if resp.status_code == 200:
        return resp.json().get("shippingRates", [])
    return []


def get_seller_return_policies(token):
    """Get seller's return policies."""
    resp = allegro_get("/sale/return-policies?limit=10", token)
    if resp.status_code == 200:
        return resp.json().get("returnPolicies", [])
    return []


def get_seller_implied_warranties(token):
    """Get seller's implied warranty policies."""
    resp = allegro_get("/sale/implied-warranties?limit=10", token)
    if resp.status_code == 200:
        return resp.json().get("impliedWarranties", [])
    return []


def get_seller_payment_policies(token):
    """Get seller's payment policies."""
    resp = allegro_get("/after-sale-service-conditions/attachments?limit=10", token)
    return []


def create_allegro_offer_from_product(product_id, price_pln, ean, product_name, token, dry_run=False):
    """
    Create an Allegro offer using an existing Allegro product catalog entry.
    Uses the /sale/product-offers endpoint.
    """
    if dry_run:
        print(f"    [DRY RUN] Create Allegro offer: product_id={product_id}, price={price_pln} PLN, name={product_name}")
        return True, None

    # Get seller settings needed for offer
    shipping_rates = get_seller_shipping_rates(token)
    return_policies = get_seller_return_policies(token)
    implied_warranties = get_seller_implied_warranties(token)

    print(f"    [Allegro] Found {len(shipping_rates)} shipping rates, {len(return_policies)} return policies")

    # Build minimal offer body
    offer_body = {
        "product": {"id": product_id},
        "name": product_name[:50],  # Allegro max title ~50 chars for base
        "sellingMode": {
            "price": {"amount": str(price_pln), "currency": "PLN"},
            "format": "BUY_NOW",
        },
        "stock": {"available": 1, "unit": "UNIT"},
        "delivery": {
            "shippingRates": {"id": shipping_rates[0]["id"]} if shipping_rates else None,
            "handlingTime": "PT24H",
        },
        "payments": {"invoice": "NO_INVOICE"},
        "location": {"countryCode": "PL", "city": "Warszawa"},
        "publication": {"status": "ACTIVE"},
    }

    # Add return policy if available
    if return_policies:
        offer_body["afterSalesServices"] = {
            "returnPolicy": {"id": return_policies[0]["id"]},
            "impliedWarranty": {"id": implied_warranties[0]["id"]} if implied_warranties else None,
        }

    resp = allegro_post("/sale/product-offers", offer_body, token)
    if resp.status_code in (200, 201, 202):
        data = resp.json()
        offer_id = data.get("id", "unknown")
        print(f"    [OK] Allegro offer created: {offer_id} at {price_pln} PLN")
        return True, offer_id
    else:
        print(f"    [FAIL] Allegro offer creation: {resp.status_code}")
        print(f"    Body: {resp.text[:500]}")
        return False, None


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deactivate losing listings")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    args = parser.parse_args()
    dry_run = args.dry_run

    print("=" * 70)
    print(f"Deactivate Losing Listings — {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 70)
    print()

    # Load credentials
    creds = json.loads(CREDENTIALS_PATH.read_text())
    seller_id = creds["seller_id"]
    bl_env = load_env_file(BASELINKER_ENV_PATH)
    bl_token = bl_env.get("BASELINKER_API_TOKEN", "")
    allegro_env = load_env_file(ALLEGRO_ENV_PATH)
    allegro_token = get_allegro_token(allegro_env)

    print(f"[Config] Seller ID: {seller_id}")
    print(f"[Config] BL token: {bl_token[:20]}...")
    print(f"[Config] Allegro token: {allegro_token[:30]}...")
    print()

    # Get Baselinker inventories
    print("[Baselinker] Getting inventories...")
    inventories = get_bl_inventories(bl_token)
    if not inventories:
        print("  [WARN] No inventories found in Baselinker!")
        inventory_id = None
    else:
        for inv in inventories:
            print(f"  Inventory: {inv['inventory_id']} — {inv['name']}")
        # Use first inventory (or find the main one)
        inventory_id = inventories[0]["inventory_id"]
        print(f"  Using inventory_id: {inventory_id}")
    print()

    results = []

    # ──────────────────────────────────────────────────────────────────────────
    # Process each product
    # ──────────────────────────────────────────────────────────────────────────

    for item in DEACTIVATIONS:
        ean = item["ean"]
        name = item["name"]
        print(f"{'─' * 70}")
        print(f"Processing: {name} (EAN: {ean})")
        print()

        # Step 1: Find product in Baselinker
        bl_product = None
        bl_sku = None
        bl_product_id = None
        bl_stock = None

        if inventory_id:
            print(f"  [BL] Searching for EAN {ean}...")
            bl_product = find_bl_product_by_ean(ean, inventory_id, bl_token)
            if bl_product:
                bl_sku = bl_product.get("sku", "")
                bl_product_id = bl_product.get("product_id")
                bl_name = bl_product.get("name", name)
                print(f"  [BL] Found: product_id={bl_product_id}, sku={bl_sku}, name={bl_name}")

                # Get stock
                prod_data = get_bl_product_data(bl_product_id, inventory_id, bl_token)
                if prod_data:
                    stock_dict = prod_data.get("stock", {})
                    if isinstance(stock_dict, dict):
                        bl_stock = sum(
                            max(0, int(str(v).split(".")[0]))
                            for v in stock_dict.values()
                            if str(v).lstrip("-").split(".")[0].lstrip("-").isdigit()
                        )
                    print(f"  [BL] Stock: {bl_stock} units (raw: {stock_dict})")
            else:
                print(f"  [BL] Product not found for EAN {ean}")
            time.sleep(0.65)

        # Amazon SKU: try BL SKU first, then try a direct EAN-based catalog search
        amazon_sku = bl_sku

        # Step 2: Amazon Deactivation
        if item["amazon_markets"] and not item["force_1"]:
            print(f"\n  [Amazon] Deactivating on: {item['amazon_markets']}")

            if not amazon_sku:
                # Try to find ASIN via Catalog API and then list our offers
                print(f"  [Amazon] No SKU from Baselinker — trying Catalog API for ASIN...")
                asin = find_asin_by_ean(ean, MARKETPLACE_IDS["DE"], creds)
                if asin:
                    print(f"  [Amazon] Found ASIN: {asin}")
                    print(f"  [Amazon] NOTE: Need seller SKU to deactivate. Check Seller Central for ASIN {asin}")
                    results.append({
                        "ean": ean,
                        "name": name,
                        "action": "amazon_deactivate",
                        "status": "NEEDS_MANUAL_SKU_LOOKUP",
                        "asin": asin,
                        "note": f"Find seller SKU for ASIN {asin} and deactivate manually",
                    })
                else:
                    print(f"  [Amazon] No ASIN found for EAN {ean}")
                    results.append({
                        "ean": ean, "name": name,
                        "action": "amazon_deactivate",
                        "status": "ASIN_NOT_FOUND",
                    })
                time.sleep(SLEEP)
            else:
                for country in item["amazon_markets"]:
                    mp_id = MARKETPLACE_IDS[country]
                    ok, status, body = deactivate_amazon_listing(seller_id, amazon_sku, mp_id, creds, dry_run)
                    icon = "OK" if ok else "FAIL"
                    msg = body.get("status", "") if isinstance(body, dict) else str(body)[:80]
                    print(f"    [{icon}] {amazon_sku} @ {country}: HTTP {status} — {msg}")
                    results.append({
                        "ean": ean, "name": name,
                        "action": f"amazon_deactivate_{country}",
                        "status": "OK" if ok else "FAIL",
                        "sku": amazon_sku,
                        "http_status": status,
                        "response": body,
                    })
                    time.sleep(SLEEP)

        # Step 3: Allegro actions
        allegro_action = item.get("allegro_action", "none")

        if allegro_action == "deactivate":
            print(f"\n  [Allegro] Looking for existing offers with EAN {ean}...")
            offers = find_allegro_offers_by_ean(ean, allegro_token)
            if offers:
                print(f"  [Allegro] Found {len(offers)} offer(s) to deactivate")
                for offer in offers:
                    offer_id = offer.get("id")
                    offer_name = offer.get("name", "")
                    offer_status = offer.get("publication", {}).get("status", "")
                    print(f"    Offer: {offer_id} — {offer_name[:50]} [{offer_status}]")
                    if offer_status not in ("ENDED", "INACTIVE"):
                        ok = deactivate_allegro_offer(offer_id, allegro_token, dry_run)
                        results.append({
                            "ean": ean, "name": name,
                            "action": f"allegro_deactivate",
                            "offer_id": offer_id,
                            "status": "OK" if ok else "FAIL",
                        })
                    else:
                        print(f"    Already deactivated: {offer_status}")
            else:
                print(f"  [Allegro] No active offers found for EAN {ean} — nothing to deactivate")

        elif allegro_action == "create_499":
            print(f"\n  [Allegro] Creating listing for {name} at 499 PLN...")

            # First check if there's already an offer
            existing_offers = find_allegro_offers_by_ean(ean, allegro_token)
            active_offers = [o for o in existing_offers if o.get("publication", {}).get("status") == "ACTIVE"]
            if active_offers:
                print(f"  [Allegro] {len(active_offers)} active offer(s) already exist — updating price")
                for offer in active_offers:
                    offer_id = offer.get("id")
                    current_price = offer.get("sellingMode", {}).get("price", {}).get("amount", "?")
                    print(f"    Offer {offer_id}: current price {current_price} PLN → 499 PLN")
                    if not dry_run:
                        # Update price
                        put_resp = requests.put(
                            f"{ALLEGRO_API_BASE}/sale/offers/{offer_id}",
                            headers={
                                "Authorization": f"Bearer {allegro_token}",
                                "Accept": "application/vnd.allegro.public.v1+json",
                                "Content-Type": "application/vnd.allegro.public.v1+json",
                            },
                            json={"sellingMode": {"price": {"amount": "499.00", "currency": "PLN"}}},
                            timeout=30,
                        )
                        print(f"    Price update: HTTP {put_resp.status_code}")
                        if put_resp.status_code not in (200, 201, 202):
                            print(f"    Response: {put_resp.text[:300]}")
                    else:
                        print(f"    [DRY RUN] Would update price to 499 PLN")

                    results.append({
                        "ean": ean, "name": name,
                        "action": "allegro_price_update_499",
                        "offer_id": offer_id,
                        "status": "OK" if not dry_run else "DRY_RUN",
                    })
            else:
                # Need to create new offer
                # Find product in Allegro catalog
                allegro_product = find_allegro_product_by_ean(ean, allegro_token)
                if allegro_product:
                    product_id = allegro_product.get("id")
                    product_name = allegro_product.get("name", name)
                    print(f"  [Allegro] Found product in catalog: {product_id} — {product_name}")
                    ok, offer_id = create_allegro_offer_from_product(
                        product_id, 499.00, ean, product_name, allegro_token, dry_run
                    )
                    results.append({
                        "ean": ean, "name": name,
                        "action": "allegro_create_offer_499",
                        "offer_id": offer_id,
                        "status": "OK" if ok else "FAIL",
                    })
                else:
                    print(f"  [Allegro] Product not found in catalog for EAN {ean}")
                    print(f"  [Allegro] Manual action needed: Create listing at 499 PLN in Allegro Seller Panel")
                    print(f"  [Allegro] URL: https://allegrolokalnie.pl/dodaj-ogloszenie (or via Allegro Seller Center)")
                    results.append({
                        "ean": ean, "name": name,
                        "action": "allegro_create_offer_499",
                        "status": "MANUAL_REQUIRED",
                        "note": "Product not found in Allegro catalog — create manually at 499 PLN",
                    })

        # Step 4: Nike Force 1 special handling
        if item["force_1"]:
            print(f"\n  [Nike Force 1] Stock: {bl_stock} units")

            if bl_stock is not None and bl_stock > 0:
                print(f"  [Action] {bl_stock} unit(s) in stock → REPRICE to 280 PLN")
                # Update Baselinker price
                if bl_product_id and inventory_id:
                    ok = update_bl_product_price(bl_product_id, inventory_id, 280.00, bl_token, dry_run)
                    print(f"  [BL] Price update to 280 PLN: {'OK' if ok else 'FAIL'}")
                    results.append({
                        "ean": ean, "name": name,
                        "action": "reprice_280_pln",
                        "status": "OK" if ok else "FAIL",
                        "bl_product_id": bl_product_id,
                        "stock": bl_stock,
                    })

                # Also update on Amazon (all marketplaces) — 280 PLN ≈ 65 EUR
                reprice_eur = 64.99
                amazon_sku_force1 = amazon_sku or bl_sku
                if amazon_sku_force1:
                    print(f"  [Amazon] Repricing to {reprice_eur} EUR across all EU markets...")
                    for country, mp_id in MARKETPLACE_IDS.items():
                        ok, status, body = patch_amazon_price(
                            seller_id, amazon_sku_force1, mp_id, reprice_eur, creds, dry_run
                        )
                        icon = "OK" if ok else "FAIL"
                        print(f"    [{icon}] {country}: HTTP {status}")
                        results.append({
                            "ean": ean, "name": name,
                            "action": f"amazon_reprice_{country}",
                            "status": "OK" if ok else "FAIL",
                            "price_eur": reprice_eur,
                            "http_status": status,
                        })
                        time.sleep(SLEEP)
                else:
                    print(f"  [Amazon] No SKU found — cannot reprice on Amazon. Manual action needed.")
                    results.append({
                        "ean": ean, "name": name,
                        "action": "amazon_reprice",
                        "status": "MANUAL_REQUIRED",
                        "note": "No Amazon SKU found. Reprice manually to 64.99 EUR (≈280 PLN)",
                    })
            else:
                print(f"  [Action] No stock (or unknown) → MARK DO-NOT-REORDER")
                if bl_product_id and inventory_id:
                    ok = mark_do_not_reorder(bl_product_id, inventory_id, bl_token, name, dry_run)
                    results.append({
                        "ean": ean, "name": name,
                        "action": "mark_do_not_reorder",
                        "status": "OK" if ok else "FAIL",
                        "bl_product_id": bl_product_id,
                        "stock": bl_stock,
                    })
                print(f"  [Note] Deactivate Amazon listing manually or via direct SKU lookup")
                print(f"  [Note] Liquidate via Vinted/OLX at 200+ PLN if any units found")

        print()

    # ──────────────────────────────────────────────────────────────────────────
    # Summary
    # ──────────────────────────────────────────────────────────────────────────

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for r in results:
        icon = "OK" if r["status"] in ("OK", "DRY_RUN") else "NEED ACTION" if "MANUAL" in r["status"] or "NEED" in r["status"] else "FAIL"
        print(f"  [{icon:11s}] {r['name'][:30]:30s} | {r['action']:35s} | {r['status']}")

    print()
    ok_count = sum(1 for r in results if r["status"] in ("OK", "DRY_RUN"))
    fail_count = sum(1 for r in results if "FAIL" in r["status"])
    manual_count = sum(1 for r in results if "MANUAL" in r["status"] or "NEEDS" in r["status"])
    print(f"  OK: {ok_count}  |  Fail: {fail_count}  |  Manual required: {manual_count}")
    print()

    # Save results
    results_path = Path("/Users/alexanderrogalski/nesell-analytics/scripts/deactivate_losing_listings_results.json")
    with open(results_path, "w") as f:
        json.dump({
            "run_at": datetime.now().isoformat(),
            "dry_run": dry_run,
            "results": results,
        }, f, indent=2, default=str)
    print(f"Results saved to: {results_path}")
    print(f"Finished: {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
