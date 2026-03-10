#!/usr/bin/env python3
"""
Upload images from Baselinker to Amazon listings for all PFT-* "Make Great Again" products.
Covers all 8 EU marketplaces: DE, FR, IT, ES, NL, PL, SE, BE.

Usage: python3.11 scripts/upload_images_to_amazon.py [--dry-run] [--sku SINGLE_SKU]
"""

import requests
import json
import time
import sys
import os
import argparse
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────

BL_TOKEN = os.environ.get("BASELINKER_API_TOKEN", "")
INVENTORY_ID = 52954

# Amazon SP-API
AMAZON_CREDS_PATH = os.path.expanduser("~/.keys/amazon-sp-api.json")
AMAZON_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
AMAZON_ENDPOINT = "https://sellingpartnerapi-eu.amazon.com"
SELLER_ID = "A1IZH6PW7A624A"

MARKETPLACES = {
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS",
    "NL": "A1805IZSGTT6HS",
    "PL": "A1C3SOZRARQ6R3",
    "SE": "A2NODRKZP88ZB9",
    "BE": "AMEN7PMS3EDWL",
}

# Amazon image attribute names (in order)
IMAGE_SLOTS = ["MAIN", "PT01", "PT02", "PT03", "PT04", "PT05", "PT06", "PT07"]


# ── Baselinker API ──────────────────────────────────────────────────────────

class BaselinkerAPI:
    URL = "https://api.baselinker.com/connector.php"

    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers["X-BLToken"] = token
        self._last_request = 0

    def call(self, method, params=None):
        elapsed = time.time() - self._last_request
        if elapsed < 0.65:
            time.sleep(0.65 - elapsed)

        resp = self.session.post(self.URL, data={
            "method": method,
            "parameters": json.dumps(params or {})
        })
        self._last_request = time.time()
        data = resp.json()

        if data.get("status") == "ERROR":
            code = data.get("error_code", "?")
            msg = data.get("error_message", "Unknown error")
            if code == "TOO_MANY_REQUESTS":
                print(f"  [BL] Rate limited, waiting 60s...")
                time.sleep(60)
                return self.call(method, params)
            raise RuntimeError(f"Baselinker API error {code}: {msg}")
        return data


# ── Amazon SP-API Auth ──────────────────────────────────────────────────────

def get_amazon_access_token(creds):
    """Get a fresh LWA access token."""
    resp = requests.post(AMAZON_TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": creds["refresh_token"],
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
    })
    resp.raise_for_status()
    token_data = resp.json()
    return token_data["access_token"]


# ── Amazon Listings API ─────────────────────────────────────────────────────

def patch_listing_images(access_token, seller_id, sku, marketplace_ids, image_urls, product_type, dry_run=False):
    """
    Use the Listings Items API to PATCH images for a listing.
    PUT /listings/2021-08-01/items/{sellerId}/{sku}

    We use PATCH semantics to only update image attributes.
    """
    # Build image attributes
    images = []
    for i, url in enumerate(image_urls[:8]):  # Max 8 images
        slot = IMAGE_SLOTS[i]
        images.append({
            "marketplace_id": marketplace_ids[0],  # primary marketplace
            "media_location": url,
            "type": slot,
        })

    # Build the patches array
    patches = []
    for i, url in enumerate(image_urls[:8]):
        slot = IMAGE_SLOTS[i]
        attribute_name = "main_product_image_locator" if slot == "MAIN" else "other_product_image_locator_1"

    # Use the JSON Listings Feed approach with product images
    # The Listings API uses attributes-based patching

    # Build image attributes per Amazon Listings API spec
    image_attributes = {}

    # Main image
    if len(image_urls) > 0:
        image_attributes["main_product_image_locator"] = [{
            "media_location": image_urls[0],
            "marketplace_id": marketplace_ids[0],
        }]

    # Other images (PT01-PT07)
    other_images = []
    for i, url in enumerate(image_urls[1:8]):  # Up to 7 additional
        other_images.append({
            "media_location": url,
            "marketplace_id": marketplace_ids[0],
        })

    if other_images:
        image_attributes["other_product_image_locator_1"] = other_images

    body = {
        "productType": product_type,
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/main_product_image_locator",
                "value": image_attributes.get("main_product_image_locator", []),
            },
        ]
    }

    if other_images:
        body["patches"].append({
            "op": "replace",
            "path": "/attributes/other_product_image_locator_1",
            "value": other_images,
        })

    # URL-encode the SKU
    import urllib.parse
    encoded_sku = urllib.parse.quote(sku, safe="")

    url = f"{AMAZON_ENDPOINT}/listings/2021-08-01/items/{seller_id}/{encoded_sku}"

    # Include all marketplace IDs
    params = {
        "marketplaceIds": ",".join(marketplace_ids),
    }

    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
    }

    if dry_run:
        print(f"    [DRY RUN] Would PATCH {url}")
        print(f"    Marketplaces: {','.join(marketplace_ids)}")
        print(f"    Images: {len(image_urls[:8])} (1 main + {len(image_urls[1:8])} other)")
        return {"status": "DRY_RUN", "sku": sku}

    resp = requests.patch(url, headers=headers, params=params, json=body)

    return {
        "status_code": resp.status_code,
        "sku": sku,
        "response": resp.json() if resp.status_code in (200, 400, 403, 404) else resp.text,
    }


def get_listing_info(access_token, seller_id, sku, marketplace_id):
    """Check if a listing exists and get its productType."""
    import urllib.parse
    encoded_sku = urllib.parse.quote(sku, safe="")

    url = f"{AMAZON_ENDPOINT}/listings/2021-08-01/items/{seller_id}/{encoded_sku}"
    params = {
        "marketplaceIds": marketplace_id,
        "includedData": "attributes,summaries",
    }
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
    }

    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 404:
        return None
    elif resp.status_code == 429:
        print(f"    [AMAZON] Rate limited on GET, waiting 5s...")
        time.sleep(5)
        return get_listing_info(access_token, seller_id, sku, marketplace_id)
    else:
        print(f"    [AMAZON] GET listing error {resp.status_code}: {resp.text[:200]}")
        return None


# ── Image URL filtering ─────────────────────────────────────────────────────

def is_permanent_url(url):
    """Check if a URL is permanent (not a temp Printful/S3 signed URL)."""
    if not url:
        return False
    # Printful temp URLs have X-Amz-Expires or similar
    if "X-Amz-Expires" in url or "X-Amz-Credential" in url:
        return False
    # Printful temp S3 URLs
    if "s3.amazonaws.com" in url and "Signature" in url:
        return False
    # Baselinker CDN URLs are permanent
    if "baselinker.com" in url or "blfrn.com" in url:
        return True
    # Printful static images (not signed) are OK
    if "printful.com" in url or "files.cdn.printful.com" in url:
        return True
    # Generic URLs - assume permanent if no AWS signing
    return True


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Upload Baselinker images to Amazon listings")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually update listings")
    parser.add_argument("--sku", type=str, help="Process only a single SKU")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of products to process")
    args = parser.parse_args()

    print("=" * 80)
    print(f"AMAZON IMAGE UPLOAD - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 80)

    # Load BL token
    bl_token = BL_TOKEN
    if not bl_token:
        with open(os.path.expanduser("~/.keys/baselinker.env")) as f:
            for line in f:
                if line.startswith("BASELINKER_API_TOKEN="):
                    bl_token = line.strip().split("=", 1)[1]

    # Load Amazon creds
    with open(AMAZON_CREDS_PATH) as f:
        amazon_creds = json.load(f)

    bl = BaselinkerAPI(bl_token)

    # ── Step 1: Get all PFT-* products from Baselinker ──
    print("\n[1] Fetching PFT-* products from Baselinker inventory 52954...")

    all_products = {}
    page = 1
    while True:
        data = bl.call("getInventoryProductsList", {
            "inventory_id": INVENTORY_ID,
            "filter_sku": "PFT-",
            "page": page,
        })
        batch = data.get("products", {})
        all_products.update(batch)
        print(f"    Page {page}: {len(batch)} products")
        if len(batch) < 1000:
            break
        page += 1

    # Filter only parent products (not variants) with PFT- prefix
    parent_products = {}
    for pid, pdata in all_products.items():
        sku = pdata.get("sku", "")
        if sku.startswith("PFT-") and "great-again" in sku.lower():
            parent_products[pid] = pdata

    # Also include all PFT- products in case some don't have "great-again" in SKU
    # Let's get all PFT- parents and then check which are Make Great Again
    pft_parents = {pid: pdata for pid, pdata in all_products.items()
                   if pdata.get("sku", "").startswith("PFT-") and pdata.get("parent_id", 0) == 0}

    if args.sku:
        pft_parents = {pid: pdata for pid, pdata in pft_parents.items() if pdata.get("sku") == args.sku}

    print(f"    Found {len(pft_parents)} PFT-* parent products")

    if not pft_parents:
        # Maybe all PFT-* products are what we want (no parent_id filter in list)
        # Let's check - the list API might not return parent_id
        pft_parents = {pid: pdata for pid, pdata in all_products.items()
                       if pdata.get("sku", "").startswith("PFT-")}
        print(f"    (Without parent filter: {len(pft_parents)} products)")

    if args.limit > 0:
        # Take only first N
        limited = dict(list(pft_parents.items())[:args.limit])
        pft_parents = limited
        print(f"    (Limited to {len(pft_parents)} products)")

    # ── Step 2: Get detailed data (images) for all products ──
    print(f"\n[2] Fetching detailed data for {len(pft_parents)} products...")

    product_ids = list(pft_parents.keys())
    product_details = {}

    # Batch by 50 (API limit)
    batch_size = 50
    for i in range(0, len(product_ids), batch_size):
        batch_ids = [int(pid) for pid in product_ids[i:i+batch_size]]
        data = bl.call("getInventoryProductsData", {
            "inventory_id": INVENTORY_ID,
            "products": batch_ids,
        })
        products_data = data.get("products", {})
        product_details.update(products_data)
        print(f"    Batch {i//batch_size + 1}: got {len(products_data)} products")

    # ── Step 3: Get Amazon access token ──
    print("\n[3] Getting Amazon SP-API access token...")
    access_token = get_amazon_access_token(amazon_creds)
    token_time = time.time()
    print("    Token acquired")

    # ── Step 4: For each product, check & update all marketplaces ──
    print(f"\n[4] Processing {len(product_details)} products across {len(MARKETPLACES)} marketplaces...")

    results = {mp: {"updated": [], "skipped": [], "failed": [], "not_found": []} for mp in MARKETPLACES}
    total_updated = 0
    total_failed = 0

    for idx, (pid, pdata) in enumerate(product_details.items()):
        sku = pdata.get("sku", "")

        # Get images
        images_dict = pdata.get("images", {})

        # Images come as dict with numeric keys and URL values
        image_urls = []
        for pos in sorted(images_dict.keys(), key=lambda x: int(x)):
            url = images_dict[pos]
            if url and is_permanent_url(url):
                image_urls.append(url)

        if len(image_urls) <= 1:
            print(f"\n  [{idx+1}/{len(product_details)}] {sku} - Only {len(image_urls)} image(s), skipping")
            for mp in MARKETPLACES:
                results[mp]["skipped"].append(sku)
            continue

        print(f"\n  [{idx+1}/{len(product_details)}] {sku} - {len(image_urls)} images")

        # Refresh token if older than 50 minutes
        if time.time() - token_time > 3000:
            print("    Refreshing Amazon token...")
            access_token = get_amazon_access_token(amazon_creds)
            token_time = time.time()

        # First, check the listing on DE marketplace to get productType
        # (listings are usually created on DE first)
        product_type = None

        listing_info = get_listing_info(access_token, SELLER_ID, sku, MARKETPLACES["DE"])
        time.sleep(0.5)  # rate limiting

        if listing_info:
            # Extract product type from summaries
            summaries = listing_info.get("summaries", [])
            if summaries:
                product_type = summaries[0].get("productType", "")

            # Also check attributes for existing images
            attributes = listing_info.get("attributes", {})
            existing_main = attributes.get("main_product_image_locator", [])
            existing_other = attributes.get("other_product_image_locator_1", [])
            existing_count = len(existing_main) + len(existing_other)
            print(f"    Current images on DE: {existing_count}")

        if not product_type:
            # Try to guess or check other marketplaces
            for mp_name, mp_id in MARKETPLACES.items():
                if mp_name == "DE":
                    continue
                listing_info = get_listing_info(access_token, SELLER_ID, sku, mp_id)
                time.sleep(0.5)
                if listing_info:
                    summaries = listing_info.get("summaries", [])
                    if summaries:
                        product_type = summaries[0].get("productType", "")
                        print(f"    Found productType from {mp_name}: {product_type}")
                        break

        if not product_type:
            print(f"    WARNING: No listing found on any marketplace, skipping {sku}")
            for mp in MARKETPLACES:
                results[mp]["not_found"].append(sku)
            continue

        print(f"    productType: {product_type}")
        print(f"    Uploading {min(len(image_urls), 8)} images to all marketplaces...")

        # Now update across all marketplaces at once
        all_mp_ids = list(MARKETPLACES.values())

        result = patch_listing_images(
            access_token, SELLER_ID, sku, all_mp_ids,
            image_urls, product_type, dry_run=args.dry_run
        )

        if args.dry_run:
            for mp in MARKETPLACES:
                results[mp]["updated"].append(sku)
            total_updated += 1
        elif result.get("status_code") == 200:
            response_data = result.get("response", {})
            status = response_data.get("status", "")
            print(f"    Result: {status}")

            if status == "ACCEPTED":
                for mp in MARKETPLACES:
                    results[mp]["updated"].append(sku)
                total_updated += 1
            else:
                # Check for individual marketplace issues
                issues = response_data.get("issues", [])
                if issues:
                    for issue in issues[:3]:  # Show first 3 issues
                        print(f"    Issue: {issue.get('code', '?')}: {issue.get('message', '?')}")
                for mp in MARKETPLACES:
                    results[mp]["failed"].append({"sku": sku, "reason": str(issues[:1])})
                total_failed += 1
        else:
            status_code = result.get("status_code", "?")
            resp_text = str(result.get("response", ""))[:300]
            print(f"    FAILED ({status_code}): {resp_text}")

            # If 404, try per-marketplace
            if status_code == 404:
                print(f"    Trying per-marketplace updates...")
                for mp_name, mp_id in MARKETPLACES.items():
                    mp_listing = get_listing_info(access_token, SELLER_ID, sku, mp_id)
                    time.sleep(0.3)

                    if not mp_listing:
                        results[mp_name]["not_found"].append(sku)
                        continue

                    mp_summaries = mp_listing.get("summaries", [])
                    mp_product_type = mp_summaries[0].get("productType", product_type) if mp_summaries else product_type

                    mp_result = patch_listing_images(
                        access_token, SELLER_ID, sku, [mp_id],
                        image_urls, mp_product_type, dry_run=args.dry_run
                    )
                    time.sleep(0.5)

                    if mp_result.get("status_code") == 200:
                        mp_status = mp_result.get("response", {}).get("status", "")
                        if mp_status == "ACCEPTED":
                            results[mp_name]["updated"].append(sku)
                            print(f"      {mp_name}: OK")
                        else:
                            results[mp_name]["failed"].append({"sku": sku, "reason": mp_status})
                            print(f"      {mp_name}: {mp_status}")
                    else:
                        results[mp_name]["failed"].append({"sku": sku, "reason": f"HTTP {mp_result.get('status_code')}"})
                        print(f"      {mp_name}: FAILED ({mp_result.get('status_code')})")
            else:
                for mp in MARKETPLACES:
                    results[mp]["failed"].append({"sku": sku, "reason": f"HTTP {status_code}"})
                total_failed += 1

        time.sleep(1)  # Global rate limiting between products

    # ── Summary ──
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    for mp_name in MARKETPLACES:
        r = results[mp_name]
        print(f"\n  {mp_name}:")
        print(f"    Updated:   {len(r['updated'])}")
        print(f"    Skipped:   {len(r['skipped'])} (<=1 image)")
        print(f"    Not found: {len(r['not_found'])}")
        print(f"    Failed:    {len(r['failed'])}")
        if r['updated']:
            print(f"    Updated SKUs: {', '.join(r['updated'][:10])}")
            if len(r['updated']) > 10:
                print(f"                  ...and {len(r['updated'])-10} more")
        if r['failed']:
            for f in r['failed'][:5]:
                print(f"    Failed: {f['sku']} - {f['reason']}")

    print(f"\n  TOTAL: {total_updated} products updated, {total_failed} failed")
    print("=" * 80)

    # Save detailed results to JSON
    output_path = os.path.expanduser("~/nesell-analytics/scripts/image_upload_results.json")
    with open(output_path, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "mode": "dry_run" if args.dry_run else "live",
            "results": results,
            "total_updated": total_updated,
            "total_failed": total_failed,
        }, f, indent=2)
    print(f"\nDetailed results saved to {output_path}")


if __name__ == "__main__":
    main()
