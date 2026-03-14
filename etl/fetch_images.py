"""Fetch product images from Baselinker and Amazon SP-API for products missing image_url.

Sources:
1. Baselinker main warehouse (inv_id=30229) - has Amazon product images
2. Baselinker Printful warehouse (inv_id=52954) - already synced via sync_products
3. Fallback: SKU pattern matching against existing DB images (for variant SKUs)
4. Fallback: Amazon SP-API Catalog Items API - search by EAN/SKU identifier

Usage:
    cd ~/nesell-analytics && python3.11 -m etl.fetch_images
    cd ~/nesell-analytics && python3.11 -m etl.fetch_images --dry-run
    cd ~/nesell-analytics && python3.11 -m etl.fetch_images --warehouse 30229
"""
import argparse
import json
import re
import sys
import time

import requests

from . import config

SUPABASE_URL = config.SUPABASE_URL
SUPABASE_KEY = config.SUPABASE_KEY
BASELINKER_TOKEN = config.BASELINKER_TOKEN
BASELINKER_URL = config.BASELINKER_URL

HEADERS_DB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

DEFAULT_WAREHOUSE_ID = 30229
BATCH_SIZE = 100  # Baselinker getInventoryProductsData max batch


def bl_api(method: str, params: dict = None) -> dict:
    """Call Baselinker API with rate limit retry."""
    from datetime import datetime

    for attempt in range(5):
        resp = requests.post(BASELINKER_URL, data={
            "token": BASELINKER_TOKEN,
            "method": method,
            "parameters": json.dumps(params or {}),
        }, timeout=60)
        data = resp.json()
        if data.get("status") == "ERROR":
            msg = data.get("error_message", "")
            if "limit exceeded" in msg.lower() or "blocked until" in msg.lower():
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


def get_products_without_images() -> list[dict]:
    """Load products from Supabase where image_url IS NULL."""
    url = f"{SUPABASE_URL}/rest/v1/products"
    all_rows = []
    offset = 0
    batch = 1000
    while True:
        resp = requests.get(url, headers=HEADERS_DB, params={
            "select": "sku,name,source,ean",
            "image_url": "is.null",
            "limit": batch,
            "offset": offset,
        })
        if resp.status_code != 200:
            print(f"  [ERROR] Failed to load products: {resp.status_code} {resp.text[:200]}")
            break
        rows = resp.json()
        all_rows.extend(rows)
        if len(rows) < batch:
            break
        offset += batch
    return all_rows


def get_bl_product_list(inventory_id: int) -> dict:
    """Get all product IDs and SKUs from a Baselinker inventory.

    Returns: {sku: product_id} mapping
    """
    sku_to_id = {}
    page = 1
    while True:
        data = bl_api("getInventoryProductsList", {
            "inventory_id": inventory_id,
            "filter_limit": 1000,
            "page": page,
        })
        products = data.get("products", {})
        if not products:
            break
        for pid, p in products.items():
            sku = p.get("sku", "")
            if sku:
                sku_to_id[sku] = int(pid)
        page += 1
        print(f"  [BL] Product list page {page-1}: {len(products)} products (total SKUs: {len(sku_to_id)})")
        time.sleep(0.3)
    return sku_to_id


def fetch_images_for_ids(inventory_id: int, product_ids: list[int]) -> dict:
    """Fetch product detail data and extract images.

    Returns: {sku: image_url} for products that have images.
    """
    sku_images = {}
    total_batches = (len(product_ids) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(product_ids), BATCH_SIZE):
        batch_ids = product_ids[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        data = bl_api("getInventoryProductsData", {
            "inventory_id": inventory_id,
            "products": batch_ids,
        })
        products = data.get("products", {})

        for pid, p in products.items():
            sku = p.get("sku", "")
            if not sku:
                continue

            # Extract main image (key "1" is primary, fallback to first available)
            images = p.get("images", {})
            if isinstance(images, dict) and images:
                image_url = images.get("1") or next(iter(images.values()), None)
                if image_url and image_url.startswith("http"):
                    sku_images[sku] = image_url

            # Also check variants for their own images
            for vid, v in p.get("variants", {}).items():
                vsku = v.get("sku", "")
                if not vsku:
                    continue
                v_images = v.get("images", {})
                if isinstance(v_images, dict) and v_images:
                    v_img = v_images.get("1") or next(iter(v_images.values()), None)
                    if v_img and v_img.startswith("http"):
                        sku_images[vsku] = v_img
                elif image_url and image_url.startswith("http"):
                    # Variant inherits parent image
                    sku_images[vsku] = image_url

        print(f"  [BL] Detail batch {batch_num}/{total_batches}: "
              f"{len(products)} products, {len(sku_images)} images found so far")
        time.sleep(0.3)

    return sku_images


def update_product_images(sku_image_map: dict, dry_run: bool = False) -> int:
    """Update image_url for products in Supabase.

    Uses individual PATCH requests since PostgREST doesn't support
    bulk updates with different values per row easily.
    """
    url = f"{SUPABASE_URL}/rest/v1/products"
    updated = 0
    errors = 0

    for sku, image_url in sku_image_map.items():
        if dry_run:
            updated += 1
            continue

        resp = requests.patch(
            url,
            headers={**HEADERS_DB, "Prefer": "return=representation"},
            json={"image_url": image_url},
            params={"sku": f"eq.{sku}"},
        )
        if resp.status_code in (200, 204):
            result = resp.json() if resp.text else []
            if result:
                updated += 1
            # Product might not exist in DB (BL has more products than DB)
        else:
            errors += 1
            if errors <= 5:
                print(f"  [WARN] Failed to update {sku}: {resp.status_code} {resp.text[:100]}")

        if updated % 100 == 0 and updated > 0:
            print(f"  [DB] Updated {updated} products so far...")

    if errors > 0:
        print(f"  [WARN] {errors} update errors total")
    return updated


def get_all_product_images() -> dict:
    """Load all products with images from DB for fallback matching.

    Returns: {sku: image_url}
    """
    url = f"{SUPABASE_URL}/rest/v1/products"
    all_rows = []
    offset = 0
    batch = 1000
    while True:
        resp = requests.get(url, headers=HEADERS_DB, params={
            "select": "sku,image_url",
            "limit": batch,
            "offset": offset,
        })
        if resp.status_code != 200:
            break
        rows = resp.json()
        all_rows.extend(rows)
        if len(rows) < batch:
            break
        offset += batch
    return {
        p["sku"]: p["image_url"]
        for p in all_rows
        if p.get("image_url") and str(p["image_url"]).startswith("http")
    }


def match_by_sku_patterns(
    missing: list[dict], existing_images: dict
) -> dict:
    """Try to match products without images to existing product images.

    Uses SKU pattern extraction to find base products for variants:
    - Strip prefixes: 'FBA-', 'x2_', '5x', 'S_', 'L_', 'XL_', 'sb/'
    - Strip suffixes: '-fba', '_t4', '_x', trailing dashes/spaces
    - Extract 12-13 digit EAN from compound SKUs
    - Match size-stripped variants (e.g., 'SX7666-010-38' to 'SX7666-010-M')

    Returns: {sku: image_url}
    """
    matched = {}

    # Build EAN-based lookup from existing images
    ean_to_img = {}
    for sku, img in existing_images.items():
        # Extract numeric sequences that look like EANs
        nums = re.findall(r"(\d{12,13})", sku)
        for num in nums:
            ean_to_img.setdefault(num, img)

    for p in missing:
        sku = p["sku"]
        if sku in matched:
            continue

        # 1. Strip prefixes and suffixes for exact match
        cleaned = re.sub(r"^(\d+x|x\d+_|FBA-|[SML]_|XL_|sb/)", "", sku)
        cleaned = re.sub(r"(_x|_t4|-fba|/[\d.]+)$", "", cleaned)
        cleaned = cleaned.rstrip("-").rstrip(" ")
        if cleaned in existing_images:
            matched[sku] = existing_images[cleaned]
            continue

        # 2. Try with trailing dash variants
        stripped = sku.rstrip("-").rstrip(" ")
        if stripped in existing_images:
            matched[sku] = existing_images[stripped]
            continue
        for suffix in ["-", "--"]:
            if stripped + suffix in existing_images:
                matched[sku] = existing_images[stripped + suffix]
                break
        if sku in matched:
            continue

        # 3. Extract 12-13 digit EAN from anywhere in SKU
        nums = re.findall(r"(\d{12,13})", sku.replace(" ", ""))
        for num in nums:
            if num in existing_images:
                matched[sku] = existing_images[num]
                break
            if num in ean_to_img:
                matched[sku] = ean_to_img[num]
                break
        if sku in matched:
            continue

        # 4. For compound SKUs, try splitting on space
        parts = sku.split(" ")
        if len(parts) > 1:
            for part in parts:
                part = part.strip().rstrip("-")
                if part in existing_images:
                    matched[sku] = existing_images[part]
                    break
        if sku in matched:
            continue

        # 5. Match model-based SKUs by stripping size suffix
        # e.g., 'SX7666-010-38' -> try 'SX7666-010-*'
        m = re.match(r"^([A-Z]{2}\d{4}-\d{3})", sku)
        if m:
            base = m.group(1)
            for existing_sku, img in existing_images.items():
                if existing_sku.startswith(base):
                    matched[sku] = img
                    break

    return matched


# --- Amazon SP-API image fallback ---

# Marketplace IDs for searching - try DE first (largest), then others
AMAZON_MARKETPLACE_IDS = [
    "A1PA6795UKMFR9",  # DE
    "A13V1IB3VIYZZH",  # FR
    "APJ6JRA9NG5V4",   # IT
    "A1RKKUPIHCS9HS",  # ES
    "A1805IZSGTT6HS",  # NL
    "A1C3SOZRARQ6R3",  # PL
    "A2NODRKZP88ZB9",  # SE
    "AMEN7PMS3EDWL",   # BE
]


def _amz_headers() -> dict:
    """Get Amazon SP-API request headers with fresh access token."""
    token = config.get_amazon_token()
    return {
        "x-amz-access-token": token,
        "Content-Type": "application/json",
    }


def _amz_get(path: str, params: dict = None) -> dict:
    """GET request to Amazon SP-API with retry and backoff."""
    url = f"{config.AMZ_API_BASE}{path}"
    for attempt in range(6):
        try:
            resp = requests.get(url, headers=_amz_headers(), params=params, timeout=30)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"    [AMZ ConnectionError] retrying in {wait}s (attempt {attempt+1}/6)")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [AMZ 429] rate limited, waiting {wait}s (attempt {attempt+1}/6)...")
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            print(f"    [AMZ 403] token may be expired, refreshing (attempt {attempt+1}/6)...")
            time.sleep(3)
            continue
        if resp.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [AMZ {resp.status_code}] server error, retrying in {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 200:
            return resp.json()
        # Other error (400, 404, etc.) - return empty
        print(f"    [AMZ {resp.status_code}] {resp.text[:200]}")
        return {}
    print(f"    [AMZ WARN] All 6 attempts failed for {path}")
    return {}


def _extract_image_from_catalog_item(item: dict) -> str | None:
    """Extract the best image URL from an Amazon catalog item response."""
    images_data = item.get("images", [])
    if not images_data:
        return None

    # images is a list of marketplace-specific image sets
    for img_set in images_data:
        images_list = img_set.get("images", [])
        for img in images_list:
            # Prefer MAIN variant
            if img.get("variant") == "MAIN":
                link = img.get("link")
                if link and link.startswith("http"):
                    return link

    # Fallback: take any image
    for img_set in images_data:
        images_list = img_set.get("images", [])
        for img in images_list:
            link = img.get("link")
            if link and link.startswith("http"):
                return link

    return None


def fetch_images_from_amazon(missing_products: list[dict], dry_run: bool = False) -> dict:
    """Search Amazon Catalog Items API for product images using EAN/SKU as identifiers.

    For each product without an image, searches by identifier (EAN barcode)
    across EU marketplaces to find the ASIN, then extracts images.

    Returns: {sku: image_url}
    """
    if not config.AMZ_CREDS:
        print("  [AMZ] No Amazon SP-API credentials found, skipping")
        return {}

    sku_images = {}

    for idx, product in enumerate(missing_products):
        sku = product["sku"]
        ean = product.get("ean") or ""

        # Determine identifier to search with
        # These SKUs look like EAN barcodes (12-13 digits)
        identifier = ean if ean else sku
        if not re.match(r"^\d{10,13}$", identifier):
            print(f"    [AMZ] Skipping {sku} - not a valid EAN/UPC identifier")
            continue

        # Determine identifier type
        id_len = len(identifier)
        if id_len == 13:
            id_type = "EAN"
        elif id_len == 12:
            id_type = "UPC"
        else:
            id_type = "EAN"

        print(f"    [AMZ] ({idx+1}/{len(missing_products)}) Searching {id_type} {identifier}...")

        # searchCatalogItems only accepts 1 marketplaceId at a time with identifiers.
        # Try each marketplace until we find a result (DE first as largest market).
        found = False
        for mkt_id in AMAZON_MARKETPLACE_IDS:
            params = {
                "identifiers": identifier,
                "identifiersType": id_type,
                "marketplaceIds": mkt_id,
                "includedData": "images",
                "pageSize": 1,
            }

            data = _amz_get("/catalog/2022-04-01/items", params)
            items = data.get("items", [])

            if items:
                item = items[0]
                asin = item.get("asin", "")
                image_url = _extract_image_from_catalog_item(item)
                if image_url:
                    sku_images[sku] = image_url
                    print(f"      Found ASIN {asin} on {mkt_id}, image OK")
                    if not dry_run and asin:
                        _update_product_asin(sku, asin)
                    found = True
                    break
                else:
                    print(f"      Found ASIN {asin} on {mkt_id}, but no images - trying next marketplace")
            # Rate limit between marketplace attempts
            time.sleep(0.8)

        if not found:
            print(f"      No results found on any Amazon marketplace")

        # Extra delay between products to respect rate limits
        time.sleep(1.0)

    return sku_images


def _update_product_asin(sku: str, asin: str):
    """Update the ASIN field for a product in Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/products"
    resp = requests.patch(
        url,
        headers={**HEADERS_DB, "Prefer": "return=representation"},
        json={"asin": asin},
        params={"sku": f"eq.{sku}"},
    )
    if resp.status_code not in (200, 204):
        print(f"      [WARN] Failed to update ASIN for {sku}: {resp.status_code}")


def run(warehouse_id: int = DEFAULT_WAREHOUSE_ID, dry_run: bool = False):
    """Main pipeline: find products without images, fetch from BL + Amazon, update DB."""
    print("=" * 60)
    print("FETCH PRODUCT IMAGES FROM BASELINKER + AMAZON SP-API")
    print("=" * 60)

    # Step 1: Load products without images from DB
    print("\n[1/6] Loading products without images from DB...")
    missing = get_products_without_images()
    print(f"  Found {len(missing)} products without images")
    if not missing:
        print("  All products have images. Nothing to do.")
        return

    missing_skus = {p["sku"] for p in missing}

    # Step 2: Get BL product list to find matching product IDs
    print(f"\n[2/6] Fetching product list from Baselinker warehouse {warehouse_id}...")
    bl_sku_to_id = get_bl_product_list(warehouse_id)
    print(f"  Total BL products: {len(bl_sku_to_id)}")

    # Find which missing SKUs exist in BL
    matched_skus = missing_skus & set(bl_sku_to_id.keys())
    print(f"  Matched {len(matched_skus)} / {len(missing_skus)} missing SKUs in BL")

    # Collect the product IDs to fetch
    product_ids_to_fetch = [bl_sku_to_id[sku] for sku in matched_skus] if matched_skus else []

    # Step 3: Fetch detailed data with images from Baselinker
    relevant_images = {}
    if product_ids_to_fetch:
        print(f"\n[3/6] Fetching product images from Baselinker ({len(product_ids_to_fetch)} products)...")
        sku_images = fetch_images_for_ids(warehouse_id, product_ids_to_fetch)

        # Filter to only SKUs that are in our missing set
        relevant_images = {sku: url for sku, url in sku_images.items() if sku in missing_skus}
        print(f"  Found images for {len(relevant_images)} / {len(matched_skus)} matched products")

        # Also check: some BL products might have variants whose SKU matches our DB
        # but the parent product has a different SKU.
        unmatched_skus = missing_skus - set(relevant_images.keys())
        if unmatched_skus:
            extra = {sku: url for sku, url in sku_images.items()
                     if sku in unmatched_skus and sku not in relevant_images}
            if extra:
                relevant_images.update(extra)
                print(f"  Found {len(extra)} additional matches via variant SKUs")
    else:
        print("\n[3/6] No matching products in Baselinker, skipping...")

    still_missing = missing_skus - set(relevant_images.keys())
    if still_missing:
        print(f"  Still missing images for {len(still_missing)} products after Baselinker")

    # Step 4: Update DB with BL images
    if relevant_images:
        action = "Would update" if dry_run else "Updating"
        print(f"\n[4/6] {action} {len(relevant_images)} product images from Baselinker...")
        updated_bl = update_product_images(relevant_images, dry_run=dry_run)
    else:
        updated_bl = 0
        print("\n[4/6] No Baselinker images to update.")

    # Step 5: Fallback - match remaining products by SKU patterns
    still_missing = missing_skus - set(relevant_images.keys())
    fallback_matches = {}
    if still_missing:
        print(f"\n[5/6] Fallback: matching {len(still_missing)} remaining products by SKU patterns...")
        existing_images = get_all_product_images()
        still_missing_products = [p for p in missing if p["sku"] in still_missing]
        fallback_matches = match_by_sku_patterns(still_missing_products, existing_images)
        if fallback_matches:
            action = "Would update" if dry_run else "Updating"
            print(f"  {action} {len(fallback_matches)} products via SKU pattern matching")
            updated_fallback = update_product_images(fallback_matches, dry_run=dry_run)
        else:
            updated_fallback = 0
            print("  No additional matches found via SKU patterns")
    else:
        updated_fallback = 0

    # Step 6: Amazon SP-API fallback - search by EAN identifier
    still_missing = missing_skus - set(relevant_images.keys()) - set(fallback_matches.keys())
    updated_amazon = 0
    if still_missing:
        print(f"\n[6/6] Amazon SP-API fallback: searching {len(still_missing)} products by EAN...")
        still_missing_products = [p for p in missing if p["sku"] in still_missing]
        amazon_images = fetch_images_from_amazon(still_missing_products, dry_run=dry_run)
        if amazon_images:
            action = "Would update" if dry_run else "Updating"
            print(f"  {action} {len(amazon_images)} products via Amazon Catalog API")
            updated_amazon = update_product_images(amazon_images, dry_run=dry_run)
        else:
            print("  No images found via Amazon SP-API")
        still_missing = still_missing - set(amazon_images.keys())
    else:
        print("\n[6/6] All products have images, skipping Amazon fallback.")

    total_updated = updated_bl + updated_fallback + updated_amazon
    final_missing = len(still_missing)
    print(f"\n{'=' * 60}")
    print(f"DONE: {'[DRY RUN] ' if dry_run else ''}{total_updated} products updated with images")
    print(f"  From Baselinker: {updated_bl}")
    print(f"  From SKU patterns: {updated_fallback}")
    print(f"  From Amazon SP-API: {updated_amazon}")
    print(f"  Products still without images: {final_missing}")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch product images from Baselinker and update Supabase."
    )
    parser.add_argument(
        "--warehouse", type=int, default=DEFAULT_WAREHOUSE_ID,
        help=f"Baselinker inventory/warehouse ID (default: {DEFAULT_WAREHOUSE_ID})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be updated without making changes",
    )
    args = parser.parse_args()
    run(warehouse_id=args.warehouse, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
