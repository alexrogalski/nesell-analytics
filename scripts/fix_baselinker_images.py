#!/usr/bin/env python3.11
"""
Fix expired Printful image URLs in Baselinker inventory 52954.

Strategy:
1. Get all PFT- products from Baselinker inventory 52954
2. Check which ones have expired image URLs (403/404)
3. For each expired product, generate fresh mockup images via Printful mockup API
4. Upload each mockup to Printful File API to get permanent CDN URLs
5. Update BL products with permanent CDN URLs (url: prefix format, 0-indexed)

Key insight: Printful mockup generator produces temporary S3 URLs that expire.
But uploading those to Printful's /files API creates permanent CDN URLs
at files.cdn.printful.com (preview_url field).

Baselinker addInventoryProduct images format:
  {"0": "url:https://...", "1": "url:https://...", "2": ""}
  0-indexed positions, url: prefix required, empty string to clear slot.

Author: Claude Code
Date: 2026-03-10
"""

import requests
import json
import time
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# --- Config ---
KEYS_DIR = Path.home() / ".keys"
INVENTORY_ID = 52954
MAX_IMAGES_PER_PRODUCT = 7  # Front + 6 extras from mockup generator


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
_bl = load_env(KEYS_DIR / "baselinker.env")
BL_TOKEN = _bl.get("BASELINKER_API_TOKEN", "")

_pf = load_env(KEYS_DIR / "printful.env")
PRINTFUL_TOKEN = _pf.get("PRINTFUL_API_TOKEN", "")
PRINTFUL_STORE_ID = _pf.get("PRINTFUL_STORE_ID", "15269225")

if not BL_TOKEN:
    print("ERROR: Missing BASELINKER_API_TOKEN in ~/.keys/baselinker.env")
    sys.exit(1)
if not PRINTFUL_TOKEN:
    print("ERROR: Missing PRINTFUL_API_TOKEN in ~/.keys/printful.env")
    sys.exit(1)


# --- API Helpers ---

bl_session = requests.Session()
bl_session.headers["X-BLToken"] = BL_TOKEN

pf_headers = {
    "Authorization": f"Bearer {PRINTFUL_TOKEN}",
    "X-PF-Store-Id": PRINTFUL_STORE_ID,
    "Content-Type": "application/json",
}


def bl_call(method, params=None, retries=5):
    """Call Baselinker API with retry on rate limit."""
    for attempt in range(retries):
        resp = bl_session.post(
            "https://api.baselinker.com/connector.php",
            data={
                "method": method,
                "parameters": json.dumps(params or {}),
            },
        )
        data = resp.json()
        if data.get("status") == "ERROR":
            msg = data.get("error_message", "")
            if "limit exceeded" in msg.lower() or "blocked until" in msg.lower():
                wait = 60 * (attempt + 1)
                print(f"  [BL Rate limit] Waiting {wait}s (attempt {attempt+1}/{retries})...")
                time.sleep(wait)
                continue
            raise Exception(f"Baselinker {method}: {msg}")
        return data
    raise Exception(f"Baselinker {method}: rate limit exceeded after {retries} retries")


def pf_get(path, params=None, retries=5):
    """GET request to Printful API with retry on rate limit and connection errors."""
    for attempt in range(retries):
        try:
            r = requests.get(
                f"https://api.printful.com{path}",
                headers=pf_headers,
                params=params or {},
                timeout=30,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            wait = 30 * (attempt + 1)
            print(f"  [PF Connection error] {e.__class__.__name__}, waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  [PF Rate limit] Waiting {wait}s...")
            time.sleep(wait + 2)
            continue
        # Handle rate limit in response body
        if r.status_code == 200:
            data = r.json()
            result = data.get("result")
            if isinstance(result, str) and "too many" in result.lower():
                wait = 30
                print(f"  [PF Rate limit in body] Waiting {wait}s...")
                time.sleep(wait)
                continue
            return data
        print(f"  [Printful] GET {path} -> {r.status_code}: {r.text[:200]}")
        return None
    return None


def pf_post(path, payload, retries=5):
    """POST request to Printful API with retry on rate limit and connection errors."""
    for attempt in range(retries):
        try:
            r = requests.post(
                f"https://api.printful.com{path}",
                headers=pf_headers,
                json=payload,
                timeout=30,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            wait = 30 * (attempt + 1)
            print(f"  [PF Connection error] {e.__class__.__name__}, waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  [PF Rate limit] Waiting {wait}s...")
            time.sleep(wait + 2)
            continue
        if r.status_code == 200:
            data = r.json()
            result = data.get("result")
            if isinstance(result, str) and "too many" in result.lower():
                wait = 30
                print(f"  [PF Rate limit in body] Waiting {wait}s...")
                time.sleep(wait)
                continue
            return data
        print(f"  [Printful] POST {path} -> {r.status_code}: {r.text[:200]}")
        return None
    return None


def check_url(url, timeout=5):
    """Check if URL is accessible. Returns True if working, False if expired/broken."""
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return False
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


def upload_to_printful_cdn(temp_url):
    """Upload a temporary S3 URL to Printful file storage.

    Returns permanent CDN preview_url, or None on failure.
    Printful processes the file and creates a permanent URL at files.cdn.printful.com.
    """
    # Upload via Printful file API
    result = pf_post("/files", {"url": temp_url, "type": "default"})
    if not result or not result.get("result"):
        return None

    file_id = result["result"].get("id")
    if not file_id:
        return None

    # Poll for processing completion
    for attempt in range(15):
        time.sleep(2)
        r = pf_get(f"/files/{file_id}")
        if not r or not r.get("result"):
            continue

        file_data = r["result"]
        status = file_data.get("status", "")

        if status == "ok":
            preview_url = file_data.get("preview_url", "")
            if preview_url:
                return preview_url
            # Fallback to thumbnail
            return file_data.get("thumbnail_url", "")

        elif status == "failed":
            print(f"      File processing failed for {file_id}")
            return None

    print(f"      File processing timed out for {file_id}")
    return None


# --- Step 1: Get all PFT products from Baselinker ---

def get_bl_products():
    """Get all products from Baselinker inventory 52954."""
    print("\n[1/5] Getting products from Baselinker inventory 52954...")
    all_ids = {}
    page = 1
    while True:
        data = bl_call("getInventoryProductsList", {"inventory_id": INVENTORY_ID, "page": page})
        batch = data.get("products", {})
        all_ids.update(batch)
        print(f"  Page {page}: {len(batch)} products")
        if len(batch) < 1000:
            break
        page += 1
        time.sleep(0.5)

    # Filter PFT products
    pft_ids = [pid for pid, pd in all_ids.items() if pd.get("sku", "").startswith("PFT-")]
    print(f"  Total products: {len(all_ids)}, PFT- products: {len(pft_ids)}")

    # Get full product data
    products = {}
    for i in range(0, len(pft_ids), 100):
        batch = pft_ids[i : i + 100]
        data = bl_call(
            "getInventoryProductsData",
            {"inventory_id": INVENTORY_ID, "products": [int(x) for x in batch]},
        )
        products.update(data.get("products", {}))
        time.sleep(0.5)

    print(f"  Got full data for {len(products)} products")
    return products


# --- Step 2: Check which products have expired images ---

def check_expired_images(products):
    """Check which products have expired image URLs."""
    print("\n[2/5] Checking image URLs...")
    expired = {}
    working = {}
    no_images = {}

    for pid, pd in products.items():
        sku = pd.get("sku", "")
        images = pd.get("images", {})

        if not images:
            no_images[pid] = pd
            print(f"  {sku}: no images")
            continue

        # Check first image URL
        first_url = None
        for k in sorted(images.keys(), key=lambda x: int(x)):
            url = images[k]
            if url and isinstance(url, str) and url.startswith("http"):
                first_url = url
                break

        if not first_url:
            no_images[pid] = pd
            print(f"  {sku}: no valid image URLs")
            continue

        # Skip if already on permanent CDN
        if "cdn.baselinker.com" in first_url or "cdn.printful.com" in first_url:
            working[pid] = pd
            print(f"  {sku}: OK (permanent CDN)")
            continue

        is_working = check_url(first_url)
        if is_working:
            # Even if currently working, S3 temp URLs will expire
            # Mark for fixing if on printful-upload.s3
            if "printful-upload.s3" in first_url:
                expired[pid] = pd
                print(f"  {sku}: TEMP S3 URL (will expire, marking for fix)")
            else:
                working[pid] = pd
                print(f"  {sku}: OK (working)")
        else:
            expired[pid] = pd
            print(f"  {sku}: EXPIRED")

    print(f"\n  Summary: {len(working)} working/permanent, {len(expired)} expired/temp, {len(no_images)} no images")
    return expired, working, no_images


# --- Step 3: Get Printful product templates ---

def get_printful_templates():
    """Get all product templates from Printful store."""
    print("\n[3/5] Getting Printful product templates...")
    r = pf_get("/v2/product-templates", {"limit": 100})
    if not r:
        print("  ERROR: Could not fetch product templates")
        return {}

    templates = r.get("data", [])
    template_map = {}
    for t in templates:
        tid = t["id"]
        template_map[tid] = {
            "catalog_product_id": t["catalog_product_id"],
            "name": t.get("name", ""),
            "variant_ids": t.get("available_catalog_variants", []),
        }
        print(f"  Template {tid}: {t.get('name', '')} (catalog: {t['catalog_product_id']}, variants: {len(t.get('available_catalog_variants', []))})")

    print(f"  Found {len(template_map)} product templates")
    return template_map


# --- Step 4: Generate mockups, get permanent CDN URLs, update Baselinker ---

def generate_mockup(catalog_product_id, template_id, variant_ids=None):
    """Generate mockup images for a product template.

    Returns dict: {variant_id: [temp_url1, temp_url2, ...]}
    """
    payload = {"product_template_id": template_id}
    if variant_ids:
        payload["variant_ids"] = variant_ids

    result = pf_post(f"/mockup-generator/create-task/{catalog_product_id}", payload)
    if not result:
        return None

    result_data = result.get("result", {})
    if isinstance(result_data, str):
        print(f"    Mockup task error: {result_data}")
        return None

    task_key = result_data.get("task_key", "")
    if not task_key:
        print(f"    No task_key in response")
        return None

    # Poll for completion
    for attempt in range(30):
        time.sleep(3)
        r = pf_get("/mockup-generator/task", {"task_key": task_key})
        if not r:
            continue

        task_result = r.get("result", {})
        if isinstance(task_result, str):
            continue

        status = task_result.get("status", "")

        if status == "completed":
            mockups = task_result.get("mockups", [])
            variant_images = {}
            for m in mockups:
                vids = m.get("variant_ids", [])
                main_url = m.get("mockup_url", "")
                extra_urls = []
                for e in m.get("extra", []):
                    if isinstance(e, dict) and e.get("url"):
                        extra_urls.append(e["url"])

                all_urls = []
                if main_url:
                    all_urls.append(main_url)
                all_urls.extend(extra_urls)

                for vid in vids:
                    variant_images[vid] = all_urls

            return variant_images

        elif status == "failed":
            error = task_result.get("error", "unknown")
            print(f"    Mockup task failed: {error}")
            return None

    print(f"    Mockup task timed out for template {template_id}")
    return None


def convert_to_permanent_urls(temp_urls):
    """Convert temporary S3 URLs to permanent Printful CDN URLs.

    Uploads each temp URL to Printful file API and returns permanent preview URLs.
    """
    permanent_urls = []
    for i, temp_url in enumerate(temp_urls):
        cdn_url = upload_to_printful_cdn(temp_url)
        if cdn_url:
            permanent_urls.append(cdn_url)
            print(f"        [{i+1}/{len(temp_urls)}] -> CDN: {cdn_url[:80]}...")
        else:
            print(f"        [{i+1}/{len(temp_urls)}] -> FAILED to get CDN URL")
        time.sleep(0.5)  # Rate limit

    return permanent_urls


def fix_expired_products(expired_products, template_map):
    """Generate fresh mockups, get permanent CDN URLs, update Baselinker."""
    print("\n[4/5] Generating mockups and converting to permanent CDN URLs...")

    # Group by template_id
    template_products = defaultdict(list)
    unmatched = []

    for pid, pd in expired_products.items():
        sku = pd.get("sku", "")
        parts = sku.replace("PFT-", "").split("-")
        try:
            template_id = int(parts[0])
        except (ValueError, IndexError):
            unmatched.append((pid, sku))
            continue

        variant_id = None
        if len(parts) > 1:
            try:
                variant_id = int(parts[1])
            except ValueError:
                pass

        template_products[template_id].append({
            "bl_product_id": pid,
            "sku": sku,
            "variant_id": variant_id,
            "product_data": pd,
        })

    if unmatched:
        print(f"  WARNING: {len(unmatched)} products could not be matched to templates:")
        for pid, sku in unmatched:
            print(f"    {sku} (BL ID: {pid})")

    fixed = 0
    failed = 0
    results = []

    for template_id, bl_products in sorted(template_products.items()):
        tmpl_info = template_map.get(template_id)
        if not tmpl_info:
            print(f"\n  Template {template_id}: NOT FOUND in Printful templates")
            for bp in bl_products:
                failed += 1
                results.append({"sku": bp["sku"], "status": "failed", "reason": "template not found"})
            continue

        catalog_pid = tmpl_info["catalog_product_id"]
        template_name = tmpl_info["name"]
        available_variants = tmpl_info["variant_ids"]

        print(f"\n  Template {template_id} ({template_name}):")
        print(f"    Catalog product: {catalog_pid}, Variants: {len(available_variants)}")
        print(f"    BL products to fix: {len(bl_products)}")

        # Determine which variants we need
        needed_variant_ids = set()
        for bp in bl_products:
            if bp["variant_id"]:
                needed_variant_ids.add(bp["variant_id"])
            else:
                # Parent product: use first available variant
                if available_variants:
                    needed_variant_ids.add(available_variants[0])

        if not needed_variant_ids:
            print(f"    No variant IDs available, trying with catalog variants...")
            if available_variants:
                needed_variant_ids.add(available_variants[0])
            else:
                for bp in bl_products:
                    failed += 1
                    results.append({"sku": bp["sku"], "status": "failed", "reason": "no variant IDs"})
                continue

        variant_ids_list = sorted(needed_variant_ids)
        print(f"    Generating mockups for {len(variant_ids_list)} variant(s): {variant_ids_list}")

        # Generate mockups (temp S3 URLs)
        mockup_images = generate_mockup(catalog_pid, template_id, variant_ids_list)
        if not mockup_images:
            for bp in bl_products:
                failed += 1
                results.append({"sku": bp["sku"], "status": "failed", "reason": "mockup generation failed"})
            continue

        print(f"    Got temp mockups for {len(mockup_images)} variant(s)")

        # Convert temp URLs to permanent CDN URLs for each variant
        permanent_mockups = {}
        for vid, temp_urls in mockup_images.items():
            print(f"    Converting variant {vid} ({len(temp_urls)} images) to CDN URLs...")
            permanent = convert_to_permanent_urls(temp_urls[:MAX_IMAGES_PER_PRODUCT])
            if permanent:
                permanent_mockups[vid] = permanent
            else:
                print(f"      WARNING: No permanent URLs obtained for variant {vid}")

        if not permanent_mockups:
            print(f"    ERROR: Could not get any permanent URLs for template {template_id}")
            for bp in bl_products:
                failed += 1
                results.append({"sku": bp["sku"], "status": "failed", "reason": "CDN conversion failed"})
            continue

        # Update each BL product
        for bp in bl_products:
            sku = bp["sku"]
            variant_id = bp["variant_id"]

            # Select the right images
            if variant_id and variant_id in permanent_mockups:
                urls = permanent_mockups[variant_id]
            elif variant_id:
                first_vid = next(iter(permanent_mockups.keys()))
                urls = permanent_mockups[first_vid]
                print(f"      WARNING: variant {variant_id} not available, using {first_vid}")
            else:
                first_vid = next(iter(permanent_mockups.keys()))
                urls = permanent_mockups[first_vid]

            # Build image dict: 0-indexed, url: prefix, clear old slots
            image_dict = {}
            for i, url in enumerate(urls[:MAX_IMAGES_PER_PRODUCT]):
                image_dict[str(i)] = f"url:{url}"
            # Clear remaining slots
            for j in range(len(urls), 16):
                image_dict[str(j)] = ""

            if not any(v for v in image_dict.values() if v):
                failed += 1
                results.append({"sku": sku, "status": "failed", "reason": "no URLs to set"})
                continue

            # Update in Baselinker
            bl_pid = int(bp["bl_product_id"])
            try:
                update_data = bl_call(
                    "addInventoryProduct",
                    {
                        "inventory_id": INVENTORY_ID,
                        "product_id": str(bl_pid),
                        "images": image_dict,
                    },
                )

                if update_data.get("product_id"):
                    fixed += 1
                    print(f"      {sku}: UPDATED ({len(urls)} permanent CDN images)")
                    results.append({"sku": sku, "status": "fixed", "images_count": len(urls)})
                else:
                    failed += 1
                    error_msg = update_data.get("error_message", json.dumps(update_data)[:200])
                    print(f"      {sku}: FAILED - {error_msg}")
                    results.append({"sku": sku, "status": "failed", "reason": error_msg})

            except Exception as e:
                failed += 1
                print(f"      {sku}: ERROR - {e}")
                results.append({"sku": sku, "status": "failed", "reason": str(e)})

            time.sleep(0.7)

        # Longer pause between templates for rate limiting
        time.sleep(3)

    return fixed, failed, results


# --- Step 5: Verification ---

def verify_updates(results_list, products):
    """Verify that updated products now have permanent working images."""
    print("\n[5/5] Verifying updated images...")
    verified = 0
    failed_verify = 0

    fixed_skus = {r["sku"] for r in results_list if r["status"] == "fixed"}
    if not fixed_skus:
        print("  No products to verify")
        return 0, 0

    pids_to_check = [pid for pid, pd in products.items() if pd.get("sku", "") in fixed_skus]

    for i in range(0, len(pids_to_check), 100):
        batch = pids_to_check[i : i + 100]
        data = bl_call(
            "getInventoryProductsData",
            {"inventory_id": INVENTORY_ID, "products": [int(x) for x in batch]},
        )
        time.sleep(0.5)

        for pid, pd in data.get("products", {}).items():
            sku = pd.get("sku", "")
            images = pd.get("images", {})
            if not images:
                failed_verify += 1
                print(f"  {sku}: NO IMAGES after update")
                continue

            first_url = None
            for k in sorted(images.keys(), key=lambda x: int(x)):
                url = images[k]
                if url and isinstance(url, str) and url.startswith("http"):
                    first_url = url
                    break

            if first_url:
                is_permanent = "cdn.printful.com" in first_url or "cdn.baselinker.com" in first_url
                is_working = check_url(first_url)
                if is_working:
                    verified += 1
                    domain = first_url.split("//")[1].split("/")[0] if "//" in first_url else "?"
                    perm_tag = " (PERMANENT)" if is_permanent else " (temp)"
                    print(f"  {sku}: VERIFIED{perm_tag} (domain: {domain})")
                else:
                    failed_verify += 1
                    print(f"  {sku}: STILL BROKEN")
            else:
                failed_verify += 1
                print(f"  {sku}: no valid URL found")

    print(f"\n  Verification: {verified} working, {failed_verify} still broken")
    return verified, failed_verify


# --- Main ---

def main():
    start_time = datetime.now()
    print("=" * 70)
    print("Fix Expired Printful Images in Baselinker Inventory")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Step 1
    products = get_bl_products()

    # Step 2
    expired, working, no_images = check_expired_images(products)

    if not expired and not no_images:
        print("\nAll images are working and permanent! Nothing to fix.")
        return

    # Step 3
    template_map = get_printful_templates()

    # Step 4
    to_fix = {**expired, **no_images}
    if not to_fix:
        print("\nNo products to fix.")
        return

    fixed, failed, results = fix_expired_products(to_fix, template_map)

    # Step 5
    verified, failed_verify = verify_updates(results, products)

    # Final report
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 70)
    print("FINAL REPORT")
    print("=" * 70)
    print(f"  Total PFT products:    {len(products)}")
    print(f"  Already working:       {len(working)}")
    print(f"  Expired/temp S3:       {len(expired)}")
    print(f"  No images:             {len(no_images)}")
    print(f"  Fixed (permanent CDN): {fixed}")
    print(f"  Failed:                {failed}")
    print(f"  Verified working:      {verified}")
    print(f"  Verification failed:   {failed_verify}")
    print(f"  Elapsed time:          {elapsed:.1f}s ({elapsed/60:.1f}min)")
    print("=" * 70)

    # Save results
    results_path = Path(__file__).parent / "fix_images_results.json"
    with open(results_path, "w") as f:
        json.dump(
            {
                "timestamp": start_time.isoformat(),
                "total_products": len(products),
                "already_working": len(working),
                "expired": len(expired),
                "no_images": len(no_images),
                "fixed": fixed,
                "failed": failed,
                "verified": verified,
                "results": results,
            },
            f,
            indent=2,
        )
    print(f"\nResults saved to: {results_path}")


if __name__ == "__main__":
    main()
