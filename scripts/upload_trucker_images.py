#!/usr/bin/env python3
"""
Generate Printful mockups and upload to Amazon for Trucker Cap PFT-90229846 variants.
Uses Printful product template 90229846 to generate fresh mockup images,
then uploads them to Amazon listings via SP-API.

Usage: python3.11 scripts/upload_trucker_images.py [--dry-run]
"""

import requests
import json
import time
import urllib.parse
from datetime import datetime


# ── Config ──────────────────────────────────────────────────────────────────

with open('/Users/alexanderrogalski/.keys/amazon-sp-api.json') as f:
    AMZ_CREDS = json.load(f)

PRINTFUL_TOKEN = "RKXQOs4qRdYbKU4JlNQEYO2yyUqSNbIsIgvLQwsH"
STORE_ID = "15269225"

SELLER_ID = "A1IZH6PW7A624A"
AMAZON_ENDPOINT = "https://sellingpartnerapi-eu.amazon.com"

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

# Trucker Cap: Printful product 252, template 90229846
TEMPLATE_ID = 90229846
PRODUCT_ID = 252

# SKUs with 0 images on Amazon DE (confirmed by audit)
VARIANT_MAP = {
    "PFT-90229846-8747": 8747,   # Black
    "PFT-90229846-8748": 8748,   # Black/White
    "PFT-90229846-8749": 8749,   # Navy
    "PFT-90229846-8751": 8751,   # Navy/White
    "PFT-90229846-8752": 8752,   # Charcoal/Black
    "PFT-90229846-8753": 8753,   # Charcoal
}
PARENT_SKU = "PFT-90229846"
PRODUCT_TYPE = "HAT"


# ── Helpers ─────────────────────────────────────────────────────────────────

def get_amz_token():
    resp = requests.post('https://api.amazon.com/auth/o2/token', data={
        'grant_type': 'refresh_token',
        'refresh_token': AMZ_CREDS['refresh_token'],
        'client_id': AMZ_CREDS['client_id'],
        'client_secret': AMZ_CREDS['client_secret'],
    })
    resp.raise_for_status()
    return resp.json()['access_token']


def pf_headers():
    return {
        "Authorization": f"Bearer {PRINTFUL_TOKEN}",
        "X-PF-Store-Id": STORE_ID,
        "Content-Type": "application/json",
    }


def generate_mockups(variant_id, max_retries=3):
    """Generate mockup images for a Printful variant. Returns list of image URLs."""
    body = {
        "variant_ids": [variant_id],
        "format": "jpg",
        "width": 1000,
        "product_template_id": TEMPLATE_ID,
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(
                f"https://api.printful.com/mockup-generator/create-task/{PRODUCT_ID}",
                headers=pf_headers(),
                json=body,
                timeout=30,
            )
        except requests.exceptions.ConnectionError:
            print(f"    Connection error (attempt {attempt+1}), waiting 60s...")
            time.sleep(60)
            continue

        if resp.status_code == 429:
            # Rate limited - parse retry-after or wait 60s
            retry_after = 60
            try:
                msg = resp.json().get("error", {}).get("message", "")
                import re
                m = re.search(r'(\d+) seconds', msg)
                if m:
                    retry_after = int(m.group(1)) + 5
            except:
                pass
            print(f"    Rate limited, waiting {retry_after}s (attempt {attempt+1})...")
            time.sleep(retry_after)
            continue

        if resp.status_code != 200:
            print(f"    ERROR {resp.status_code}: {resp.text[:200]}")
            return None

        task_key = resp.json().get("result", {}).get("task_key")
        break
    else:
        print(f"    Failed after {max_retries} attempts")
        return None

    # Poll for completion
    for poll in range(30):
        time.sleep(5)
        try:
            resp2 = requests.get(
                f"https://api.printful.com/mockup-generator/task?task_key={task_key}",
                headers=pf_headers(),
                timeout=30,
            )
        except requests.exceptions.ConnectionError:
            print(f"    Poll connection error, retrying...")
            time.sleep(10)
            continue

        if resp2.status_code == 429:
            time.sleep(30)
            continue

        data2 = resp2.json()
        status = data2.get("result", {}).get("status", "?")

        if status == "completed":
            mockups = data2.get("result", {}).get("mockups", [])
            if not mockups:
                return None

            # Build image list
            images = [mockups[0].get("mockup_url")]
            for extra in mockups[0].get("extra", []):
                url = extra.get("url")
                if url:
                    images.append(url)
            return images

        elif status == "failed":
            error = data2.get("result", {}).get("error", "?")
            print(f"    Mockup generation failed: {error}")
            return None

    print(f"    Mockup generation timed out")
    return None


def check_listing_exists(token, sku, marketplace_id):
    """Check if an Amazon listing exists."""
    encoded_sku = urllib.parse.quote(sku, safe="")
    url = f"{AMAZON_ENDPOINT}/listings/2021-08-01/items/{SELLER_ID}/{encoded_sku}"
    headers = {"x-amz-access-token": token, "Content-Type": "application/json"}
    params = {"marketplaceIds": marketplace_id, "includedData": "summaries"}

    for retry in range(3):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        except:
            time.sleep(5)
            continue

        if resp.status_code == 429:
            time.sleep(5)
            continue
        elif resp.status_code == 200:
            return True
        elif resp.status_code == 404:
            return False
        else:
            return False

    return False


def patch_images(token, sku, marketplace_id, image_urls):
    """Upload images to an Amazon listing."""
    encoded_sku = urllib.parse.quote(sku, safe="")
    url = f"{AMAZON_ENDPOINT}/listings/2021-08-01/items/{SELLER_ID}/{encoded_sku}"

    patches = []

    # Main image
    if image_urls:
        patches.append({
            "op": "replace",
            "path": "/attributes/main_product_image_locator",
            "value": [{"media_location": image_urls[0], "marketplace_id": marketplace_id}],
        })

    # Other images (up to 7 additional)
    for i, img_url in enumerate(image_urls[1:8]):
        patches.append({
            "op": "replace",
            "path": f"/attributes/other_product_image_locator_{i+1}",
            "value": [{"media_location": img_url, "marketplace_id": marketplace_id}],
        })

    body = {
        "productType": PRODUCT_TYPE,
        "patches": patches,
    }

    headers = {"x-amz-access-token": token, "Content-Type": "application/json"}
    params = {"marketplaceIds": marketplace_id}

    for retry in range(3):
        try:
            resp = requests.patch(url, headers=headers, params=params, json=body, timeout=30)
        except:
            time.sleep(5)
            continue

        if resp.status_code == 429:
            time.sleep(5)
            continue

        try:
            resp_data = resp.json()
        except:
            resp_data = {"raw": resp.text[:300]}

        return resp.status_code, resp_data

    return 0, {"error": "max retries"}


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    import sys
    dry_run = "--dry-run" in sys.argv

    print("=" * 80)
    print(f"TRUCKER CAP IMAGE UPLOAD - {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 80)

    # Step 1: Generate mockups
    print("\n[1] Generating Printful mockups for each variant...")
    variant_images = {}

    for sku, variant_id in VARIANT_MAP.items():
        print(f"\n  {sku} (variant {variant_id}):")
        images = generate_mockups(variant_id)
        if images:
            variant_images[sku] = images
            print(f"    OK - {len(images)} images generated")
            # Verify first image is accessible
            try:
                r = requests.head(images[0], timeout=10)
                print(f"    First image status: {r.status_code}")
            except:
                print(f"    WARNING: Could not verify first image")
        else:
            print(f"    FAILED")

        # Wait between mockup generations (Printful rate limit)
        time.sleep(15)

    # Use Black variant images for parent SKU
    if "PFT-90229846-8747" in variant_images:
        variant_images[PARENT_SKU] = variant_images["PFT-90229846-8747"]
        print(f"\n  Parent SKU: using Black variant images")

    print(f"\n  Total: {len(variant_images)} SKUs with images")

    if not variant_images:
        print("\nERROR: No images generated. Exiting.")
        return

    # Step 2: Get Amazon token
    print("\n[2] Getting Amazon access token...")
    amz_token = get_amz_token()
    token_time = time.time()
    print("  OK")

    # Step 3: Upload to Amazon
    print("\n[3] Uploading images to Amazon listings...")
    results = {}

    all_skus = [PARENT_SKU] + list(VARIANT_MAP.keys())

    for sku in all_skus:
        if sku not in variant_images:
            print(f"\n  {sku}: No images, skipping")
            results[sku] = {"all": "NO_IMAGES"}
            continue

        images = variant_images[sku]
        results[sku] = {}
        print(f"\n  {sku} ({len(images)} images):")

        # Refresh token if needed
        if time.time() - token_time > 2700:
            print("    Refreshing Amazon token...")
            amz_token = get_amz_token()
            token_time = time.time()

        for mp_name, mp_id in MARKETPLACES.items():
            # Check listing exists
            exists = check_listing_exists(amz_token, sku, mp_id)
            time.sleep(0.3)

            if not exists:
                results[sku][mp_name] = "NOT_FOUND"
                continue

            if dry_run:
                results[sku][mp_name] = "DRY_RUN_OK"
                print(f"    {mp_name}: would upload {len(images)} images")
                continue

            # Upload
            status_code, resp_data = patch_images(amz_token, sku, mp_id, images)
            time.sleep(0.5)

            if status_code == 200:
                amz_status = resp_data.get("status", "?")
                results[sku][mp_name] = amz_status
                issues = resp_data.get("issues", [])
                if issues:
                    print(f"    {mp_name}: {amz_status} ({len(issues)} issues)")
                    for issue in issues[:2]:
                        print(f"      {issue.get('code', '?')}: {issue.get('message', '?')[:80]}")
                else:
                    print(f"    {mp_name}: {amz_status}")
            else:
                results[sku][mp_name] = f"ERROR_{status_code}"
                print(f"    {mp_name}: ERROR {status_code}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    for sku in all_skus:
        r = results.get(sku, {})
        if isinstance(r, dict) and r.get("all"):
            print(f"\n  {sku}: {r['all']}")
            continue
        print(f"\n  {sku}:")
        for mp_name in MARKETPLACES:
            status = r.get(mp_name, "?")
            marker = "+" if status == "ACCEPTED" else "-" if status == "NOT_FOUND" else "!" if "ERROR" in str(status) else " "
            print(f"    [{marker}] {mp_name}: {status}")

    # Count
    accepted = sum(1 for sku, r in results.items() if isinstance(r, dict)
                   for mp, s in r.items() if s == "ACCEPTED")
    not_found = sum(1 for sku, r in results.items() if isinstance(r, dict)
                    for mp, s in r.items() if s == "NOT_FOUND")
    errors = sum(1 for sku, r in results.items() if isinstance(r, dict)
                 for mp, s in r.items() if "ERROR" in str(s))

    print(f"\n  TOTAL: {accepted} accepted, {not_found} not found, {errors} errors")
    print("=" * 80)

    # Save
    output_path = '/Users/alexanderrogalski/nesell-analytics/scripts/image_upload_results.json'
    with open(output_path, 'w') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "mode": "dry_run" if dry_run else "live",
            "variant_images": variant_images,
            "results": results,
        }, f, indent=2)
    print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()
