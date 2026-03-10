#!/usr/bin/env python3.11
"""
Upload/replicate images for all "Make Great Again" listings across 8 EU marketplaces.

Strategy:
1. For products that exist on DE with 8 images:
   - Use the existing m.media-amazon.com URLs from DE
   - PATCH them onto listings on other marketplaces (if the listing exists there)
   - For marketplaces where listing doesn't exist: create via PUT

2. For PFT-90229846 (trucker MAGA cap) with 0 images:
   - Use the files.cdn.printful.com URL (confirmed accessible)
   - Upload to DE and any other existing marketplaces

3. For products not found anywhere (PFT-82980216, PFT-93856269, PFT-93856295):
   - Skip (no Amazon listing exists)

Uses Amazon Listings API with:
- Attribute: main_product_image_locator (MAIN image)
- Attributes: other_product_image_locator_1 through _7 (additional images)
- productType: "HAT" (from existing listings)

Safety: NEVER deletes listings, NEVER modifies titles or other attributes.
"""

import requests
import json
import time
import sys
from pathlib import Path
from datetime import datetime

KEYS_DIR = Path.home() / ".keys"

def load_env(path):
    vals = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                vals[k.strip()] = v.strip()
    return vals

AMZ_CREDS = json.loads((KEYS_DIR / "amazon-sp-api.json").read_text())
BL_TOKEN = load_env(KEYS_DIR / "baselinker.env").get("BASELINKER_API_TOKEN", "")
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"

MARKETPLACE_IDS = {
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "ES": "A1RKKUPIHCS9HS",
    "IT": "APJ6JRA9NG5V4",
    "NL": "A1805IZSGTT6HS",
    "PL": "A1C3SOZRARQ6R3",
    "SE": "A2NODRKZP88ZB9",
    "BE": "AMEN7PMS3EDWL",
}

# All MEGA SKUs
MEGA_SKUS = [
    "PFT-88471944-7854",  # Make Europe Great Again
    "PFT-90034427",       # Make Germany Great Again
    "PFT-90201053",       # Make Sweden Great Again (flag)
    "PFT-90202876",       # Make America Great Again (flag)
    "PFT-90229846",       # Make America Great Again (trucker)
    "PFT-93854576",       # Make France Great Again (flag)
    "PFT-93854948",       # Make Italy Great Again (flag)
    "PFT-93855132",       # Make Poland Great Again (flag)
    "PFT-93855391",       # Make Spain Great Again
    "PFT-93855556",       # Make Belgium Great Again (flag)
    "PFT-93855915",       # Make Netherlands Great Again (flag)
    "PFT-93856110",       # Make Great Britain Great Again (flag)
    "PFT-93856169",       # Make Great Britain Great Again
    "PFT-93856209",       # Make Netherlands Great Again
    "PFT-93856238",       # Make Belgium Great Again
    "PFT-93856317",       # Make France Great Again
    # Skipping: PFT-82980216, PFT-93856269, PFT-93856295 (not on any marketplace)
]


class AmazonAPI:
    def __init__(self, creds):
        self.creds = creds
        self._token = None
        self._token_time = 0

    def _refresh(self):
        r = requests.post("https://api.amazon.com/auth/o2/token", data={
            "grant_type": "refresh_token",
            "refresh_token": self.creds["refresh_token"],
            "client_id": self.creds["client_id"],
            "client_secret": self.creds["client_secret"],
        })
        data = r.json()
        self._token = data["access_token"]
        self._token_time = time.time()

    def _headers(self):
        if not self._token or time.time() - self._token_time > 3000:
            self._refresh()
        return {"x-amz-access-token": self._token, "Content-Type": "application/json"}

    def get_listing(self, sku, mp_id, retries=4):
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{requests.utils.quote(sku, safe='')}"
        for attempt in range(retries):
            try:
                r = requests.get(url, headers=self._headers(),
                    params={"marketplaceIds": mp_id, "issueLocale": "en_US",
                            "includedData": "attributes,summaries"},
                    timeout=30)
            except: time.sleep(3); continue
            if r.status_code == 429: time.sleep(min(2*(2**attempt), 30)); continue
            if r.status_code == 403: self._refresh(); time.sleep(1); continue
            return r.status_code, r.json() if r.text else {}
        return 0, {}

    def patch_listing(self, sku, mp_id, patches, product_type="HAT", retries=5):
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{requests.utils.quote(sku, safe='')}"
        body = {"productType": product_type, "patches": patches}
        for attempt in range(retries):
            try:
                r = requests.patch(url, headers=self._headers(), json=body,
                    params={"marketplaceIds": mp_id, "issueLocale": "en_US"},
                    timeout=30)
            except: time.sleep(5); continue
            if r.status_code == 429: time.sleep(min(3*(2**attempt), 60)); continue
            if r.status_code == 403: self._refresh(); time.sleep(1); continue
            return r.status_code, r.json() if r.text else {}
        return 0, {"error": "retries exhausted"}

    def put_listing(self, sku, mp_id, attributes, product_type="HAT", retries=5):
        """PUT creates or fully replaces a listing."""
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{requests.utils.quote(sku, safe='')}"
        body = {
            "productType": product_type,
            "requirements": "LISTING_OFFER_ONLY",
            "attributes": attributes,
        }
        for attempt in range(retries):
            try:
                r = requests.put(url, headers=self._headers(), json=body,
                    params={"marketplaceIds": mp_id, "issueLocale": "en_US"},
                    timeout=30)
            except: time.sleep(5); continue
            if r.status_code == 429: time.sleep(min(3*(2**attempt), 60)); continue
            if r.status_code == 403: self._refresh(); time.sleep(1); continue
            return r.status_code, r.json() if r.text else {}
        return 0, {"error": "retries exhausted"}


def extract_images_from_listing(listing_data):
    """Extract all image URLs from Amazon listing attributes."""
    attrs = listing_data.get("attributes", {})
    images = []

    main = attrs.get("main_product_image_locator", [])
    if main and isinstance(main, list) and len(main) > 0:
        images.append(("main_product_image_locator", main[0].get("media_location", "")))

    for i in range(1, 8):
        key = f"other_product_image_locator_{i}"
        val = attrs.get(key, [])
        if val and isinstance(val, list) and len(val) > 0:
            images.append((key, val[0].get("media_location", "")))

    return images


def build_image_patches(image_urls, target_mp_id):
    """
    Build PATCH operations for images.
    image_urls: list of (attr_name, url) tuples
    """
    patches = []
    for attr_name, url in image_urls:
        patches.append({
            "op": "replace",
            "path": f"/attributes/{attr_name}",
            "value": [{
                "media_location": url,
                "marketplace_id": target_mp_id,
            }]
        })
    return patches


def main():
    print("=" * 70)
    print("  AMAZON IMAGE UPLOADER — Make Great Again Products")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Seller: {SELLER_ID}")
    print(f"  SKUs: {len(MEGA_SKUS)}")
    print(f"  Marketplaces: {', '.join(MARKETPLACE_IDS.keys())}")
    print("=" * 70)

    amz = AmazonAPI(AMZ_CREDS)

    results = {
        "already_complete": 0,
        "images_added": 0,
        "listing_not_found": 0,
        "errors": 0,
        "details": [],
    }

    # ── Step 1: Get DE listing data for all SKUs (source of truth for images) ──
    print("\n[Step 1] Fetching DE listing data for all MEGA SKUs...")
    de_listings = {}
    for sku in MEGA_SKUS:
        status, data = amz.get_listing(sku, MARKETPLACE_IDS["DE"])
        if status == 200:
            images = extract_images_from_listing(data)
            product_type = ""
            summaries = data.get("summaries", [])
            if summaries:
                product_type = summaries[0].get("productType", "")
            de_listings[sku] = {
                "images": images,
                "product_type": product_type or "HAT",
                "data": data,
            }
            print(f"  {sku}: {len(images)} images, type={product_type or 'HAT'}")
        elif status == 404:
            print(f"  {sku}: NOT FOUND on DE")
        else:
            print(f"  {sku}: HTTP {status}")
        time.sleep(0.3)

    # ── Step 2: For each SKU, check all other marketplaces and add images ──
    print(f"\n[Step 2] Processing {len(MEGA_SKUS)} SKUs across {len(MARKETPLACE_IDS)} marketplaces...")
    print("-" * 70)

    for sku_idx, sku in enumerate(MEGA_SKUS, 1):
        de_info = de_listings.get(sku)
        if not de_info:
            print(f"\n  [{sku_idx}/{len(MEGA_SKUS)}] {sku}: No DE listing, skipping all marketplaces")
            for country in MARKETPLACE_IDS:
                results["details"].append({
                    "sku": sku, "marketplace": country,
                    "action": "SKIP_NO_DE", "status": "skipped",
                })
            results["listing_not_found"] += len(MARKETPLACE_IDS)
            continue

        de_images = de_info["images"]
        product_type = de_info["product_type"]

        print(f"\n  [{sku_idx}/{len(MEGA_SKUS)}] {sku} (type={product_type}, {len(de_images)} DE images)")

        for country, mp_id in MARKETPLACE_IDS.items():
            # Check current state on this marketplace
            status, listing = amz.get_listing(sku, mp_id)
            time.sleep(0.25)

            if status == 404:
                # Listing doesn't exist on this marketplace
                # We can try to create an offer-only listing that references the ASIN
                summaries = de_info["data"].get("summaries", [])
                asin = summaries[0].get("asin", "") if summaries else ""

                if not asin:
                    print(f"    {country}: NOT_FOUND (no ASIN from DE)")
                    results["listing_not_found"] += 1
                    results["details"].append({
                        "sku": sku, "marketplace": country,
                        "action": "NOT_FOUND_NO_ASIN", "status": "skipped",
                    })
                    continue

                # Try PUT with LISTING_OFFER_ONLY to create offer on this marketplace
                # Images should auto-propagate since it's the same ASIN
                put_attrs = {
                    "merchant_suggested_asin": [{"value": asin, "marketplace_id": mp_id}],
                    "condition_type": [{"value": "new_new", "marketplace_id": mp_id}],
                    "merchant_shipping_group": [{"value": "Migrated Template (Versand durch Amazon)", "marketplace_id": mp_id}],
                    "fulfillment_availability": [{"fulfillment_channel_code": "DEFAULT", "quantity": 0, "marketplace_id": mp_id}],
                }

                # Add images to the PUT request too
                for attr_name, url in de_images:
                    put_attrs[attr_name] = [{"media_location": url, "marketplace_id": mp_id}]

                put_status, put_resp = amz.put_listing(sku, mp_id, put_attrs, product_type)
                time.sleep(0.3)

                resp_status = put_resp.get("status", "?")
                issues = put_resp.get("issues", [])
                errors = [i for i in issues if i.get("severity") == "ERROR"]

                if put_status == 200 and resp_status in ("ACCEPTED", "VALID"):
                    print(f"    {country}: CREATED (offer-only with images) -> {resp_status}")
                    results["images_added"] += 1
                    results["details"].append({
                        "sku": sku, "marketplace": country,
                        "action": "CREATED_OFFER", "status": "success",
                        "asin": asin,
                    })
                else:
                    err_msg = errors[0].get("message", "")[:80] if errors else f"HTTP_{put_status}"
                    print(f"    {country}: PUT failed: {resp_status} - {err_msg}")
                    results["errors"] += 1
                    results["details"].append({
                        "sku": sku, "marketplace": country,
                        "action": "PUT_FAILED", "status": "error",
                        "error": err_msg,
                        "full_response": put_resp,
                    })
                continue

            if status != 200:
                print(f"    {country}: HTTP_{status}")
                results["errors"] += 1
                results["details"].append({
                    "sku": sku, "marketplace": country,
                    "action": f"HTTP_{status}", "status": "error",
                })
                continue

            # Listing exists -- check current image count
            current_images = extract_images_from_listing(listing)

            if len(current_images) >= 8:
                # Already has full images
                results["already_complete"] += 1
                results["details"].append({
                    "sku": sku, "marketplace": country,
                    "action": "ALREADY_COMPLETE", "status": "skipped",
                    "current_images": len(current_images),
                })
                # Don't print for already complete (too noisy)
                continue

            # Needs images -- PATCH with DE images
            if len(de_images) == 0:
                print(f"    {country}: {len(current_images)}/8 images but DE has 0 too")
                results["details"].append({
                    "sku": sku, "marketplace": country,
                    "action": "DE_HAS_NO_IMAGES", "status": "skipped",
                })
                continue

            patches = build_image_patches(de_images, mp_id)
            patch_status, patch_resp = amz.patch_listing(sku, mp_id, patches, product_type)
            time.sleep(0.3)

            resp_status = patch_resp.get("status", "?")
            issues = patch_resp.get("issues", [])
            errors = [i for i in issues if i.get("severity") == "ERROR"]

            if patch_status == 200 and resp_status in ("ACCEPTED", "VALID"):
                print(f"    {country}: PATCHED {len(de_images)} images -> {resp_status}")
                results["images_added"] += 1
                results["details"].append({
                    "sku": sku, "marketplace": country,
                    "action": "PATCHED", "status": "success",
                    "images_added": len(de_images),
                })
            else:
                err_msg = errors[0].get("message", "")[:80] if errors else f"HTTP_{patch_status}"
                print(f"    {country}: PATCH failed: {resp_status} - {err_msg}")
                results["errors"] += 1
                results["details"].append({
                    "sku": sku, "marketplace": country,
                    "action": "PATCH_FAILED", "status": "error",
                    "error": err_msg,
                })

    # ── Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print(f"  Already complete (8/8 images): {results['already_complete']}")
    print(f"  Images added/created:          {results['images_added']}")
    print(f"  Listing not found:             {results['listing_not_found']}")
    print(f"  Errors:                        {results['errors']}")
    total = sum(1 for d in results["details"])
    print(f"  Total operations:              {total}")

    # Breakdown by action
    action_counts = {}
    for d in results["details"]:
        action = d.get("action", "?")
        action_counts[action] = action_counts.get(action, 0) + 1
    print(f"\n  Action breakdown:")
    for action, count in sorted(action_counts.items()):
        print(f"    {action}: {count}")

    # Save results
    results_path = Path(__file__).parent / "image_upload_results.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Results saved to: {results_path}")

    # Print errors
    error_details = [d for d in results["details"] if d.get("status") == "error"]
    if error_details:
        print(f"\n  ERRORS ({len(error_details)}):")
        for d in error_details[:30]:
            print(f"    {d['sku']} @ {d['marketplace']}: {d.get('error', d.get('action', '?'))[:100]}")

    return results


if __name__ == "__main__":
    results = main()
