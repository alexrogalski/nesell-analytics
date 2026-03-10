#!/usr/bin/env python3.11
"""
Create offers on all 7 non-DE EU marketplaces for MEGA products that only exist on DE.
Also fix PFT-90229846 (trucker cap) which has 0 images.

Strategy:
- Use PUT with LISTING_OFFER_ONLY + merchant_suggested_asin
- Include images from DE listing
- Don't include shipping_group (let Amazon use default)
- For FBA products, set fulfillment_channel_code to AMAZON_NA/AMAZON_EU

For the trucker cap (PFT-90229846):
- First upload its image to DE, then create offers elsewhere
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

# Products that exist ONLY on DE and need offers on other marketplaces
DE_ONLY_SKUS = [
    "PFT-90201053",    # Make Sweden Great Again (flag)
    "PFT-90202876",    # Make America Great Again (flag)
    "PFT-93854576",    # Make France Great Again (flag)
    "PFT-93854948",    # Make Italy Great Again (flag)
    "PFT-93855132",    # Make Poland Great Again (flag)
    "PFT-93855391",    # Make Spain Great Again
    "PFT-93855556",    # Make Belgium Great Again (flag)
    "PFT-93855915",    # Make Netherlands Great Again (flag)
    "PFT-93856110",    # Make Great Britain Great Again (flag)
    "PFT-93856169",    # Make Great Britain Great Again
    "PFT-93856209",    # Make Netherlands Great Again
    "PFT-93856238",    # Make Belgium Great Again
    "PFT-93856317",    # Make France Great Again
]

# SKU with 0 images on DE
TRUCKER_SKU = "PFT-90229846"  # Only has 1 image in BL (CDN URL that works)
TRUCKER_IMAGE = "https://files.cdn.printful.com/upload/product-templates/36/36f0028c58df72983b5375c47451a598_l"


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
        self._token = r.json()["access_token"]
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

    def put_listing(self, sku, mp_id, body, retries=5):
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{requests.utils.quote(sku, safe='')}"
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


def extract_images(listing_data):
    """Extract images from listing attributes as dict {attr_name: url}."""
    attrs = listing_data.get("attributes", {})
    images = {}
    main = attrs.get("main_product_image_locator", [])
    if main:
        images["main_product_image_locator"] = main[0].get("media_location", "")
    for i in range(1, 8):
        key = f"other_product_image_locator_{i}"
        val = attrs.get(key, [])
        if val:
            images[key] = val[0].get("media_location", "")
    return images


def main():
    print("=" * 70)
    print("  CREATE EU OFFERS + IMAGE UPLOAD")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    amz = AmazonAPI(AMZ_CREDS)
    results = {"success": 0, "already_exists": 0, "errors": 0, "details": []}

    # ── Part A: Fix trucker cap (PFT-90229846) images on DE ──────────
    print("\n[Part A] Fix trucker cap PFT-90229846 images on DE")
    print("-" * 50)

    status, listing = amz.get_listing(TRUCKER_SKU, MARKETPLACE_IDS["DE"])
    if status == 200:
        summaries = listing.get("summaries", [])
        product_type = summaries[0].get("productType", "HAT") if summaries else "HAT"

        # PATCH the single image we have as MAIN
        patches = [{
            "op": "replace",
            "path": "/attributes/main_product_image_locator",
            "value": [{
                "media_location": TRUCKER_IMAGE,
                "marketplace_id": MARKETPLACE_IDS["DE"],
            }]
        }]

        ps, pr = amz.patch_listing(TRUCKER_SKU, MARKETPLACE_IDS["DE"], patches, product_type)
        resp_status = pr.get("status", "?")
        issues = pr.get("issues", [])
        errors_list = [i for i in issues if i.get("severity") == "ERROR"]

        if ps == 200 and resp_status in ("ACCEPTED", "VALID"):
            print(f"  DE: PATCHED main image -> {resp_status}")
            results["success"] += 1
        else:
            err = errors_list[0].get("message", "")[:100] if errors_list else f"HTTP_{ps}"
            print(f"  DE: PATCH failed: {resp_status} - {err}")
            print(f"  Full response: {json.dumps(pr, indent=2)[:500]}")
            results["errors"] += 1
    else:
        print(f"  DE: Listing not found (HTTP {status})")

    # ── Part B: Create offers on non-DE marketplaces ──────────────────
    print(f"\n[Part B] Creating offers on FR/ES/IT/NL/PL/SE/BE")
    print(f"  Products to process: {len(DE_ONLY_SKUS)}")
    print("-" * 50)

    NON_DE = {k: v for k, v in MARKETPLACE_IDS.items() if k != "DE"}

    for sku_idx, sku in enumerate(DE_ONLY_SKUS, 1):
        # Get DE listing data
        status, de_listing = amz.get_listing(sku, MARKETPLACE_IDS["DE"])
        time.sleep(0.25)

        if status != 200:
            print(f"\n  [{sku_idx}] {sku}: No DE listing (HTTP {status}), skip")
            continue

        summaries = de_listing.get("summaries", [])
        asin = summaries[0].get("asin", "") if summaries else ""
        product_type = summaries[0].get("productType", "HAT") if summaries else "HAT"
        title = summaries[0].get("itemName", "?")[:60] if summaries else "?"

        images = extract_images(de_listing)

        print(f"\n  [{sku_idx}/{len(DE_ONLY_SKUS)}] {sku} (ASIN={asin}, {len(images)} imgs)")
        print(f"    Title: {title}")

        if not asin:
            print(f"    SKIP: No ASIN")
            continue

        for country, mp_id in NON_DE.items():
            # First check if listing already exists
            check_status, check_data = amz.get_listing(sku, mp_id)
            time.sleep(0.2)

            if check_status == 200:
                # Listing already exists - check images
                existing_images = extract_images(check_data)
                if len(existing_images) >= 8:
                    results["already_exists"] += 1
                    continue
                elif len(existing_images) > 0:
                    # Has some but not all images - PATCH missing ones
                    patches = []
                    for attr_name, url in images.items():
                        if attr_name not in existing_images:
                            patches.append({
                                "op": "replace",
                                "path": f"/attributes/{attr_name}",
                                "value": [{"media_location": url, "marketplace_id": mp_id}]
                            })
                    if patches:
                        ps, pr = amz.patch_listing(sku, mp_id, patches, product_type)
                        rs = pr.get("status", "?")
                        if ps == 200 and rs in ("ACCEPTED", "VALID"):
                            print(f"    {country}: PATCHED {len(patches)} missing images -> {rs}")
                            results["success"] += 1
                        else:
                            iss = [i for i in pr.get("issues", []) if i.get("severity") == "ERROR"]
                            print(f"    {country}: PATCH failed: {rs} - {iss[0].get('message', '')[:80] if iss else ''}")
                            results["errors"] += 1
                    continue
                else:
                    # Exists but 0 images - PATCH all
                    patches = []
                    for attr_name, url in images.items():
                        patches.append({
                            "op": "replace",
                            "path": f"/attributes/{attr_name}",
                            "value": [{"media_location": url, "marketplace_id": mp_id}]
                        })
                    if patches:
                        ps, pr = amz.patch_listing(sku, mp_id, patches, product_type)
                        rs = pr.get("status", "?")
                        if ps == 200 and rs in ("ACCEPTED", "VALID"):
                            print(f"    {country}: PATCHED {len(patches)} images -> {rs}")
                            results["success"] += 1
                        else:
                            iss = [i for i in pr.get("issues", []) if i.get("severity") == "ERROR"]
                            print(f"    {country}: PATCH failed: {rs}")
                            results["errors"] += 1
                    continue

            # Listing doesn't exist - create offer-only with PUT
            # Minimal attributes for LISTING_OFFER_ONLY
            put_attrs = {
                "condition_type": [{"value": "new_new", "marketplace_id": mp_id}],
                "merchant_suggested_asin": [{"value": asin, "marketplace_id": mp_id}],
            }

            # Add images
            for attr_name, url in images.items():
                put_attrs[attr_name] = [{"media_location": url, "marketplace_id": mp_id}]

            body = {
                "productType": product_type,
                "requirements": "LISTING_OFFER_ONLY",
                "attributes": put_attrs,
            }

            ps, pr = amz.put_listing(sku, mp_id, body)
            time.sleep(0.3)

            rs = pr.get("status", "?")
            issues = pr.get("issues", [])
            errors_list = [i for i in issues if i.get("severity") == "ERROR"]
            warnings = [i for i in issues if i.get("severity") == "WARNING"]

            if ps == 200 and rs in ("ACCEPTED", "VALID"):
                print(f"    {country}: CREATED offer -> {rs}")
                results["success"] += 1
                results["details"].append({
                    "sku": sku, "marketplace": country, "action": "CREATED",
                    "status": "success", "asin": asin,
                })
            else:
                err_msg = errors_list[0].get("message", "")[:100] if errors_list else f"HTTP_{ps} {rs}"
                print(f"    {country}: PUT failed: {err_msg}")
                results["errors"] += 1
                results["details"].append({
                    "sku": sku, "marketplace": country, "action": "FAILED",
                    "error": err_msg, "full_response": pr,
                })

    # ── Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print(f"  Successful:      {results['success']}")
    print(f"  Already exists:  {results['already_exists']}")
    print(f"  Errors:          {results['errors']}")

    # Save
    rpath = Path(__file__).parent / "create_offers_results.json"
    with open(rpath, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Results saved to: {rpath}")

    if results["errors"]:
        print(f"\n  Error details:")
        for d in results["details"]:
            if d.get("action") == "FAILED":
                print(f"    {d['sku']} @ {d['marketplace']}: {d.get('error', '?')}")

    return results


if __name__ == "__main__":
    main()
