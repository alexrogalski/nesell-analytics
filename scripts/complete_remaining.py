#!/usr/bin/env python3.11
"""
Complete remaining tasks:
1. Create trucker cap (PFT-90229846) offers on non-DE marketplaces
2. Check/create SE offers for products that were skipped
3. Final verification across all marketplaces
"""

import requests
import json
import time
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

ALL_MEGA_SKUS = [
    "PFT-88471944-7854", "PFT-90034427", "PFT-90201053", "PFT-90202876",
    "PFT-90229846", "PFT-93854576", "PFT-93854948", "PFT-93855132",
    "PFT-93855391", "PFT-93855556", "PFT-93855915", "PFT-93856110",
    "PFT-93856169", "PFT-93856209", "PFT-93856238", "PFT-93856317",
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


def extract_images(listing_data):
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


def count_images(listing_data):
    return len(extract_images(listing_data))


amz = AmazonAPI(AMZ_CREDS)

# ── Part 1: Create trucker cap offers on non-DE ────────────────────

print("=" * 70)
print("Part 1: Trucker cap PFT-90229846 on non-DE marketplaces")
print("=" * 70)

# Get DE listing for ASIN
status, de_listing = amz.get_listing("PFT-90229846", MARKETPLACE_IDS["DE"])
if status == 200:
    summaries = de_listing.get("summaries", [])
    asin = summaries[0].get("asin", "") if summaries else ""
    pt = summaries[0].get("productType", "HAT") if summaries else "HAT"
    de_images = extract_images(de_listing)
    print(f"  ASIN: {asin}, productType: {pt}, DE images: {len(de_images)}")

    # After our PATCH in the previous run, DE should now have 1 image
    # Let's also use the CDN image URL
    trucker_image = "https://files.cdn.printful.com/upload/product-templates/36/36f0028c58df72983b5375c47451a598_l"

    for country, mp_id in MARKETPLACE_IDS.items():
        if country == "DE":
            continue

        check_status, _ = amz.get_listing("PFT-90229846", mp_id)
        time.sleep(0.2)

        if check_status == 200:
            print(f"  {country}: Already exists")
            continue

        # Create offer
        put_attrs = {
            "condition_type": [{"value": "new_new", "marketplace_id": mp_id}],
            "merchant_suggested_asin": [{"value": asin, "marketplace_id": mp_id}],
            "main_product_image_locator": [{"media_location": trucker_image, "marketplace_id": mp_id}],
        }

        body = {
            "productType": pt,
            "requirements": "LISTING_OFFER_ONLY",
            "attributes": put_attrs,
        }

        ps, pr = amz.put_listing("PFT-90229846", mp_id, body)
        rs = pr.get("status", "?")
        issues = [i for i in pr.get("issues", []) if i.get("severity") == "ERROR"]
        if ps == 200 and rs in ("ACCEPTED", "VALID"):
            print(f"  {country}: CREATED -> {rs}")
        else:
            err = issues[0].get("message", "")[:80] if issues else f"{rs}"
            print(f"  {country}: FAILED: {err}")
        time.sleep(0.3)

# ── Part 2: Scan ALL and create missing offers ────────────────────

print(f"\n{'='*70}")
print("Part 2: Full scan - find and fix any missing marketplace offers")
print("=" * 70)

missing = []
for sku in ALL_MEGA_SKUS:
    # Get DE data first
    de_status, de_data = amz.get_listing(sku, MARKETPLACE_IDS["DE"])
    time.sleep(0.2)

    if de_status != 200:
        print(f"  {sku}: No DE listing, skip")
        continue

    summaries = de_data.get("summaries", [])
    asin = summaries[0].get("asin", "") if summaries else ""
    pt = summaries[0].get("productType", "HAT") if summaries else "HAT"
    de_images = extract_images(de_data)

    row = f"  {sku:<25} "
    for country, mp_id in MARKETPLACE_IDS.items():
        if country == "DE":
            row += f"DE:OK "
            continue
        cs, cd = amz.get_listing(sku, mp_id)
        time.sleep(0.15)
        if cs == 200:
            ic = count_images(cd)
            row += f"{country}:{ic} "
        elif cs == 404:
            row += f"{country}:-- "
            missing.append((sku, country, mp_id, asin, pt, de_images))
        else:
            row += f"{country}:E{cs} "
    print(row)

if missing:
    print(f"\n  Creating {len(missing)} missing offers...")
    for sku, country, mp_id, asin, pt, images in missing:
        put_attrs = {
            "condition_type": [{"value": "new_new", "marketplace_id": mp_id}],
            "merchant_suggested_asin": [{"value": asin, "marketplace_id": mp_id}],
        }
        for attr_name, url in images.items():
            put_attrs[attr_name] = [{"media_location": url, "marketplace_id": mp_id}]

        body = {
            "productType": pt,
            "requirements": "LISTING_OFFER_ONLY",
            "attributes": put_attrs,
        }

        ps, pr = amz.put_listing(sku, mp_id, body)
        rs = pr.get("status", "?")
        issues = [i for i in pr.get("issues", []) if i.get("severity") == "ERROR"]
        if ps == 200 and rs in ("ACCEPTED", "VALID"):
            print(f"    {sku} @ {country}: CREATED -> {rs}")
        else:
            err = issues[0].get("message", "")[:80] if issues else f"{rs}"
            print(f"    {sku} @ {country}: FAILED: {err}")
        time.sleep(0.3)
else:
    print("\n  No missing offers found!")

# ── Part 3: Final verification ───────────────────────────────────

print(f"\n{'='*70}")
print("Part 3: Final verification")
print("=" * 70)
print(f"{'SKU':<25} " + " ".join(f"{c:>4}" for c in MARKETPLACE_IDS.keys()))
print("-" * 70)

for sku in ALL_MEGA_SKUS:
    row = f"{sku:<25} "
    for country, mp_id in MARKETPLACE_IDS.items():
        cs, cd = amz.get_listing(sku, mp_id)
        if cs == 200:
            ic = count_images(cd)
            row += f"  {ic:>2} "
        elif cs == 404:
            row += " N/A "
        else:
            row += f" E{cs:>2} "
        time.sleep(0.15)
    print(row)

print("\nDone!")
