#!/usr/bin/env python3.11
"""
Check image counts for all MEGA products across all 8 EU marketplaces.
"""

import requests
import json
import time
from pathlib import Path

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

# All 19 MEGA SKUs
MEGA_SKUS = [
    "PFT-82980216",    # Make Europe Great Again (EU flag)
    "PFT-88471944-7854", # Make Europe Great Again
    "PFT-90034427",    # Make Germany Great Again
    "PFT-90201053",    # Make Sweden Great Again (flag)
    "PFT-90202876",    # Make America Great Again (flag)
    "PFT-90229846",    # Make America Great Again (trucker)
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
    "PFT-93856269",    # Make Poland Great Again
    "PFT-93856295",    # Make Italy Great Again
    "PFT-93856317",    # Make France Great Again
]

def get_amz_token():
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": AMZ_CREDS["refresh_token"],
        "client_id": AMZ_CREDS["client_id"],
        "client_secret": AMZ_CREDS["client_secret"],
    })
    return r.json()["access_token"]

_state = {
    "token": None,
    "headers": None,
}

def refresh_amz():
    _state["token"] = get_amz_token()
    _state["headers"] = {"x-amz-access-token": _state["token"], "Content-Type": "application/json"}

refresh_amz()

def count_images(listing_data):
    """Count total images from listing attributes."""
    attrs = listing_data.get("attributes", {})
    count = 0
    main = attrs.get("main_product_image_locator", [])
    if main:
        count += 1
    for i in range(1, 8):
        other = attrs.get(f"other_product_image_locator_{i}", [])
        if other:
            count += 1
    return count

def get_listing(sku, mp_id, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(
                f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{requests.utils.quote(sku, safe='')}",
                headers=_state["headers"],
                params={"marketplaceIds": mp_id, "issueLocale": "en_US",
                        "includedData": "attributes,summaries"},
                timeout=30,
            )
        except:
            time.sleep(3)
            continue
        if r.status_code == 429:
            time.sleep(3)
            continue
        if r.status_code == 403:
            refresh_amz()
            continue
        return r.status_code, r.json() if r.text else {}
    return 0, {}

# Check all SKUs x all marketplaces
print("Checking all MEGA SKUs across all 8 EU marketplaces...")
print(f"{'SKU':<25} " + " ".join(f"{c:>4}" for c in MARKETPLACE_IDS.keys()))
print("-" * 70)

needs_images = []  # (sku, country, mp_id, current_count)

for sku in MEGA_SKUS:
    row = f"{sku:<25} "
    for country, mp_id in MARKETPLACE_IDS.items():
        status_code, data = get_listing(sku, mp_id)
        if status_code == 404:
            row += f" N/A "
        elif status_code == 200:
            img_count = count_images(data)
            row += f"  {img_count:>2} "
            if img_count < 8:
                needs_images.append((sku, country, mp_id, img_count))
        else:
            row += f" E{status_code:>2} "
        time.sleep(0.25)
    print(row)

print(f"\n{'='*70}")
print(f"Listings with <8 images: {len(needs_images)}")
for sku, country, mp_id, count in needs_images:
    print(f"  {sku} @ {country}: {count}/8 images")

# For products with <8 images on any marketplace, check if the DE version has the full set
# If so, we can copy the m.media-amazon.com URLs to the other marketplaces
print(f"\n{'='*70}")
print("Checking if DE has full images that can be replicated...")
for sku in MEGA_SKUS:
    status, data = get_listing(sku, MARKETPLACE_IDS["DE"])
    if status != 200:
        continue
    attrs = data.get("attributes", {})
    main = attrs.get("main_product_image_locator", [])
    if main:
        main_url = main[0].get("media_location", "")
        # Check if it's a media-amazon URL (permanent)
        is_amz = "m.media-amazon.com" in main_url
        print(f"  {sku}: MAIN is {'m.media-amazon.com (PERMANENT)' if is_amz else main_url[:60]}")
    time.sleep(0.25)
