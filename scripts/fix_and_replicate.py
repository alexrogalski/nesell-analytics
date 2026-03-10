#!/usr/bin/env python3.11
"""
1. Fix PFT-90229846 (trucker MAGA cap) - upload image
2. Check how to replicate DE listings to other marketplaces
3. For products only on DE, try to create offers on other marketplaces
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

BL_TOKEN = load_env(KEYS_DIR / "baselinker.env").get("BASELINKER_API_TOKEN", "")
AMZ_CREDS = json.loads((KEYS_DIR / "amazon-sp-api.json").read_text())
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"
PRINTFUL_TOKEN = load_env(KEYS_DIR / "printful.env").get("PRINTFUL_API_TOKEN", "")

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

_state = {"token": None, "headers": None}

def refresh_amz():
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": AMZ_CREDS["refresh_token"],
        "client_id": AMZ_CREDS["client_id"],
        "client_secret": AMZ_CREDS["client_secret"],
    })
    _state["token"] = r.json()["access_token"]
    _state["headers"] = {"x-amz-access-token": _state["token"], "Content-Type": "application/json"}

refresh_amz()

def amz_get(path, params, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(f"{AMZ_BASE}{path}", headers=_state["headers"],
                             params=params, timeout=30)
        except: time.sleep(3); continue
        if r.status_code == 429: time.sleep(3); continue
        if r.status_code == 403: refresh_amz(); continue
        return r.status_code, r.json() if r.text else {}
    return 0, {}

def amz_patch(path, body, params, retries=5):
    for attempt in range(retries):
        try:
            r = requests.patch(f"{AMZ_BASE}{path}", headers=_state["headers"],
                               json=body, params=params, timeout=30)
        except: time.sleep(5); continue
        if r.status_code == 429: time.sleep(min(3 * (2**attempt), 30)); continue
        if r.status_code == 403: refresh_amz(); continue
        return r.status_code, r.json() if r.text else {}
    return 0, {}

def amz_put(path, body, params, retries=5):
    for attempt in range(retries):
        try:
            r = requests.put(f"{AMZ_BASE}{path}", headers=_state["headers"],
                             json=body, params=params, timeout=30)
        except: time.sleep(5); continue
        if r.status_code == 429: time.sleep(min(3 * (2**attempt), 30)); continue
        if r.status_code == 403: refresh_amz(); continue
        return r.status_code, r.json() if r.text else {}
    return 0, {}


# ── Part 1: Fix PFT-90229846 (trucker MAGA cap) ────────────────────

print("="*70)
print("PART 1: Fix PFT-90229846 (Trucker MAGA cap) - currently 0 images")
print("="*70)

# Check Baselinker image
bl_session = requests.Session()
bl_session.headers["X-BLToken"] = BL_TOKEN

def bl_call(method, params=None):
    time.sleep(0.65)
    resp = bl_session.post("https://api.baselinker.com/connector.php", data={
        "method": method, "parameters": json.dumps(params or {})
    })
    return resp.json()

# Find product ID
page = 1
trucker_pid = None
while True:
    data = bl_call("getInventoryProductsList", {"inventory_id": 52954, "page": page})
    for pid, pd in data.get("products", {}).items():
        if pd.get("sku") == "PFT-90229846":
            trucker_pid = pid
            break
    if trucker_pid or len(data.get("products", {})) < 1000: break
    page += 1

if trucker_pid:
    data = bl_call("getInventoryProductsData", {
        "inventory_id": 52954, "products": [int(trucker_pid)]
    })
    pd = data["products"].get(str(trucker_pid), {})
    images = pd.get("images", {})
    print(f"\nBaselinker images for PFT-90229846:")
    for k, url in sorted(images.items(), key=lambda x: int(x[0])):
        if url:
            # Check accessibility
            try:
                r = requests.head(url, timeout=5, allow_redirects=True)
                status = r.status_code
            except:
                status = "ERR"
            print(f"  [{k}] {status} - {url}")

# Check Printful API for this product's mockups
print("\nChecking Printful for trucker cap mockups...")
if PRINTFUL_TOKEN:
    pf_headers = {"Authorization": f"Bearer {PRINTFUL_TOKEN}"}

    # Try with store_id
    r = requests.get("https://api.printful.com/store/products",
                      headers=pf_headers,
                      params={"limit": 100, "offset": 0, "store_id": "15269225"},
                      timeout=30)
    print(f"  Printful products list: HTTP {r.status_code}")
    if r.status_code == 200:
        products = r.json().get("result", [])
        for pp in products:
            name = pp.get("name", "")
            if "trucker" in name.lower() or "90229846" in str(pp.get("external_id", "")):
                print(f"  Found: [{pp.get('id')}] {name}")
                # Get details
                r2 = requests.get(f"https://api.printful.com/store/products/{pp['id']}",
                                  headers=pf_headers,
                                  params={"store_id": "15269225"},
                                  timeout=30)
                if r2.status_code == 200:
                    detail = r2.json().get("result", {})
                    sp = detail.get("sync_product", {})
                    print(f"    thumbnail: {sp.get('thumbnail_url', '?')}")
                    variants = detail.get("sync_variants", [])
                    for v in variants[:2]:
                        for f in v.get("files", []):
                            print(f"    file [{f.get('type')}]: {f.get('preview_url', '?')[:100]}")
    elif r.status_code == 400:
        # Try without store_id, use X-PF-Store-Id header
        r = requests.get("https://api.printful.com/store/products",
                          headers={**pf_headers, "X-PF-Store-Id": "15269225"},
                          params={"limit": 100, "offset": 0},
                          timeout=30)
        print(f"  Retry with X-PF-Store-Id: HTTP {r.status_code}")
        if r.status_code == 200:
            products = r.json().get("result", [])
            for pp in products:
                name = pp.get("name", "")
                if "trucker" in name.lower() or "MEGA" in name.upper() or "GREAT AGAIN" in name.upper():
                    print(f"  Found: [{pp.get('id')}] {name}")

# ── Part 2: Get DE listing details for products that need replication ──

print("\n" + "="*70)
print("PART 2: DE listings that need to be replicated to other marketplaces")
print("="*70)

# These products exist on DE but not on other marketplaces
DE_ONLY_SKUS = [
    "PFT-90201053",    # Make Sweden Great Again (flag)
    "PFT-90202876",    # Make America Great Again (flag)
    "PFT-90229846",    # Make America Great Again (trucker) -- 0 images
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

# Get full details from DE for first product to understand the format
print("\nSample DE listing (PFT-90201053):")
status, listing = amz_get(
    f"/listings/2021-08-01/items/{SELLER_ID}/PFT-90201053",
    {"marketplaceIds": "A1PA6795UKMFR9", "issueLocale": "en_US",
     "includedData": "attributes,issues,summaries,offers,fulfillmentAvailability"},
)
print(f"  HTTP {status}")
print(f"  productType: '{listing.get('productType', '')}'")
attrs = listing.get("attributes", {})
print(f"  Attribute keys: {sorted(attrs.keys())}")

# Print image attributes specifically
for key in sorted(attrs.keys()):
    if "image" in key.lower() or "locator" in key.lower():
        print(f"  {key}: {json.dumps(attrs[key])[:200]}")

# Print full listing JSON for reference
print(f"\n  Full listing (first 3000 chars):")
print(json.dumps(listing, indent=2)[:3000])

# ── Part 3: Try to create offer on FR for one product ────────────────

print("\n" + "="*70)
print("PART 3: Test - Create offer on FR for PFT-90201053")
print("="*70)

# For EU unified accounts, listings should auto-replicate via Amazon's Build International Listings (BIL)
# But if they're not showing up, we may need to use PUT to create the offer

# First, let's check if the listing exists on FR via a different approach
# Try GET with all marketplace IDs at once
status, data = amz_get(
    f"/listings/2021-08-01/items/{SELLER_ID}/PFT-90201053",
    {"marketplaceIds": ",".join(MARKETPLACE_IDS.values()), "issueLocale": "en_US",
     "includedData": "attributes,summaries,offers,fulfillmentAvailability"},
)
print(f"\nMulti-marketplace GET: HTTP {status}")
summaries = data.get("summaries", [])
print(f"  Summaries count: {len(summaries)}")
for s in summaries:
    mp = s.get("marketplaceId", "?")
    country = next((c for c, mid in MARKETPLACE_IDS.items() if mid == mp), mp)
    print(f"  {country}: status={s.get('status', '?')}, asin={s.get('asin', '?')}")

offers = data.get("offers", [])
print(f"  Offers count: {len(offers)}")
for o in offers:
    mp = o.get("marketplaceId", "?")
    country = next((c for c, mid in MARKETPLACE_IDS.items() if mid == mp), mp)
    print(f"  {country}: offerType={o.get('offerType', '?')}")

fa = data.get("fulfillmentAvailability", [])
print(f"  FulfillmentAvailability count: {len(fa)}")
for f in fa:
    print(f"  {f.get('fulfillmentChannelCode', '?')}: qty={f.get('quantity', '?')}")

# Also check what the issues are
issues = data.get("issues", [])
print(f"  Issues: {len(issues)}")
for iss in issues[:10]:
    print(f"  [{iss.get('severity')}] {iss.get('message', '')[:120]}")
