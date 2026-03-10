#!/usr/bin/env python3.11
"""
Find working image URLs for MEGA products.
Check:
1. Baselinker storage URLs (storage.baselinker.com)
2. Printful CDN URLs (files.cdn.printful.com)
3. Printful upload S3 URLs (may need re-generating)
4. External storage images from Baselinker
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

bl_session = requests.Session()
bl_session.headers["X-BLToken"] = BL_TOKEN

def bl_call(method, params=None):
    time.sleep(0.65)
    resp = bl_session.post("https://api.baselinker.com/connector.php", data={
        "method": method,
        "parameters": json.dumps(params or {})
    })
    return resp.json()

# Step 1: Find all MEGA products
print("Step 1: Finding all MEGA products...")
all_pft = {}
page = 1
while True:
    data = bl_call("getInventoryProductsList", {"inventory_id": 52954, "page": page})
    batch = data.get("products", {})
    for pid, pd in batch.items():
        if pd.get("sku", "").startswith("PFT-"):
            all_pft[pid] = pd
    if len(batch) < 1000: break
    page += 1

# Get details
pft_ids = list(all_pft.keys())
details = {}
for i in range(0, len(pft_ids), 100):
    batch = pft_ids[i:i+100]
    d = bl_call("getInventoryProductsData", {
        "inventory_id": 52954, "products": [int(x) for x in batch]
    })
    details.update(d.get("products", {}))

# Find MEGA products
mega_skus = {}
for pid, pd in details.items():
    sku = pd.get("sku", "")
    tf = pd.get("text_fields", {})
    names = ""
    if isinstance(tf, dict):
        for k, v in tf.items():
            if k.startswith("name") and isinstance(v, str):
                names += " " + v
    if "MEGA" in sku.upper() or "GREAT AGAIN" in names.upper():
        mega_skus[sku] = pd

print(f"Found {len(mega_skus)} MEGA products")

# Step 2: Analyze image URL patterns
print("\nStep 2: Analyzing image URL patterns...")
url_patterns = {}
for sku, pd in mega_skus.items():
    images = pd.get("images", {})
    for k, url in images.items():
        if not url or not isinstance(url, str): continue
        # Extract domain
        try:
            domain = url.split("//")[1].split("/")[0]
        except:
            domain = "unknown"
        url_patterns[domain] = url_patterns.get(domain, 0) + 1

print("URL domain distribution:")
for domain, count in sorted(url_patterns.items(), key=lambda x: -x[1]):
    print(f"  {domain}: {count}")

# Step 3: Check if we can get images from Printful API
print("\nStep 3: Checking Printful API for product mockups...")

if PRINTFUL_TOKEN:
    print(f"  Printful token: {PRINTFUL_TOKEN[:10]}...")
    # Try to get sync products
    pf_headers = {"Authorization": f"Bearer {PRINTFUL_TOKEN}"}

    # List sync products
    r = requests.get("https://api.printful.com/store/products", headers=pf_headers,
                      params={"limit": 100, "offset": 0}, timeout=30)
    if r.status_code == 200:
        pf_data = r.json()
        pf_products = pf_data.get("result", [])
        print(f"  Printful store products: {len(pf_products)}")

        # Find MEGA products in Printful
        mega_pf = []
        for pp in pf_products:
            name = pp.get("name", "")
            if "MEGA" in name.upper() or "GREAT AGAIN" in name.upper():
                mega_pf.append(pp)
                print(f"    [{pp.get('id')}] {name[:80]}")
                # Get product details with mockup images
                r2 = requests.get(f"https://api.printful.com/store/products/{pp['id']}",
                                  headers=pf_headers, timeout=30)
                if r2.status_code == 200:
                    pp_detail = r2.json().get("result", {})
                    pp_variants = pp_detail.get("sync_variants", [])
                    pp_product = pp_detail.get("sync_product", {})

                    # Check product thumbnail
                    thumb = pp_product.get("thumbnail_url", "")
                    if thumb:
                        r3 = requests.head(thumb, timeout=5, allow_redirects=True)
                        status = r3.status_code
                        print(f"      thumbnail: {status} {thumb[:80]}")

                    # Check variant mockup files
                    if pp_variants:
                        v = pp_variants[0]
                        files = v.get("files", [])
                        for f in files[:3]:
                            furl = f.get("preview_url") or f.get("thumbnail_url", "")
                            if furl:
                                r3 = requests.head(furl, timeout=5, allow_redirects=True)
                                print(f"      file [{f.get('type')}]: {r3.status_code} {furl[:80]}")
                time.sleep(0.3)

        if not mega_pf:
            print("  No MEGA products found in Printful store")
    else:
        print(f"  Printful API error: {r.status_code} {r.text[:200]}")
else:
    print("  No Printful token available")

# Step 4: Check Baselinker external storages for images
print("\nStep 4: Checking Baselinker external storages...")
storages = bl_call("getExternalStoragesList")
print(f"  External storages: {json.dumps(storages.get('storages', []), indent=2)[:1000]}")

# Step 5: Check current Amazon images
print("\nStep 5: Checking what images Amazon currently has...")

def get_amz_token():
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": AMZ_CREDS["refresh_token"],
        "client_id": AMZ_CREDS["client_id"],
        "client_secret": AMZ_CREDS["client_secret"],
    })
    return r.json()["access_token"]

token = get_amz_token()
amz_headers = {"x-amz-access-token": token, "Content-Type": "application/json"}

# Check first 5 MEGA SKUs on DE
for sku in sorted(mega_skus.keys())[:5]:
    r = requests.get(
        f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{sku}",
        headers=amz_headers,
        params={"marketplaceIds": "A1PA6795UKMFR9", "issueLocale": "en_US",
                "includedData": "attributes,summaries"},
        timeout=30,
    )
    if r.status_code == 200:
        listing = r.json()
        attrs = listing.get("attributes", {})
        main = attrs.get("main_product_image_locator", [])
        others = []
        for i in range(1, 8):
            other = attrs.get(f"other_product_image_locator_{i}", [])
            if other:
                others.extend(other)

        main_url = main[0].get("media_location", "?") if main else "NONE"
        print(f"\n  {sku}:")
        print(f"    MAIN: {main_url}")
        print(f"    OTHERS: {len(others)}")
        for o in others:
            print(f"      {o.get('media_location', '?')[:100]}")

        # Also show productType
        pt = listing.get("productType", "")
        print(f"    productType: '{pt}'")
    else:
        print(f"\n  {sku}: HTTP {r.status_code}")
    time.sleep(0.3)

# Step 6: Try to generate mockup via Printful
print("\n\nStep 6: Attempting Printful mockup generation...")
if PRINTFUL_TOKEN:
    pf_headers = {"Authorization": f"Bearer {PRINTFUL_TOKEN}"}

    # List mockup templates for Dad Hat (product_id for classic-dad-hat)
    # Printful product ID 206 = Classic Dad Hat (Yupoong 6245CM)
    r = requests.get("https://api.printful.com/mockup-generator/templates/206",
                      headers=pf_headers, timeout=30)
    if r.status_code == 200:
        templates = r.json().get("result", {})
        print(f"  Mockup templates: {json.dumps(templates, indent=2)[:500]}")
    else:
        print(f"  Templates error: {r.status_code}")
