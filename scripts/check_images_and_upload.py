#!/usr/bin/env python3.11
"""
Quick check: verify image URLs are accessible, then check Amazon product type,
and do a test PATCH on one SKU/marketplace.
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

bl_session = requests.Session()
bl_session.headers["X-BLToken"] = BL_TOKEN

def bl_call(method, params=None):
    resp = bl_session.post("https://api.baselinker.com/connector.php", data={
        "method": method,
        "parameters": json.dumps(params or {})
    })
    return resp.json()

# Step 1: Find the right product IDs for PFT-90034427
print("1. Finding PFT-90034427 product ID in Baselinker...")
page = 1
target_pid = None
while True:
    data = bl_call("getInventoryProductsList", {"inventory_id": 52954, "page": page})
    batch = data.get("products", {})
    for pid, pd in batch.items():
        if pd.get("sku") == "PFT-90034427":
            target_pid = pid
            print(f"   Found: product_id={pid}, sku={pd.get('sku')}")
            break
    if target_pid or len(batch) < 1000:
        break
    page += 1

if not target_pid:
    print("   NOT FOUND!")
    exit(1)

# Step 2: Get full product data
print(f"\n2. Getting full product data for ID {target_pid}...")
data = bl_call("getInventoryProductsData", {
    "inventory_id": 52954,
    "products": [int(target_pid)]
})

products = data.get("products", {})
pd = products.get(str(target_pid), products.get(target_pid, {}))

if not pd:
    print(f"   No data returned! Keys: {list(products.keys())}")
    exit(1)

images = pd.get("images", {})
print(f"   SKU: {pd.get('sku')}")
print(f"   Images: {len(images)}")

image_urls = []
for k in sorted(images.keys(), key=lambda x: int(x)):
    url = images[k]
    if url and isinstance(url, str) and url.startswith("http"):
        image_urls.append(url)
        print(f"   [{k}] {url}")

# Step 3: Test URL accessibility
print(f"\n3. Testing image URL accessibility...")
for i, url in enumerate(image_urls[:3]):
    try:
        # Try GET instead of HEAD (S3 sometimes blocks HEAD)
        r = requests.get(url, timeout=10, allow_redirects=True, stream=True)
        ct = r.headers.get("Content-Type", "?")
        cl = r.headers.get("Content-Length", "?")
        print(f"   [{i+1}] GET status={r.status_code}, type={ct}, size={cl}")
        r.close()
    except Exception as e:
        print(f"   [{i+1}] Error: {e}")

# Step 4: Get Amazon listing
print(f"\n4. Getting Amazon listing for PFT-90034427 on DE...")

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

sku = "PFT-90034427"
mp_id = "A1PA6795UKMFR9"

r = requests.get(
    f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{sku}",
    headers=amz_headers,
    params={"marketplaceIds": mp_id, "issueLocale": "en_US",
            "includedData": "attributes,issues,summaries"},
    timeout=30,
)
listing = r.json()
print(f"   HTTP {r.status_code}")
product_type = listing.get("productType", "")
print(f"   productType: '{product_type}'")

summaries = listing.get("summaries", [])
if summaries:
    s = summaries[0]
    print(f"   Title: {s.get('itemName', '?')}")
    print(f"   ASIN: {s.get('asin', '?')}")

attrs = listing.get("attributes", {})
current_images = attrs.get("item_images", [])
print(f"   Current images: {len(current_images)}")

# Check main_product_image_locator too
main_img = attrs.get("main_product_image_locator", [])
print(f"   main_product_image_locator: {main_img}")
other_img = attrs.get("other_product_image_locator_1", [])
print(f"   other_product_image_locator_1: {other_img}")

# Print ALL attribute keys
print(f"   All attribute keys: {sorted(attrs.keys())}")

issues = listing.get("issues", [])
if issues:
    print(f"   Issues ({len(issues)}):")
    for iss in issues[:5]:
        print(f"     [{iss.get('severity')}] {iss.get('message', '')[:120]}")

# Step 5: Try PATCH with images
print(f"\n5. TEST PATCH: Adding 8 images for PFT-90034427 on DE...")

IMAGE_VARIANTS = ["MAIN", "PT01", "PT02", "PT03", "PT04", "PT05", "PT06", "PT07"]

image_values = []
for i, img_url in enumerate(image_urls[:8]):
    image_values.append({
        "media_location": img_url,
        "image_type": IMAGE_VARIANTS[i],
    })

print(f"   Sending {len(image_values)} images:")
for iv in image_values:
    print(f"     {iv['image_type']}: {iv['media_location'][:100]}...")

# Use productType from listing or fallback
if not product_type:
    product_type = "HEADWEAR"
    print(f"   Using fallback productType: {product_type}")

patch_body = {
    "productType": product_type,
    "patches": [{
        "op": "replace",
        "path": "/attributes/item_images",
        "value": image_values,
    }]
}

r = requests.patch(
    f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{sku}",
    headers=amz_headers,
    json=patch_body,
    params={"marketplaceIds": mp_id, "issueLocale": "en_US"},
    timeout=30,
)

print(f"\n   PATCH HTTP {r.status_code}")
try:
    resp_data = r.json()
except:
    resp_data = {"raw": r.text[:500]}

print(f"   Status: {resp_data.get('status', '?')}")

issues = resp_data.get("issues", [])
if issues:
    print(f"   Issues ({len(issues)}):")
    for iss in issues:
        print(f"     [{iss.get('severity')}] {iss.get('attributeNames', [])} {iss.get('message', '')[:120]}")

print(f"\n   Full response:\n{json.dumps(resp_data, indent=2)[:3000]}")
