#!/usr/bin/env python3.11
"""
Fix trucker cap images. The CDN URL returns WebP, which Amazon might not accept.
Try different approaches:
1. Check if Printful CDN has JPEG version
2. Download image, re-upload to a public hosting
3. Try the original printful-upload S3 URLs (some might work)
4. Use Baselinker to host the image
"""

import requests
import json
import time
import base64
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

# Step 1: Check all Baselinker images for the trucker cap
print("Step 1: Get all Baselinker images for PFT-90229846...")
bl_session = requests.Session()
bl_session.headers["X-BLToken"] = BL_TOKEN

def bl_call(method, params=None):
    time.sleep(0.65)
    r = bl_session.post("https://api.baselinker.com/connector.php",
                        data={"method": method, "parameters": json.dumps(params or {})})
    return r.json()

# Find product
page = 1
target_pid = None
while True:
    data = bl_call("getInventoryProductsList", {"inventory_id": 52954, "page": page})
    for pid, pd in data.get("products", {}).items():
        if pd.get("sku") == "PFT-90229846":
            target_pid = pid
            break
    if target_pid or len(data.get("products", {})) < 1000: break
    page += 1

print(f"  Product ID: {target_pid}")

if target_pid:
    data = bl_call("getInventoryProductsData", {
        "inventory_id": 52954, "products": [int(target_pid)]
    })
    pd = data["products"].get(str(target_pid), {})
    images = pd.get("images", {})

    print(f"  Images in Baselinker: {len(images)}")
    for k, url in sorted(images.items(), key=lambda x: int(x[0])):
        if not url: continue
        try:
            r = requests.get(url, timeout=10, allow_redirects=True, stream=True)
            ct = r.headers.get("Content-Type", "?")
            cl = r.headers.get("Content-Length", "?")
            r.close()
            print(f"  [{k}] {r.status_code} type={ct} size={cl} - {url[:100]}")
        except Exception as e:
            print(f"  [{k}] ERR: {e} - {url[:100]}")

# Step 2: Try CDN URL with different suffixes
print("\nStep 2: Testing CDN URL variants...")
base_url = "https://files.cdn.printful.com/upload/product-templates/36/36f0028c58df72983b5375c47451a598"
suffixes = ["_l", "_l.jpg", "_l.png", ".jpg", ".png", "_preview"]

for suffix in suffixes:
    url = base_url + suffix
    try:
        r = requests.head(url, timeout=5, allow_redirects=True)
        ct = r.headers.get("Content-Type", "?")
        cl = r.headers.get("Content-Length", "?")
        print(f"  {suffix}: {r.status_code} type={ct} size={cl}")
    except:
        print(f"  {suffix}: ERROR")

# Step 3: Download the working image and upload via Baselinker
# Baselinker can store images and generate permanent URLs
print("\nStep 3: Downloading image and re-uploading to Baselinker...")

working_url = "https://files.cdn.printful.com/upload/product-templates/36/36f0028c58df72983b5375c47451a598_l"
r = requests.get(working_url, timeout=15)
print(f"  Downloaded: {r.status_code}, Content-Type: {r.headers.get('Content-Type')}, size: {len(r.content)}")

if r.status_code == 200:
    img_data = r.content
    img_b64 = base64.b64encode(img_data).decode("ascii")

    # Upload to Baselinker by updating the product image
    # Baselinker accepts base64 images in addInventoryProduct
    print(f"  Base64 size: {len(img_b64)}")

    # Update product with base64 image
    result = bl_call("addInventoryProduct", {
        "inventory_id": "52954",
        "product_id": str(target_pid),
        "images": {
            "0": f"data:{img_b64}"
        }
    })
    print(f"  Upload result: {json.dumps(result)[:300]}")

    # Re-fetch product to get the new URL
    time.sleep(2)
    data = bl_call("getInventoryProductsData", {
        "inventory_id": 52954, "products": [int(target_pid)]
    })
    pd = data["products"].get(str(target_pid), {})
    images = pd.get("images", {})
    print(f"\n  Updated images:")
    for k, url in sorted(images.items(), key=lambda x: int(x[0])):
        if not url: continue
        try:
            r = requests.head(url, timeout=5, allow_redirects=True)
            ct = r.headers.get("Content-Type", "?")
            print(f"  [{k}] {r.status_code} type={ct} - {url[:120]}")
        except:
            print(f"  [{k}] ERR - {url[:120]}")

    # Get the new URL
    new_url = images.get("1", "")
    if new_url:
        print(f"\n  New image URL: {new_url}")

        # Step 4: PATCH to Amazon DE
        print("\nStep 4: PATCHing to Amazon DE...")
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

        for country, mp_id in MARKETPLACE_IDS.items():
            patches = [{
                "op": "replace",
                "path": "/attributes/main_product_image_locator",
                "value": [{"media_location": new_url, "marketplace_id": mp_id}]
            }]

            body = {"productType": "HAT", "patches": patches}

            r = requests.patch(
                f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/PFT-90229846",
                headers=amz_headers,
                json=body,
                params={"marketplaceIds": mp_id, "issueLocale": "en_US"},
                timeout=30,
            )
            resp = r.json() if r.text else {}
            rs = resp.get("status", "?")
            issues = [i for i in resp.get("issues", []) if i.get("severity") == "ERROR"]
            err = issues[0].get("message", "")[:80] if issues else ""

            if r.status_code == 200 and rs in ("ACCEPTED", "VALID"):
                print(f"  {country}: PATCHED -> {rs}")
            else:
                print(f"  {country}: FAILED {rs} - {err}")
            time.sleep(0.3)
