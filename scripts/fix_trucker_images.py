#!/usr/bin/env python3.11
"""
Fix the trucker MAGA cap (PFT-90229846) images.
The Printful S3 URLs return 403, so we need to:
1. Generate new mockup images via Printful API
2. Upload them to Amazon

Printful Mockup Generator flow:
1. Create mockup task for the sync product
2. Poll until ready
3. Get permanent mockup URLs
4. PATCH to Amazon
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
BL_TOKEN = load_env(KEYS_DIR / "baselinker.env").get("BASELINKER_API_TOKEN", "")
PRINTFUL_TOKEN = load_env(KEYS_DIR / "printful.env").get("PRINTFUL_API_TOKEN", "")
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

pf_headers = {
    "Authorization": f"Bearer {PRINTFUL_TOKEN}",
    "X-PF-Store-Id": "15269225",
}

# Step 1: Find the trucker cap in Printful
print("Step 1: Finding trucker cap in Printful...")

# List all products
all_pf = []
offset = 0
while True:
    r = requests.get("https://api.printful.com/store/products",
                      headers=pf_headers,
                      params={"limit": 100, "offset": offset},
                      timeout=30)
    if r.status_code != 200:
        print(f"  Error: {r.status_code} {r.text[:200]}")
        break
    products = r.json().get("result", [])
    all_pf.extend(products)
    if len(products) < 100:
        break
    offset += 100
    time.sleep(0.5)

print(f"  Total Printful products: {len(all_pf)}")

trucker = None
for pp in all_pf:
    name = pp.get("name", "")
    ext_id = str(pp.get("external_id", ""))
    if "trucker" in name.lower() and ("america" in name.lower() or "great again" in name.lower()):
        trucker = pp
        print(f"  Found trucker: [{pp['id']}] {name}")
        break
    if "90229846" in ext_id:
        trucker = pp
        print(f"  Found by ext_id: [{pp['id']}] {name}")
        break

# Also search by name pattern
if not trucker:
    for pp in all_pf:
        name = pp.get("name", "")
        if "MAGA" in name.upper() or ("MAKE" in name.upper() and "GREAT" in name.upper() and "TRUCKER" in name.lower()):
            trucker = pp
            print(f"  Found by MAGA pattern: [{pp['id']}] {name}")
            break

if not trucker:
    # List all products with "great" in name
    print("\n  All 'Great Again' products in Printful:")
    for pp in all_pf:
        name = pp.get("name", "")
        if "great again" in name.lower() or "mega" in name.lower():
            print(f"    [{pp['id']}] {name} (thumb: {pp.get('thumbnail_url', '?')[:60]})")

if trucker:
    # Step 2: Get product details with files
    print(f"\nStep 2: Getting trucker details (ID={trucker['id']})...")
    r = requests.get(f"https://api.printful.com/store/products/{trucker['id']}",
                      headers=pf_headers, timeout=30)
    if r.status_code == 200:
        detail = r.json().get("result", {})
        sp = detail.get("sync_product", {})
        print(f"  Product: {sp.get('name', '?')}")
        print(f"  Thumbnail: {sp.get('thumbnail_url', '?')}")

        variants = detail.get("sync_variants", [])
        print(f"  Variants: {len(variants)}")

        # Collect all file URLs
        all_urls = set()
        for v in variants:
            for f in v.get("files", []):
                url = f.get("preview_url") or f.get("thumbnail_url", "")
                if url:
                    all_urls.add(url)
                    ftype = f.get("type", "?")
                    # Check accessibility
                    try:
                        rr = requests.head(url, timeout=5, allow_redirects=True)
                        print(f"    [{ftype}] {rr.status_code}: {url[:100]}")
                    except:
                        print(f"    [{ftype}] ERR: {url[:100]}")

        # Step 3: Try to create mockup task
        print(f"\nStep 3: Generating mockups...")
        # Get the Printful product ID (catalog product, not sync product)
        if variants:
            v0 = variants[0]
            catalog_variant_id = v0.get("product", {}).get("variant_id")
            catalog_product_id = v0.get("product", {}).get("product_id")
            print(f"  Catalog product_id: {catalog_product_id}, variant_id: {catalog_variant_id}")

            # Get mockup templates
            if catalog_product_id:
                r = requests.get(f"https://api.printful.com/mockup-generator/templates/{catalog_product_id}",
                                  headers=pf_headers, timeout=30)
                if r.status_code == 200:
                    templates = r.json().get("result", {})
                    variant_mapping = templates.get("variant_mapping", [])
                    template_list = templates.get("templates", [])
                    print(f"  Templates: {len(template_list)}")
                    for t in template_list[:5]:
                        print(f"    [{t.get('template_id')}] {t.get('placement')}: {t.get('image_url', '?')[:80]}")

                    # Create mockup generation task
                    # Need the print file URL
                    print_files = []
                    for v in variants:
                        for f in v.get("files", []):
                            if f.get("type") == "default" or f.get("type") == "front":
                                print_files.append(f)

                    if print_files:
                        print_file = print_files[0]
                        print(f"\n  Using print file: {print_file.get('preview_url', '?')[:80]}")

                        # Create mockup task
                        mockup_body = {
                            "variant_ids": [catalog_variant_id],
                            "files": [{
                                "placement": "front",
                                "image_url": print_file.get("preview_url", ""),
                            }],
                            "format": "jpg",
                        }

                        r = requests.post(
                            f"https://api.printful.com/mockup-generator/create-task/{catalog_product_id}",
                            headers={**pf_headers, "Content-Type": "application/json"},
                            json=mockup_body,
                            timeout=30,
                        )
                        print(f"  Mockup task creation: {r.status_code}")
                        if r.status_code == 200:
                            task = r.json().get("result", {})
                            task_key = task.get("task_key", "")
                            print(f"  Task key: {task_key}")

                            # Poll for result
                            for i in range(30):
                                time.sleep(5)
                                r = requests.get(
                                    f"https://api.printful.com/mockup-generator/task?task_key={task_key}",
                                    headers=pf_headers, timeout=30,
                                )
                                if r.status_code == 200:
                                    result = r.json().get("result", {})
                                    status = result.get("status", "")
                                    print(f"  Poll {i+1}: {status}")
                                    if status == "completed":
                                        mockups = result.get("mockups", [])
                                        print(f"  Mockups generated: {len(mockups)}")
                                        for m in mockups:
                                            print(f"    {m.get('placement', '?')}: {m.get('mockup_url', '?')}")
                                            for extra in m.get("extra", []):
                                                print(f"      extra: {extra.get('url', '?')[:100]}")
                                        break
                                    elif status == "failed":
                                        print(f"  Mockup task failed: {result}")
                                        break
                        else:
                            print(f"  Mockup task error: {r.text[:300]}")
                else:
                    print(f"  Templates error: {r.status_code} {r.text[:200]}")
else:
    print("\n  Trucker cap NOT found in Printful!")
    print("  Checking if the files.cdn.printful.com URL still works...")
    url = "https://files.cdn.printful.com/upload/product-templates/36/36f0028c58df72983b5375c47451a598_l"
    r = requests.head(url, timeout=5, allow_redirects=True)
    print(f"  CDN URL: {r.status_code}")
    if r.status_code == 200:
        print(f"  Content-Type: {r.headers.get('Content-Type')}")

# Step 4: Check current DE listing status after our earlier PATCH
print(f"\nStep 4: Check DE listing for trucker cap...")

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

r = requests.get(
    f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/PFT-90229846",
    headers=amz_headers,
    params={"marketplaceIds": "A1PA6795UKMFR9", "issueLocale": "en_US",
            "includedData": "attributes,issues,summaries"},
    timeout=30,
)
listing = r.json()
print(f"  HTTP {r.status_code}")
attrs = listing.get("attributes", {})
main = attrs.get("main_product_image_locator", [])
print(f"  main_product_image_locator: {json.dumps(main)[:200]}")

summaries = listing.get("summaries", [])
if summaries:
    main_img = summaries[0].get("mainImage", {})
    print(f"  mainImage from summaries: {main_img}")

issues = listing.get("issues", [])
print(f"  Issues: {len(issues)}")
for iss in issues[:5]:
    print(f"    [{iss.get('severity')}] {iss.get('message', '')[:120]}")
