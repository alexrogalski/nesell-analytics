#!/usr/bin/env python3.11
"""
Inspect: List all MEGA (Make Great Again) products in Baselinker
and their image counts. Also check which SKUs exist on Amazon.
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

class BL:
    def __init__(self, token):
        self.s = requests.Session()
        self.s.headers["X-BLToken"] = token
        self._t = 0
    def call(self, method, params=None):
        elapsed = time.time() - self._t
        if elapsed < 0.65: time.sleep(0.65 - elapsed)
        r = self.s.post("https://api.baselinker.com/connector.php",
                        data={"method": method, "parameters": json.dumps(params or {})})
        self._t = time.time()
        d = r.json()
        if d.get("status") == "ERROR":
            if d.get("error_code") == "TOO_MANY_REQUESTS":
                time.sleep(60); return self.call(method, params)
            raise RuntimeError(f"BL: {d}")
        return d

class AMZ:
    def __init__(self, creds):
        self.c = creds; self._tok = None; self._tt = 0
    def _refresh(self):
        r = requests.post("https://api.amazon.com/auth/o2/token", data={
            "grant_type": "refresh_token", "refresh_token": self.c["refresh_token"],
            "client_id": self.c["client_id"], "client_secret": self.c["client_secret"]
        })
        self._tok = r.json()["access_token"]; self._tt = time.time()
    def _h(self):
        if not self._tok or time.time() - self._tt > 3000: self._refresh()
        return {"x-amz-access-token": self._tok, "Content-Type": "application/json"}
    def get_listing(self, sku, mp_id):
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{requests.utils.quote(sku, safe='')}"
        for attempt in range(3):
            try:
                r = requests.get(url, headers=self._h(),
                    params={"marketplaceIds": mp_id, "issueLocale": "en_US",
                            "includedData": "attributes,summaries"}, timeout=30)
            except: time.sleep(5); continue
            if r.status_code == 429: time.sleep(3); continue
            if r.status_code == 403: self._refresh(); continue
            return r.status_code, r.json() if r.text else {}
        return 0, {}

def is_permanent(url):
    if not url: return False
    return "X-Amz-Expires" not in url and "X-Amz-Credential" not in url and "Expires=" not in url

bl = BL(BL_TOKEN)
amz = AMZ(AMZ_CREDS)

# Get ALL PFT products
print("Fetching PFT products from Baselinker...")
all_pft = {}
page = 1
while True:
    data = bl.call("getInventoryProductsList", {"inventory_id": 52954, "page": page})
    batch = data.get("products", {})
    for pid, pd in batch.items():
        if pd.get("sku", "").startswith("PFT-"):
            all_pft[pid] = pd
    if len(batch) < 1000: break
    page += 1
print(f"Total PFT products: {len(all_pft)}")

# Get detailed data
print("Fetching details...")
pft_ids = list(all_pft.keys())
details = {}
for i in range(0, len(pft_ids), 100):
    batch = pft_ids[i:i+100]
    d = bl.call("getInventoryProductsData", {
        "inventory_id": 52954, "products": [int(x) for x in batch]
    })
    details.update(d.get("products", {}))
    print(f"  {len(details)}/{len(pft_ids)}")

# Find MEGA products
print("\n" + "="*70)
print("MEGA (Make Great Again) Products:")
print("="*70)

mega_families = {}
for pid, pd in details.items():
    sku = pd.get("sku", "")
    tf = pd.get("text_fields", {})
    names = []
    if isinstance(tf, dict):
        for k, v in tf.items():
            if k.startswith("name") and isinstance(v, str):
                names.append(v)
    all_names = " ".join(names)

    is_mega = "MEGA" in sku.upper() or "GREAT AGAIN" in all_names.upper()
    if not is_mega:
        continue

    parent_id = pd.get("parent_id", 0)
    family_key = str(parent_id) if parent_id and parent_id != 0 else str(pid)
    if family_key not in mega_families:
        mega_families[family_key] = []
    mega_families[family_key].append((pid, pd))

# Also add children of MEGA parents
for pid, pd in details.items():
    parent_id = pd.get("parent_id", 0)
    if str(parent_id) in mega_families and pid not in [x[0] for fam in mega_families.values() for x in fam]:
        mega_families[str(parent_id)].append((pid, pd))

print(f"\nFound {len(mega_families)} MEGA product families:")

for fkey, members in sorted(mega_families.items()):
    parent = details.get(fkey, details.get(str(fkey), {}))
    parent_sku = parent.get("sku", "?")
    tf = parent.get("text_fields", {})
    name = tf.get("name", tf.get("name|en", "?")) if isinstance(tf, dict) else "?"

    images = parent.get("images", {})
    img_urls = [v for k, v in sorted(images.items(), key=lambda x: int(x[0]))
                if v and isinstance(v, str) and v.startswith("http")]
    perm = sum(1 for u in img_urls if is_permanent(u))
    temp = len(img_urls) - perm

    print(f"\n  Family: {parent_sku}")
    print(f"  Name: {name[:80]}")
    print(f"  Images: {len(img_urls)} total ({perm} permanent, {temp} temporary)")
    print(f"  Members: {len(members)}")

    for img_url in img_urls[:3]:
        status = "PERM" if is_permanent(img_url) else "TEMP"
        print(f"    [{status}] {img_url[:120]}")
    if len(img_urls) > 3:
        print(f"    ... and {len(img_urls)-3} more")

    # List member SKUs
    for mpid, mpd in members:
        msku = mpd.get("sku", "?")
        m_images = mpd.get("images", {})
        m_count = len([v for v in m_images.values() if v and isinstance(v, str) and v.startswith("http")])
        is_parent = mpd.get("parent_id", 0) == 0
        role = "PARENT" if is_parent else "VARIANT"
        print(f"    {role}: {msku} ({m_count} images)")

# Quick Amazon check: do these SKUs exist on DE marketplace?
print("\n" + "="*70)
print("Amazon DE listing check (sample):")
print("="*70)

# Collect all SKUs
all_skus = set()
for fkey, members in mega_families.items():
    for mpid, mpd in members:
        sku = mpd.get("sku", "")
        if sku:
            all_skus.add(sku)

# Check first 10 on DE
checked = 0
for sku in sorted(all_skus)[:10]:
    status, data = amz.get_listing(sku, MARKETPLACE_IDS["DE"])
    summaries = data.get("summaries", [])
    title = summaries[0].get("itemName", "?")[:60] if summaries else "?"
    img_count = len(data.get("attributes", {}).get("item_images", []))
    print(f"  {sku}: HTTP {status}, images={img_count}, title={title}")
    time.sleep(0.3)
    checked += 1

print(f"\nTotal MEGA SKUs to process: {len(all_skus)}")
print(f"Families: {len(mega_families)}")
print(f"Marketplaces: {len(MARKETPLACE_IDS)} (DE, FR, IT, ES, NL, SE, PL, BE)")
print(f"Max operations: {len(all_skus) * len(MARKETPLACE_IDS)}")
