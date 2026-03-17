#!/usr/bin/env python3.11
"""
Scale POD to 20+ Allegro listings: t-shirts, hoodies, sweatshirts + existing products.

Strategy:
1. Create Printful sync products using existing embroidery design (file ID 784549948)
2. Wait for mockup generation
3. Create Allegro listings for:
   a) New products (t-shirts, hoodies, sweatshirts)
   b) Existing unlisted products (MAGA caps, tote bag, mug)

Margin analysis (EUR/PLN ~4.28):
- T-shirt Bella+Canvas 3001 emb: cost ~52 PLN → sell 79 PLN → 34% margin ✅
- Hoodie Gildan 18500 emb: cost ~94 PLN → sell 139 PLN → 32% margin ✅
- Sweatshirt Gildan 18000 emb: cost ~79 PLN → sell 129 PLN → 39% margin ✅
- White Glossy Mug: cost ~25 PLN → sell 49 PLN → 48% margin ✅
- Black Glossy Mug: cost ~35 PLN → sell 59 PLN → 40% margin ✅
"""

import httpx
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime

# ── Config ───────────────────────────────────────────────────────────────────

KEYS_DIR = Path.home() / ".keys"
RESULTS_DIR = Path(__file__).parent.parent / "data"


def load_env(path):
    vals = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip().strip("'\"")
    return vals


# Printful
_pf = load_env(KEYS_DIR / "printful.env")
PF_TOKEN = _pf.get("PRINTFUL_API_TOKEN_V2", _pf.get("PRINTFUL_API_TOKEN", ""))
PF_STORE_ID = _pf.get("PRINTFUL_STORE_ID", "15269225")

# Baselinker
_bl = load_env(KEYS_DIR / "baselinker.env")
BL_TOKEN = _bl.get("BASELINKER_API_TOKEN", "")
BL_PRINTFUL_INV_ID = 52954

# Allegro
_al = load_env(KEYS_DIR / "allegro.env")
ALLEGRO_CLIENT_ID = _al.get("ALLEGRO_CLIENT_ID", "")
ALLEGRO_CLIENT_SECRET = _al.get("ALLEGRO_CLIENT_SECRET", "")

# Existing embroidery design file ID from Dad hat sync product
EMBROIDERY_FILE_ID = 784549948

# ── Printful API ─────────────────────────────────────────────────────────────

PF_HEADERS = {
    "Authorization": f"Bearer {PF_TOKEN}",
    "X-PF-Store-Id": PF_STORE_ID,
    "Content-Type": "application/json",
}


def pf_get(path, params=None):
    for attempt in range(3):
        r = httpx.get(f"https://api.printful.com{path}", headers=PF_HEADERS, params=params or {}, timeout=30)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  [Rate limit] Waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code == 200:
            return r.json()
        print(f"  [PF] GET {path} -> {r.status_code}: {r.text[:200]}")
        return None
    return None


def pf_post(path, payload):
    for attempt in range(3):
        r = httpx.post(f"https://api.printful.com{path}", headers=PF_HEADERS, json=payload, timeout=60)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  [Rate limit] Waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code in (200, 201):
            return r.json()
        print(f"  [PF] POST {path} -> {r.status_code}: {r.text[:500]}")
        return None
    return None


# ── Baselinker API ───────────────────────────────────────────────────────────

def bl_call(method, params=None):
    r = httpx.post(
        "https://api.baselinker.com/connector.php",
        data={"method": method, "parameters": json.dumps(params or {})},
        headers={"X-BLToken": BL_TOKEN},
        timeout=30,
    )
    data = r.json()
    if data.get("status") == "ERROR":
        print(f"  [BL] {method}: {data.get('error_message', 'unknown')}")
    return data


# ── Allegro API ──────────────────────────────────────────────────────────────

def get_allegro_token():
    access_token = _al.get("ALLEGRO_ACCESS_TOKEN", "")
    expires_at = int(_al.get("ALLEGRO_TOKEN_EXPIRES_AT", "0"))
    if not access_token:
        raise RuntimeError("No Allegro access token")
    if time.time() >= expires_at:
        print("  Refreshing Allegro token...")
        r = httpx.post(
            "https://allegro.pl/auth/oauth/token",
            auth=(ALLEGRO_CLIENT_ID, ALLEGRO_CLIENT_SECRET),
            data={"grant_type": "refresh_token", "refresh_token": _al["ALLEGRO_REFRESH_TOKEN"]},
        )
        r.raise_for_status()
        tokens = r.json()
        env_file = KEYS_DIR / "allegro.env"
        lines = env_file.read_text().splitlines()
        keys_to_remove = {"ALLEGRO_ACCESS_TOKEN", "ALLEGRO_REFRESH_TOKEN", "ALLEGRO_TOKEN_EXPIRES_AT"}
        lines = [l for l in lines if not any(l.startswith(k) for k in keys_to_remove)]
        expires_at_new = int(time.time()) + tokens["expires_in"] - 60
        lines += [
            f"ALLEGRO_ACCESS_TOKEN='{tokens['access_token']}'",
            f"ALLEGRO_REFRESH_TOKEN='{tokens['refresh_token']}'",
            f"ALLEGRO_TOKEN_EXPIRES_AT='{expires_at_new}'",
        ]
        env_file.write_text("\n".join(lines) + "\n")
        env_file.chmod(0o600)
        return tokens["access_token"]
    return access_token


class Allegro:
    BASE = "https://api.allegro.pl"

    def __init__(self, token):
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.allegro.public.v1+json",
            "Content-Type": "application/vnd.allegro.public.v1+json",
        }
        self._last = 0.0

    def _wait(self):
        elapsed = time.time() - self._last
        if elapsed < 0.5:
            time.sleep(0.5 - elapsed)
        self._last = time.time()

    def get(self, path, params=None):
        self._wait()
        r = httpx.get(f"{self.BASE}{path}", headers=self.headers, params=params or {}, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path, data):
        self._wait()
        r = httpx.post(f"{self.BASE}{path}", headers=self.headers, content=json.dumps(data), timeout=30)
        if r.status_code >= 400:
            print(f"  [Allegro ERR {r.status_code}] {r.text[:500]}")
        r.raise_for_status()
        return r.json()

    def upload_image(self, source_url):
        self._wait()
        r = httpx.post(
            "https://upload.allegro.pl/sale/images",
            headers={
                "Authorization": self.headers["Authorization"],
                "Accept": "application/vnd.allegro.public.v1+json",
                "Content-Type": "application/vnd.allegro.public.v1+json",
            },
            content=json.dumps({"url": source_url}),
            timeout=30,
        )
        if r.status_code in (200, 201):
            data = r.json()
            return data.get("location") or data.get("url", source_url)
        # Fallback: binary upload
        self._wait()
        img = httpx.get(source_url, timeout=30, follow_redirects=True)
        img.raise_for_status()
        ext = source_url.rsplit(".", 1)[-1].split("?")[0].lower()
        mime = "image/png" if ext == "png" else "image/jpeg"
        r2 = httpx.post(
            "https://upload.allegro.pl/sale/images",
            headers={
                "Authorization": self.headers["Authorization"],
                "Accept": "application/vnd.allegro.public.v1+json",
                "Content-Type": mime,
            },
            content=img.content,
            timeout=60,
        )
        if r2.status_code >= 400:
            print(f"  [Upload ERR] {r2.status_code}: {r2.text[:200]}")
            return source_url
        return r2.json().get("url", source_url)


# ── Allegro parameter IDs ────────────────────────────────────────────────────
# Verified from existing hat script

OFFER_PARAMS_STAN = [
    {"id": "11323", "valuesIds": ["11323_1"]},  # Stan: Nowy
]

SHIPPING_RATE_ID = "7cbffa6c-0a40-4b44-93d2-db1a9ed56754"

# Allegro categories for new products
# Koszulki (T-shirts): category 261628 (Moda > Odzież > T-shirty)
# Bluzy z kapturem (Hoodies): category 261487 (Moda > Odzież > Bluzy)
# Bluzy bez kaptura (Sweatshirts): same category as hoodies
# Kubki (Mugs): category 259123 (Dom i Ogród > Kuchnia > Kubki)
# Torby (Bags): category 261683 (Moda > Torby)


# ── Product Definitions ──────────────────────────────────────────────────────

# Printful variant IDs for the products we want to create
PRINTFUL_NEW_PRODUCTS = [
    {
        "name": "Embroidered T-shirt",
        "sku_prefix": "PFT-TS",
        "catalog_product_id": 71,  # Bella+Canvas 3001
        "technique": "embroidery",
        "embroidery_placement": "embroidery_chest_left",
        "retail_price": "79.00",
        "target_price_pln": "79.00",
        "allegro_category": "261628",  # T-shirty
        "variants": [
            {"color": "Black", "size": "M", "variant_id": 4017},
            {"color": "Black", "size": "L", "variant_id": 4018},
            {"color": "Black", "size": "XL", "variant_id": 4019},
            {"color": "Black", "size": "2XL", "variant_id": 4020},
            {"color": "Navy", "size": "M", "variant_id": 4087},
            {"color": "Navy", "size": "L", "variant_id": 4088},
            {"color": "Navy", "size": "XL", "variant_id": 4089},
            {"color": "White", "size": "M", "variant_id": 4117},
            {"color": "White", "size": "L", "variant_id": 4118},
            {"color": "White", "size": "XL", "variant_id": 4119},
        ],
        "title_pl": "Koszulka T-shirt z Haftem Premium Bawełna Unisex",
        "description_pl": """<h2>Koszulka T-shirt z Haftem — Premium Bawełna</h2>
<ul>
<li><b>PREMIUM BAWEŁNA</b> – 100% czesana bawełna ring-spun, miękka i wygodna na co dzień</li>
<li><b>HAFT PREMIUM</b> – Wysokiej jakości haft na klatce piersiowej, trwały i elegancki</li>
<li><b>UNISEX KRÓJ</b> – Klasyczny, dopasowany krój dla kobiet i mężczyzn (S–2XL)</li>
<li><b>WIOSNA 2026</b> – Idealny na codzień, do pracy i na wyjście</li>
<li><b>PREZENT</b> – Świetny pomysł na prezent w eleganckiej jakości premium</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>""",
    },
    {
        "name": "Embroidered Hoodie",
        "sku_prefix": "PFT-HD",
        "catalog_product_id": 146,  # Gildan 18500
        "technique": "embroidery",
        "embroidery_placement": "embroidery_chest_left",
        "retail_price": "139.00",
        "target_price_pln": "139.00",
        "allegro_category": "261487",  # Bluzy
        "variants": [
            {"color": "Black", "size": "M", "variant_id": 5531},
            {"color": "Black", "size": "L", "variant_id": 5532},
            {"color": "Black", "size": "XL", "variant_id": 5533},
            {"color": "Black", "size": "2XL", "variant_id": 5534},
            {"color": "Navy", "size": "M", "variant_id": 5561},
            {"color": "Navy", "size": "L", "variant_id": 5562},
            {"color": "Navy", "size": "XL", "variant_id": 5563},
            {"color": "Dark Heather", "size": "M", "variant_id": 5545},
            {"color": "Dark Heather", "size": "L", "variant_id": 5546},
            {"color": "Dark Heather", "size": "XL", "variant_id": 5547},
        ],
        "title_pl": "Bluza z Kapturem Hoodie Haftowana Unisex Premium",
        "description_pl": """<h2>Bluza z Kapturem Hoodie — Haft Premium</h2>
<ul>
<li><b>CIEPŁA BLUZA</b> – Gruba mieszanka bawełna/poliester (50/50), idealna na chłodne dni</li>
<li><b>HAFT PREMIUM</b> – Elegancki haft na klatce piersiowej, trwały i odporny na pranie</li>
<li><b>KAPTUR + KIESZEŃ</b> – Podwójnie podszywany kaptur + kangurowa kieszeń na ręce</li>
<li><b>UNISEX</b> – Wygodny, luźny krój pasujący kobietom i mężczyznom (S–2XL)</li>
<li><b>NA PREZENT</b> – Idealna bluza na prezent, urodziny, imieniny</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>""",
    },
    {
        "name": "Embroidered Crewneck Sweatshirt",
        "sku_prefix": "PFT-SW",
        "catalog_product_id": 145,  # Gildan 18000
        "technique": "embroidery",
        "embroidery_placement": "embroidery_chest_left",
        "retail_price": "129.00",
        "target_price_pln": "129.00",
        "allegro_category": "261487",  # Bluzy
        "variants": [
            {"color": "Black", "size": "M", "variant_id": 5435},
            {"color": "Black", "size": "L", "variant_id": 5436},
            {"color": "Black", "size": "XL", "variant_id": 5437},
            {"color": "Black", "size": "2XL", "variant_id": 5438},
            {"color": "Navy", "size": "M", "variant_id": 5465},
            {"color": "Navy", "size": "L", "variant_id": 5466},
            {"color": "Navy", "size": "XL", "variant_id": 5467},
            {"color": "Sport Grey", "size": "M", "variant_id": 5487},
            {"color": "Sport Grey", "size": "L", "variant_id": 5488},
            {"color": "Sport Grey", "size": "XL", "variant_id": 5489},
        ],
        "title_pl": "Bluza Crewneck bez Kaptura Haftowana Unisex",
        "description_pl": """<h2>Bluza Crewneck bez Kaptura — Haft Premium</h2>
<ul>
<li><b>KLASYCZNY CREWNECK</b> – Bluza bez kaptura z okrągłym dekoltem, ponadczasowy styl</li>
<li><b>CIEPŁA</b> – Gruba mieszanka bawełna/poliester (50/50), idealna na wiosnę i jesień</li>
<li><b>HAFT PREMIUM</b> – Elegancki haft na klatce piersiowej, trwały i wyrazisty</li>
<li><b>UNISEX</b> – Luźny, wygodny krój dla kobiet i mężczyzn (S–2XL)</li>
<li><b>WIOSNA 2026</b> – Minimalistyczny styl idealny na co dzień i do biura</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>""",
    },
]

# Existing BL products to list on Allegro (have images, not yet listed)
EXISTING_BL_PRODUCTS = [
    {
        "bl_id": 519204219,
        "sku": "PFT-MUG-BLACK",
        "title_pl": "Kubek Ceramiczny Czarny z Nadrukiem 325ml Prezent",
        "price_pln": "49.00",
        "allegro_category": "259123",  # Kubki
        "description_pl": """<h2>Kubek Ceramiczny z Nadrukiem — Czarny Błyszczący 325ml</h2>
<ul>
<li><b>PREMIUM CERAMIKA</b> – Wysokiej jakości kubek ceramiczny 325ml (11oz), błyszcząca czarna glazura</li>
<li><b>NADRUK</b> – Trwały nadruk sublimacyjny odporny na mycie w zmywarce</li>
<li><b>NA PREZENT</b> – Idealny prezent na urodziny, imieniny, święta i Dzień Matki/Ojca</li>
<li><b>DO KAWY I HERBATY</b> – Klasyczny rozmiar na poranną kawę lub popołudniową herbatę</li>
<li><b>UNIKAT</b> – Każdy kubek drukowany na zamówienie, unikalny design</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>""",
    },
    {
        "bl_id": 519204274,
        "sku": "PFT-TOTE",
        "title_pl": "Torba Bawełniana Eko z Nadrukiem Zakupowa Unisex",
        "price_pln": "59.00",
        "allegro_category": "261683",  # Torby
        "description_pl": """<h2>Ekologiczna Torba Bawełniana z Nadrukiem</h2>
<ul>
<li><b>100% BAWEŁNA</b> – Ekologiczna torba z naturalnej bawełny, wielokrotnego użytku</li>
<li><b>NADRUK DTG</b> – Trwały nadruk bezpośrednio na tkaninie, kolory nie blakną</li>
<li><b>NA ZAKUPY</b> – Idealna na codzienne zakupy, plaże, uczelnię</li>
<li><b>POJEMNA</b> – Duża przestrzeń na zakupy, laptop, książki</li>
<li><b>EKO PREZENT</b> – Świetny pomysł na prezent dla osób dbających o środowisko</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>""",
    },
]

# MAGA-style caps to list on Allegro (already in BL with designs)
MAGA_CAPS_TO_LIST = [
    {"bl_id": 519193801, "sku": "PFT-MAGA-PL", "country": "Poland",
     "title_pl": "Czapka z Haftem Make Poland Great Again Dad Hat",
     "price_pln": "89.99"},
    {"bl_id": 519192693, "sku": "PFT-MAGA-FR", "country": "France",
     "title_pl": "Czapka z Haftem Make France Great Again Dad Hat",
     "price_pln": "89.99"},
    {"bl_id": 519189077, "sku": "PFT-MAGA-DE", "country": "Germany",
     "title_pl": "Czapka z Haftem Make Germany Great Again Dad Hat",
     "price_pln": "89.99"},
    {"bl_id": 519193402, "sku": "PFT-MAGA-IT", "country": "Italy",
     "title_pl": "Czapka z Haftem Make Italy Great Again Dad Hat",
     "price_pln": "89.99"},
    {"bl_id": 519195160, "sku": "PFT-MAGA-NL", "country": "Netherlands",
     "title_pl": "Czapka z Haftem Make Netherlands Great Again Dad Hat",
     "price_pln": "89.99"},
    {"bl_id": 519198378, "sku": "PFT-MAGA-ES", "country": "Spain",
     "title_pl": "Czapka z Haftem Make Spain Great Again Dad Hat",
     "price_pln": "89.99"},
    {"bl_id": 519195958, "sku": "PFT-MAGA-GB", "country": "Great Britain",
     "title_pl": "Czapka z Haftem Make Great Britain Great Again",
     "price_pln": "89.99"},
    {"bl_id": 519194697, "sku": "PFT-MAGA-BE", "country": "Belgium",
     "title_pl": "Czapka z Haftem Make Belgium Great Again Dad Hat",
     "price_pln": "89.99"},
    {"bl_id": 519192021, "sku": "PFT-MAGA-EU", "country": "Europe",
     "title_pl": "Czapka z Haftem Make Europe Great Again Dad Hat EU",
     "price_pln": "89.99"},
    {"bl_id": 519190349, "sku": "PFT-MAGA-US", "country": "America",
     "title_pl": "Czapka z Haftem Make America Great Again MAGA",
     "price_pln": "89.99"},
]


# ── Step 1: Create Printful sync products ────────────────────────────────────

def create_printful_products():
    """Create new Printful sync products for t-shirts, hoodies, sweatshirts."""
    print("\n" + "=" * 60)
    print("STEP 1: Creating Printful sync products")
    print("=" * 60)

    created = []
    for prod in PRINTFUL_NEW_PRODUCTS:
        print(f"\n  → Creating: {prod['name']} ({len(prod['variants'])} variants)")

        sync_variants = []
        for v in prod["variants"]:
            sync_variants.append({
                "variant_id": v["variant_id"],
                "retail_price": prod["retail_price"],
                "files": [
                    {
                        "type": prod["embroidery_placement"],
                        "id": EMBROIDERY_FILE_ID,
                    }
                ],
            })

        payload = {
            "sync_product": {
                "name": prod["name"],
                "thumbnail": None,
            },
            "sync_variants": sync_variants,
        }

        result = pf_post("/store/products", payload)
        if result and result.get("result"):
            sp = result["result"].get("sync_product", {})
            sp_id = sp.get("id", "?")
            print(f"    ✓ Created Printful sync product ID: {sp_id}")
            created.append({
                "printful_sync_id": sp_id,
                "name": prod["name"],
                "sku_prefix": prod["sku_prefix"],
                "variants": result["result"].get("sync_variants", []),
                "product_def": prod,
            })
        else:
            print(f"    ✗ Failed to create {prod['name']}")

        time.sleep(1)  # Respect rate limits

    return created


# ── Step 2: Get mockup images ────────────────────────────────────────────────

def get_mockup_images(created_products):
    """Wait for and retrieve mockup images for created products."""
    print("\n" + "=" * 60)
    print("STEP 2: Retrieving mockup images")
    print("=" * 60)

    images = {}
    for cp in created_products:
        sp_id = cp["printful_sync_id"]
        # Re-fetch to get preview URLs
        detail = pf_get(f"/store/products/{sp_id}")
        if not detail or not detail.get("result"):
            print(f"  ✗ Could not fetch details for sync product {sp_id}")
            continue

        sync_variants = detail["result"].get("sync_variants", [])
        # Get the first variant's preview image
        for sv in sync_variants:
            files = sv.get("files", [])
            for f in files:
                if f.get("type") == "preview" and f.get("preview_url"):
                    images[cp["sku_prefix"]] = f["preview_url"]
                    print(f"  ✓ {cp['name']}: {f['preview_url'][:60]}...")
                    break
            if cp["sku_prefix"] in images:
                break

        # Fallback: use the product's thumbnail
        if cp["sku_prefix"] not in images:
            sp = detail["result"].get("sync_product", {})
            thumb = sp.get("thumbnail_url", "")
            if thumb:
                images[cp["sku_prefix"]] = thumb
                print(f"  ✓ {cp['name']} (thumbnail): {thumb[:60]}...")
            else:
                # Use catalog image as last resort
                cat_id = cp["product_def"]["catalog_product_id"]
                cat_data = pf_get(f"/products/{cat_id}")
                if cat_data and cat_data.get("result"):
                    cat_img = cat_data["result"].get("product", {}).get("image", "")
                    if cat_img:
                        images[cp["sku_prefix"]] = cat_img
                        print(f"  ✓ {cp['name']} (catalog): {cat_img[:60]}...")
                time.sleep(0.3)

        time.sleep(0.5)

    return images


# ── Step 3: Get images for existing BL products ─────────────────────────────

def get_existing_product_images():
    """Get images from Baselinker for existing products."""
    print("\n" + "=" * 60)
    print("STEP 3: Getting images for existing BL products")
    print("=" * 60)

    images = {}

    # Get all BL product IDs
    bl_ids = [p["bl_id"] for p in EXISTING_BL_PRODUCTS + MAGA_CAPS_TO_LIST]

    # Fetch in batches of 10
    for i in range(0, len(bl_ids), 10):
        batch = bl_ids[i:i + 10]
        result = bl_call("getInventoryProductsData", {
            "inventory_id": BL_PRINTFUL_INV_ID,
            "products": batch,
        })
        if result.get("status") == "SUCCESS":
            for pid, pdata in result.get("products", {}).items():
                imgs = pdata.get("images", {})
                if imgs:
                    # Get first image URL
                    first_img = list(imgs.values())[0]
                    images[int(pid)] = first_img
                    sku = pdata.get("sku", "?")
                    print(f"  ✓ BL {pid} ({sku}): {first_img[:60]}...")
                else:
                    print(f"  ✗ BL {pid}: no images")
        time.sleep(0.7)

    return images


# ── Step 4: Create Allegro offers ────────────────────────────────────────────

def build_allegro_offer(title, category_id, price_pln, description_html, sku, allegro_image_url, product_params=None):
    """Build Allegro offer payload."""
    assert 12 <= len(title) <= 75, f"Title length {len(title)} out of range: {title}"

    # Default product params (generic)
    if product_params is None:
        product_params = [
            {"id": "248811", "valuesIds": ["248811_958954"]},  # Marka: bez marki
        ]

    return {
        "name": title,
        "category": {"id": category_id},
        "parameters": OFFER_PARAMS_STAN,
        "images": [allegro_image_url],
        "productSet": [
            {
                "product": {
                    "name": title,
                    "category": {"id": category_id},
                    "parameters": product_params,
                    "images": [allegro_image_url],
                }
            }
        ],
        "description": {
            "sections": [
                {"items": [{"type": "TEXT", "content": description_html}]},
                {"items": [{"type": "TEXT", "content": f"<p><b>SKU:</b> {sku}</p>"}]},
            ]
        },
        "sellingMode": {
            "format": "BUY_NOW",
            "price": {"amount": price_pln, "currency": "PLN"},
        },
        "stock": {"available": 999, "unit": "UNIT"},
        "delivery": {
            "shippingRates": {"id": SHIPPING_RATE_ID},
            "handlingTime": "PT96H",
        },
        "payments": {"invoice": "VAT"},
        "publication": {"status": "INACTIVE"},  # Draft — activate after review
        "language": "pl-PL",
    }


MAGA_CAP_DESCRIPTION = """<h2>Czapka z Haftem — Regulowana Dad Hat Unisex</h2>
<ul>
<li><b>WYSOKIEJ JAKOŚCI HAFT</b> – Trwały haft maszynowy, wyrazisty i odporny na blaknięcie</li>
<li><b>PREMIUM BAWEŁNA</b> – Miękka, oddychająca bawełniana tkanina twill</li>
<li><b>REGULOWANA</b> – Rozmiar uniwersalny z regulowanym paskiem — pasuje każdemu</li>
<li><b>DAD HAT TREND</b> – Klasyczny krój dad hat, nr 1 trend na wiosnę 2026</li>
<li><b>NA PREZENT</b> – Świetny pomysł na prezent dla kobiet i mężczyzn</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>"""

PRODUCT_PARAMS_CZAPKI = [
    {"id": "248811", "valuesIds": ["248811_958954"]},  # Marka: bez marki
    {"id": "54", "valuesIds": ["3806_16"]},             # Rozmiar: uniwersalny
    {"id": "249512", "valuesIds": ["249512_1647428"]},  # Kolor: wielokolorowy
    {"id": "3766", "valuesIds": ["3766_2397"]},         # Wzór: logo
    {"id": "203885", "valuesIds": ["203885_218329"]},   # Materiał: bawełna
]


def create_allegro_offers(api, new_product_images, existing_images):
    """Create all Allegro offers."""
    print("\n" + "=" * 60)
    print("STEP 4: Creating Allegro offers")
    print("=" * 60)

    results = {"created": [], "errors": []}

    # 4a. New products (t-shirts, hoodies, sweatshirts)
    print("\n  [4a] New Printful products...")
    for prod in PRINTFUL_NEW_PRODUCTS:
        prefix = prod["sku_prefix"]
        img_url = new_product_images.get(prefix)
        if not img_url:
            print(f"    ✗ {prefix}: no image, skipping")
            results["errors"].append({"sku": prefix, "error": "no image"})
            continue

        print(f"    Uploading image for {prefix}...")
        allegro_img = api.upload_image(img_url)

        try:
            offer = build_allegro_offer(
                title=prod["title_pl"],
                category_id=prod["allegro_category"],
                price_pln=prod["target_price_pln"],
                description_html=prod["description_pl"],
                sku=prefix,
                allegro_image_url=allegro_img,
            )
            resp = api.post("/sale/product-offers", offer)
            offer_id = resp.get("id", "?")
            status = resp.get("publication", {}).get("status", "?")
            print(f"    ✓ {prefix}: Offer ID {offer_id} ({status})")
            results["created"].append({
                "sku": prefix, "allegro_offer_id": offer_id,
                "price_pln": prod["target_price_pln"], "status": status,
                "title": prod["title_pl"], "type": "new_product",
            })
        except Exception as e:
            print(f"    ✗ {prefix}: {e}")
            results["errors"].append({"sku": prefix, "error": str(e)})

    # 4b. Existing products (mug, tote)
    print("\n  [4b] Existing BL products (mug, tote)...")
    for prod in EXISTING_BL_PRODUCTS:
        bl_id = prod["bl_id"]
        img_url = existing_images.get(bl_id)
        if not img_url:
            print(f"    ✗ {prod['sku']}: no image, skipping")
            results["errors"].append({"sku": prod["sku"], "error": "no image"})
            continue

        print(f"    Uploading image for {prod['sku']}...")
        allegro_img = api.upload_image(img_url)

        try:
            offer = build_allegro_offer(
                title=prod["title_pl"],
                category_id=prod["allegro_category"],
                price_pln=prod["price_pln"],
                description_html=prod["description_pl"],
                sku=prod["sku"],
                allegro_image_url=allegro_img,
            )
            resp = api.post("/sale/product-offers", offer)
            offer_id = resp.get("id", "?")
            status = resp.get("publication", {}).get("status", "?")
            print(f"    ✓ {prod['sku']}: Offer ID {offer_id} ({status})")
            results["created"].append({
                "sku": prod["sku"], "allegro_offer_id": offer_id,
                "price_pln": prod["price_pln"], "status": status,
                "title": prod["title_pl"], "type": "existing_product",
            })
        except Exception as e:
            print(f"    ✗ {prod['sku']}: {e}")
            results["errors"].append({"sku": prod["sku"], "error": str(e)})

    # 4c. MAGA caps
    print("\n  [4c] MAGA country caps...")
    for cap in MAGA_CAPS_TO_LIST:
        bl_id = cap["bl_id"]
        img_url = existing_images.get(bl_id)
        if not img_url:
            print(f"    ✗ {cap['sku']}: no image, skipping")
            results["errors"].append({"sku": cap["sku"], "error": "no image"})
            continue

        print(f"    Uploading image for {cap['sku']}...")
        allegro_img = api.upload_image(img_url)

        try:
            offer = build_allegro_offer(
                title=cap["title_pl"],
                category_id="5553",  # Czapki z daszkiem
                price_pln=cap["price_pln"],
                description_html=MAGA_CAP_DESCRIPTION,
                sku=cap["sku"],
                allegro_image_url=allegro_img,
                product_params=PRODUCT_PARAMS_CZAPKI,
            )
            resp = api.post("/sale/product-offers", offer)
            offer_id = resp.get("id", "?")
            status = resp.get("publication", {}).get("status", "?")
            print(f"    ✓ {cap['sku']}: Offer ID {offer_id} ({status})")
            results["created"].append({
                "sku": cap["sku"], "allegro_offer_id": offer_id,
                "price_pln": cap["price_pln"], "status": status,
                "title": cap["title_pl"], "type": "maga_cap",
            })
        except Exception as e:
            print(f"    ✗ {cap['sku']}: {e}")
            results["errors"].append({"sku": cap["sku"], "error": str(e)})

    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("ALLEGRO POD SCALING — 20+ Listings")
    print(f"Date: {datetime.now().isoformat()}")
    print("=" * 60)

    # Step 1: Create Printful sync products
    created_products = create_printful_products()
    print(f"\n  Printful products created: {len(created_products)}")

    # Step 2: Get mockup images for new products
    new_product_images = get_mockup_images(created_products)
    print(f"\n  New product images found: {len(new_product_images)}")

    # Step 3: Get images for existing BL products
    existing_images = get_existing_product_images()
    print(f"\n  Existing product images found: {len(existing_images)}")

    # Step 4: Create Allegro offers
    print("\n  Authenticating with Allegro...")
    allegro_token = get_allegro_token()
    api = Allegro(allegro_token)
    me = api.get("/me")
    print(f"  ✓ Allegro auth OK: {me.get('login', '?')}")

    results = create_allegro_offers(api, new_product_images, existing_images)

    # Save results
    results_file = RESULTS_DIR / "allegro_scaling_results.json"
    results_file.write_text(json.dumps(results, ensure_ascii=False, indent=2))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Created: {len(results['created'])} offers")
    print(f"  Errors:  {len(results['errors'])}")

    if results["created"]:
        print("\n  Created offers:")
        for r in results["created"]:
            print(f"    {r['sku']:25} | {r['allegro_offer_id']:>15} | {r['price_pln']:>8} PLN | {r['type']}")

    if results["errors"]:
        print("\n  Errors:")
        for e in results["errors"]:
            print(f"    {e['sku']}: {e['error'][:80]}")

    print(f"\n  Results saved to: {results_file}")
    print(f"\n  NOTE: All offers created as INACTIVE (drafts).")
    print(f"  Review in Allegro Seller Panel, then activate.")
    return results


if __name__ == "__main__":
    main()
