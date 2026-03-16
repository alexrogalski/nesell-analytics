#!/usr/bin/env python3.11
"""
Create 5 PFT- hat listings on Allegro.pl (Spring Sale window — March 2026).

Verified:
  - Categories:  Czapki z daszkiem=5553  Kapelusze=5554
  - Shipping rate: 7cbffa6c-0a40-4b44-93d2-db1a9ed56754 (Cennik główny mały asortyment)
  - Images: Printful catalog images (public)
  - Parameters: hardcoded from /sale/categories/{id}/parameters

Rules:
  - No "nesell" in titles
  - No manufacturer model numbers in titles
  - Offers created as INACTIVE (drafts) — activate in Allegro Seller Panel after review
"""

import httpx
import json
import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

# ── Auth ──────────────────────────────────────────────────────────────────────

ENV_FILE = Path.home() / ".keys" / "allegro.env"
load_dotenv(ENV_FILE, override=True)

CLIENT_ID = os.environ["ALLEGRO_CLIENT_ID"]
CLIENT_SECRET = os.environ["ALLEGRO_CLIENT_SECRET"]
TOKEN_URL = "https://allegro.pl/auth/oauth/token"
BASE_URL = "https://api.allegro.pl"
RESULTS_FILE = Path(__file__).parent / "create_pft_hats_allegro_results.json"


def get_valid_token() -> str:
    access_token = os.environ.get("ALLEGRO_ACCESS_TOKEN")
    expires_at = int(os.environ.get("ALLEGRO_TOKEN_EXPIRES_AT", "0"))
    if not access_token:
        raise RuntimeError("No access token — run: python3.11 ~/allegro-mcp/auth.py")
    if time.time() >= expires_at:
        print("Token expired, refreshing...")
        r = httpx.post(
            TOKEN_URL,
            auth=(CLIENT_ID, CLIENT_SECRET),
            data={"grant_type": "refresh_token", "refresh_token": os.environ["ALLEGRO_REFRESH_TOKEN"]},
        )
        r.raise_for_status()
        tokens = r.json()
        lines = ENV_FILE.read_text().splitlines()
        keys_to_remove = {"ALLEGRO_ACCESS_TOKEN", "ALLEGRO_REFRESH_TOKEN", "ALLEGRO_TOKEN_EXPIRES_AT"}
        lines = [l for l in lines if not any(l.startswith(k) for k in keys_to_remove)]
        expires_at_new = int(time.time()) + tokens["expires_in"] - 60
        lines += [
            f"ALLEGRO_ACCESS_TOKEN={tokens['access_token']}",
            f"ALLEGRO_REFRESH_TOKEN={tokens['refresh_token']}",
            f"ALLEGRO_TOKEN_EXPIRES_AT={expires_at_new}",
        ]
        ENV_FILE.write_text("\n".join(lines) + "\n")
        ENV_FILE.chmod(0o600)
        return tokens["access_token"]
    return access_token


# ── Allegro client ─────────────────────────────────────────────────────────────

class Allegro:
    def __init__(self, token: str):
        self.token = token
        self.json_headers = {
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

    def get(self, path: str, params: dict = None) -> dict:
        self._wait()
        r = httpx.get(f"{BASE_URL}{path}", headers=self.json_headers, params=params or {}, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, data: dict) -> dict:
        self._wait()
        r = httpx.post(
            f"{BASE_URL}{path}",
            headers=self.json_headers,
            content=json.dumps(data),
            timeout=30,
        )
        if r.status_code >= 400:
            print(f"  [ERR {r.status_code}] {r.text[:800]}")
            r.raise_for_status()
        return r.json()

    def upload_image(self, source_url: str) -> str:
        """Upload image to Allegro (upload.allegro.pl). Returns Allegro-hosted URL.
        Tries URL-based upload first; falls back to binary if that fails."""
        self._wait()
        upload_base = "https://upload.allegro.pl"

        # Option 1: URL-based upload (Allegro downloads the image for us)
        r = httpx.post(
            f"{upload_base}/sale/images",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.allegro.public.v1+json",
                "Content-Type": "application/vnd.allegro.public.v1+json",
            },
            content=json.dumps({"url": source_url}),
            timeout=30,
        )
        if r.status_code in (200, 201):
            data = r.json()
            return data.get("location") or data.get("url", source_url)

        # Option 2: binary upload (download + re-upload raw bytes)
        self._wait()
        img = httpx.get(source_url, timeout=30, follow_redirects=True)
        img.raise_for_status()
        ext = source_url.rsplit(".", 1)[-1].split("?")[0].lower()
        mime = "image/png" if ext == "png" else "image/jpeg"

        r2 = httpx.post(
            f"{upload_base}/sale/images",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.allegro.public.v1+json",
                "Content-Type": mime,
            },
            content=img.content,
            timeout=60,
        )
        if r2.status_code >= 400:
            print(f"  [ERR upload {r2.status_code}] {r2.text[:300]}")
            r2.raise_for_status()
        data2 = r2.json()
        return data2.get("url") or source_url


# ── Hardcoded parameter IDs (verified 2026-03-16) ────────────────────────────
# Stan (11323) is offer-level (requiredForProduct=False)
# All others are product-level (requiredForProduct=True) → go in productSet[0].product.parameters

OFFER_PARAMS_STAN = [
    {"id": "11323", "valuesIds": ["11323_1"]},  # Stan: Nowy
]

PRODUCT_PARAMS_CZAPKI = [
    {"id": "248811", "valuesIds": ["248811_958954"]},  # Marka: bez marki
    {"id": "54", "valuesIds": ["3806_16"]},            # Rozmiar: uniwersalny
    {"id": "249512", "valuesIds": ["249512_1647428"]}, # Kolor: wielokolorowy
    {"id": "3766", "valuesIds": ["3766_2397"]},        # Wzór: logo
    {"id": "203885", "valuesIds": ["203885_218329"]},  # Materiał: bawełna
]

PRODUCT_PARAMS_KAPELUSZE = [
    {"id": "248811", "valuesIds": ["248811_958954"]},  # Marka: bez marki
    {"id": "4227", "valuesIds": ["4227_13"]},          # Rozmiar obwód: uniwersalny
    {"id": "249512", "valuesIds": ["249512_1647428"]}, # Kolor: wielokolorowy
    {"id": "203645", "valuesIds": ["203645_217605"]},  # Rodzaj: bucket
    {"id": "203885", "valuesIds": ["203885_218329"]},  # Materiał: bawełna
]

SHIPPING_RATE_ID = "7cbffa6c-0a40-4b44-93d2-db1a9ed56754"  # Cennik główny mały asortyment


# ── Product data ───────────────────────────────────────────────────────────────

PRODUCTS = [
    {
        "sku": "PFT-WDH",
        "printful_id": 961,
        "bl_product_id": 543455994,
        "price_pln": "99.00",
        "category_id": "5553",  # Czapki z daszkiem
        "product_params": PRODUCT_PARAMS_CZAPKI,
        "image_url": "https://files.cdn.printful.com/o/upload/product-catalog-img/d1/d12e9565c58d529b82b0e9731c8a1648_l",
        "title": "Prany Dad Hat Vintage Czapka z Daszkiem Regulowana Haft",
        "description_pl": """<h2>Prany Dad Hat — Vintage Styl, Wiosna 2026</h2>
<ul>
<li><b>GARMENT WASHED</b> – Miękki, vintageowy wygląd z efektem prania — nabiera charakteru z czasem</li>
<li><b>PREMIUM BAWEŁNA</b> – 100% bawełniana tkanina twill, lekka i oddychająca</li>
<li><b>REGULOWANA</b> – Rozmiar uniwersalny z regulowanym paskiem — dla kobiet i mężczyzn</li>
<li><b>HAFT</b> – Wysokiej jakości haft na przednim panelu, trwały i odporny na blaknięcie</li>
<li><b>TREND WIOSNA 2026</b> – Prany dad hat to trend nr 1 na wiosnę 2026</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>
""",
    },
    {
        "sku": "PFT-OBH",
        "printful_id": 547,
        "bl_product_id": 543455999,
        "price_pln": "119.00",
        "category_id": "5554",  # Kapelusze (bucket hat)
        "product_params": PRODUCT_PARAMS_KAPELUSZE,
        "image_url": "https://files.cdn.printful.com/o/upload/product-catalog-img/f7/f76719d343e1f76b2ae702e33e48e5c4_l",
        "title": "Bucket Hat Bawełna Organiczna GOTS Ekologiczna Unisex Haft",
        "description_pl": """
<h2>Bucket Hat z Bawełny Organicznej — Certyfikat GOTS</h2>
<ul>
<li><b>BAWEŁNA ORGANICZNA</b> – Z certyfikowanej bawełny organicznej — lepsza dla Ciebie i planety</li>
<li><b>CERTYFIKAT GOTS</b> – Certyfikacja GOTS gwarantuje etyczną i zrównoważoną produkcję</li>
<li><b>TREND LATO 2026</b> – Y2K revival: bucket hat to największy trend modowy lata 2026</li>
<li><b>HAFT PREMIUM</b> – Wysokiej jakości haft, trwały i odporny na pranie</li>
<li><b>UNISEX</b> – Regulowany, idealny dla kobiet i mężczyzn, festiwale, plaża i codzień</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>
""",
    },
    {
        "sku": "PFT-BBH",
        "printful_id": 379,
        "bl_product_id": 543456003,
        "price_pln": "109.00",
        "category_id": "5554",  # Kapelusze (bucket hat)
        "product_params": PRODUCT_PARAMS_KAPELUSZE,
        "image_url": "https://files.cdn.printful.com/o/products/379/product_1584958536.jpg",
        "title": "Bucket Hat Lato Festiwal Unisex Czapka Wędkarska Y2K Haft",
        "description_pl": """
<h2>Bucket Hat — Letni Festiwalowy Klasyk Y2K</h2>
<ul>
<li><b>TREND Y2K</b> – Bucket haty wróciły: trend modowy lata nr 1 z +350% wzrostem od 2020</li>
<li><b>PREMIUM BAWEŁNA</b> – Wytrzymała bawełna, lekka i komfortowa przez cały dzień</li>
<li><b>NA FESTIWALE</b> – Idealny kapelusz na letnie festiwale, plaże i wyprawy plenerowe</li>
<li><b>UNISEX</b> – Regulowany design dla każdego rozmiaru głowy — świetny prezent</li>
<li><b>HAFT</b> – Unikalne hafcione zdobienie na przednim panelu, trwałe i wyraziste</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>
""",
    },
    {
        "sku": "PFT-DDH",
        "printful_id": 396,
        "bl_product_id": 543456007,
        "price_pln": "99.00",
        "category_id": "5553",  # Czapki z daszkiem
        "product_params": PRODUCT_PARAMS_CZAPKI,
        "image_url": "https://files.cdn.printful.com/o/products/396/product_1585044725.jpg",
        "title": "Distressed Dad Hat Vintage Czapka z Daszkiem Used Look Haft",
        "description_pl": """
<h2>Distressed Dad Hat — Vintage Used Look, Wiosna 2026</h2>
<ul>
<li><b>DISTRESSED LOOK</b> – Celowo postarzany efekt dla autentycznego, vintage wyglądu</li>
<li><b>PREMIUM BAWEŁNA</b> – Miękka, wytrzymała bawełna zapewniająca komfort przez cały dzień</li>
<li><b>REGULOWANA</b> – Regulowany pasek na tyle — pasuje do każdej głowy</li>
<li><b>HAFT</b> – Trwały haft na przednim panelu, odporny na blaknięcie</li>
<li><b>UNISEX TREND</b> – Retro czapka idealna jako prezent dla kobiet i mężczyzn</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>
""",
    },
    {
        "sku": "PFT-CCH",
        "printful_id": 532,
        "bl_product_id": 543456012,
        "price_pln": "109.00",
        "category_id": "5553",  # Czapki z daszkiem
        "product_params": PRODUCT_PARAMS_CZAPKI,
        "image_url": "https://files.cdn.printful.com/o/upload/product-catalog-img/7a/7a477dc112d1aecab4184e76a4184ac2_l",
        "title": "Czapka Sztruksowa Corduroy Bejsbolówka Regulowana Unisex Haft",
        "description_pl": """
<h2>Czapka Sztruksowa Corduroy — Klasyczny Styl</h2>
<ul>
<li><b>SZTRUKS CORDUROY</b> – Miękka, elegancka tkanina sztruksowa — ponadczasowy styl</li>
<li><b>PREMIUM JAKOŚĆ</b> – Wytrzymała konstrukcja, idealna na jesień i chłodniejsze wieczory</li>
<li><b>REGULOWANA</b> – Regulowany pasek z tyłu — jeden rozmiar dla wszystkich</li>
<li><b>HAFT</b> – Wysokiej jakości haft na przednim panelu, trwały i elegancki</li>
<li><b>PREZENT</b> – Świetny pomysł na prezent dla kobiet i mężczyzn w każdym wieku</li>
</ul>
<p>Dostawa realizowana przez Printful (print-on-demand). Czas produkcji 3–5 dni roboczych + dostawa.</p>
""",
    },
]


# ── Offer builder ─────────────────────────────────────────────────────────────

def build_offer(product: dict, allegro_image_url: str) -> dict:
    """Build a /sale/product-offers payload (new API, active since 2024).

    Parameter split:
      - offer-level: Stan only (requiredForProduct=False)
      - product-level: Marka, Rozmiar, Kolor, Wzór, Materiał (requiredForProduct=True)
    """
    assert 12 <= len(product["title"]) <= 75, f"Title length {len(product['title'])} out of range: {product['title']}"
    return {
        "name": product["title"],
        "category": {"id": product["category_id"]},
        "parameters": OFFER_PARAMS_STAN,  # offer-level only: Stan
        "images": [allegro_image_url],  # string array (not objects)
        "productSet": [
            {
                "product": {
                    "name": product["title"],
                    "category": {"id": product["category_id"]},
                    "parameters": product["product_params"],
                    "images": [allegro_image_url],
                }
            }
        ],
        "description": {
            "sections": [
                {
                    "items": [
                        {
                            "type": "TEXT",
                            "content": product["description_pl"],
                        }
                    ]
                },
                {
                    "items": [
                        {
                            "type": "TEXT",
                            "content": f"<p><b>SKU:</b> {product['sku']}</p>",
                        }
                    ]
                },
            ]
        },
        "sellingMode": {
            "format": "BUY_NOW",
            "price": {"amount": product["price_pln"], "currency": "PLN"},
        },
        "stock": {
            "available": 999,
            "unit": "UNIT",
        },
        "delivery": {
            "shippingRates": {"id": SHIPPING_RATE_ID},
            "handlingTime": "PT96H",  # 4 days (Printful: 3–5 days production)
        },
        "payments": {
            "invoice": "VAT",
        },
        "publication": {
            "status": "INACTIVE",  # Draft — activate manually after review
        },
        "language": "pl-PL",
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Allegro listing creator — 5x PFT- hats (Spring Sale 2026)")
    print("=" * 60)

    token = get_valid_token()
    api = Allegro(token)

    # Verify auth
    me = api.get("/me")
    print(f"\n✓ Auth OK — {me.get('login', '?')} (ID: {me.get('id', '?')})")

    # Upload images
    print("\n[1/2] Uploading product images to Allegro...")
    allegro_urls: dict[str, str] = {}
    for product in PRODUCTS:
        sku = product["sku"]
        src = product["image_url"]
        print(f"  {sku}: uploading... ", end="", flush=True)
        try:
            url = api.upload_image(src)
            allegro_urls[sku] = url
            print(f"✓  {url[:60]}...")
        except Exception as e:
            print(f"✗  {e}")

    # Create offers
    print("\n[2/2] Creating draft offers...")
    results: dict = {"created": [], "errors": []}

    for product in PRODUCTS:
        sku = product["sku"]
        img_url = allegro_urls.get(sku)
        print(f"\n  → {sku} | {product['title'][:55]}...")
        print(f"    Cat: {product['category_id']} | {product['price_pln']} PLN | Image: {'✓' if img_url else '✗'}")

        if not img_url:
            msg = "Image upload failed — cannot create offer"
            print(f"    ✗ {msg}")
            results["errors"].append({"sku": sku, "error": msg})
            continue

        offer = build_offer(product, img_url)

        try:
            resp = api.post("/sale/product-offers", offer)
            offer_id = resp.get("id", "?")
            status = resp.get("publication", {}).get("status", "?")
            print(f"    ✓ Created! Offer ID: {offer_id} | Status: {status}")
            results["created"].append({
                "sku": sku,
                "printful_id": product["printful_id"],
                "bl_product_id": product["bl_product_id"],
                "allegro_offer_id": offer_id,
                "price_pln": product["price_pln"],
                "category_id": product["category_id"],
                "status": status,
                "title": product["title"],
            })
        except Exception as e:
            print(f"    ✗ Error: {e}")
            results["errors"].append({"sku": sku, "error": str(e)})

    # Save results
    RESULTS_FILE.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    print(f"\n{'=' * 60}")
    print(f"Saved: {RESULTS_FILE}")
    print(f"Created: {len(results['created'])}/5  |  Errors: {len(results['errors'])}/5")

    if results["created"]:
        print("\nDraft offers to activate in Allegro Seller Panel:")
        for r in results["created"]:
            print(f"  {r['sku']} → {r['allegro_offer_id']} @ {r['price_pln']} PLN")

    if results["errors"]:
        print("\nErrors:")
        for e in results["errors"]:
            print(f"  {e['sku']}: {e['error']}")

    return results


if __name__ == "__main__":
    main()
