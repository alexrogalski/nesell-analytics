"""Add Spring 2026 hats to Baselinker inventory with Printful fulfillment.

Creates product entries in Baselinker inventory (inv_id=52954, Printful).
Maps to the same SKUs used in Amazon listings (PFT-S26-*).

NOTE: Printful products must be created manually (or via Printful API) first.
      Once Printful products exist, sync them to Baselinker using
      Baselinker's Printful integration (Settings → Integrations → Printful).
      Then update SKUs in Baselinker to match the PFT-S26-* convention.

This script adds placeholder products to Baselinker so Amazon ↔ BL
SKU mapping is established before Printful sync.

Usage:
    cd ~/nesell-analytics
    python3.11 scripts/create_spring_hats_baselinker.py --dry-run
    python3.11 scripts/create_spring_hats_baselinker.py
"""
import json
import requests
import time
import argparse
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl import config

# Baselinker Printful inventory ID
BL_PRINTFUL_INV_ID = 52954

# Product definitions to add to Baselinker
SPRING_PRODUCTS = [
    {
        "sku": "PFT-S26-WASHED-KHAKI",
        "name": "Vintage Garment Washed Dad Hat | Embroidered | Khaki",
        "price_brutto": 22.99,
        "weight": 0.15,
        "printful_catalog_id": 961,
        "color": "Khaki",
    },
    {
        "sku": "PFT-S26-WASHED-STONE",
        "name": "Vintage Garment Washed Dad Hat | Embroidered | Stone",
        "price_brutto": 22.99,
        "weight": 0.15,
        "printful_catalog_id": 961,
        "color": "Stone",
    },
    {
        "sku": "PFT-S26-WASHED-SAGE",
        "name": "Vintage Garment Washed Dad Hat | Embroidered | Sage",
        "price_brutto": 22.99,
        "weight": 0.15,
        "printful_catalog_id": 961,
        "color": "Sage",
    },
    {
        "sku": "PFT-S26-WASHED-NAVY",
        "name": "Vintage Garment Washed Dad Hat | Embroidered | Navy",
        "price_brutto": 22.99,
        "weight": 0.15,
        "printful_catalog_id": 961,
        "color": "Navy",
    },
    {
        "sku": "PFT-S26-ORGBUCKET-BLACK",
        "name": "Organic Bucket Hat | GOTS Cotton Embroidered Eco | Black",
        "price_brutto": 34.99,
        "weight": 0.18,
        "printful_catalog_id": 547,
        "color": "Black",
    },
    {
        "sku": "PFT-S26-ORGBUCKET-NATURAL",
        "name": "Organic Bucket Hat | GOTS Cotton Embroidered Eco | Natural",
        "price_brutto": 34.99,
        "weight": 0.18,
        "printful_catalog_id": 547,
        "color": "Natural",
    },
    {
        "sku": "PFT-S26-ORGBUCKET-NAVY",
        "name": "Organic Bucket Hat | GOTS Cotton Embroidered Eco | Navy",
        "price_brutto": 34.99,
        "weight": 0.18,
        "printful_catalog_id": 547,
        "color": "Navy",
    },
    {
        "sku": "PFT-S26-BUCKET-BLACK",
        "name": "Bucket Hat Cotton Embroidered Festival Summer | Black",
        "price_brutto": 26.99,
        "weight": 0.16,
        "printful_catalog_id": 379,
        "color": "Black",
    },
    {
        "sku": "PFT-S26-BUCKET-NAVY",
        "name": "Bucket Hat Cotton Embroidered Festival Summer | Navy",
        "price_brutto": 26.99,
        "weight": 0.16,
        "printful_catalog_id": 379,
        "color": "Navy",
    },
    {
        "sku": "PFT-S26-BUCKET-WHITE",
        "name": "Bucket Hat Cotton Embroidered Festival Summer | White",
        "price_brutto": 26.99,
        "weight": 0.16,
        "printful_catalog_id": 379,
        "color": "White",
    },
    {
        "sku": "PFT-S26-DIST-BLACK",
        "name": "Distressed Vintage Dad Hat Embroidered Used-Look | Black",
        "price_brutto": 24.99,
        "weight": 0.15,
        "printful_catalog_id": 396,
        "color": "Black",
    },
    {
        "sku": "PFT-S26-DIST-CHARCOAL",
        "name": "Distressed Vintage Dad Hat Embroidered Used-Look | Charcoal",
        "price_brutto": 24.99,
        "weight": 0.15,
        "printful_catalog_id": 396,
        "color": "Charcoal",
    },
    {
        "sku": "PFT-S26-DIST-KHAKI",
        "name": "Distressed Vintage Dad Hat Embroidered Used-Look | Khaki",
        "price_brutto": 24.99,
        "weight": 0.15,
        "printful_catalog_id": 396,
        "color": "Khaki",
    },
    {
        "sku": "PFT-S26-DIST-NAVY",
        "name": "Distressed Vintage Dad Hat Embroidered Used-Look | Navy",
        "price_brutto": 24.99,
        "weight": 0.15,
        "printful_catalog_id": 396,
        "color": "Navy",
    },
    {
        "sku": "PFT-S26-CORD-BLACK",
        "name": "Corduroy Cap Embroidered Fashion Baseball Cap | Black",
        "price_brutto": 26.99,
        "weight": 0.17,
        "printful_catalog_id": 532,
        "color": "Black",
    },
    {
        "sku": "PFT-S26-CORD-CAMEL",
        "name": "Corduroy Cap Embroidered Fashion Baseball Cap | Camel",
        "price_brutto": 26.99,
        "weight": 0.17,
        "printful_catalog_id": 532,
        "color": "Camel",
    },
    {
        "sku": "PFT-S26-CORD-OLIVE",
        "name": "Corduroy Cap Embroidered Fashion Baseball Cap | Dark Olive",
        "price_brutto": 26.99,
        "weight": 0.17,
        "printful_catalog_id": 532,
        "color": "Dark Olive",
    },
    {
        "sku": "PFT-S26-CORD-NAVYD",
        "name": "Corduroy Cap Embroidered Fashion Baseball Cap | Oxford Navy",
        "price_brutto": 26.99,
        "weight": 0.17,
        "printful_catalog_id": 532,
        "color": "Oxford Navy",
    },
]


def bl_call(method: str, params: dict) -> dict:
    """Call Baselinker API."""
    resp = requests.post(
        "https://api.baselinker.com/connector.php",
        data={
            "token": config.BASELINKER_TOKEN,
            "method": method,
            "parameters": json.dumps(params),
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


BL_PRICE_GROUP_EUR = 31059   # "Glowna EUR" — EUR price group in Printful inventory
BL_WAREHOUSE = "bl_51139"   # Main warehouse


def add_product_to_bl(product: dict, dry_run: bool = False) -> bool:
    """Add a single product to Baselinker Printful inventory."""
    params = {
        "inventory_id": BL_PRINTFUL_INV_ID,
        "product_id": "",  # empty = new product
        "ean": "",
        "sku": product["sku"],
        "text_fields": {
            "name": product["name"],
            "description_extra1": f"Print-on-demand via Printful. Printful catalog product ID: {product['printful_catalog_id']}. Color: {product['color']}. Spring 2026 collection.",
        },
        "quantity": {BL_WAREHOUSE: 999},
        "prices": {str(BL_PRICE_GROUP_EUR): product["price_brutto"]},
        "weight": product["weight"],
        "man_name": "Printful Latvia AS",
        "category_id": 0,
        "is_bundle": False,
    }

    if dry_run:
        print(f"  [DRY-RUN] Would add: {product['sku']} — {product['name']}")
        return True

    result = bl_call("addInventoryProduct", params)
    if result.get("status") == "SUCCESS":
        pid = result.get("product_id")
        print(f"  [OK] {product['sku']} → Baselinker product_id={pid}")
        return True
    else:
        print(f"  [ERR] {product['sku']}: {result}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Add Spring 2026 hats to Baselinker")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Adding {len(SPRING_PRODUCTS)} products to Baselinker (inv_id={BL_PRINTFUL_INV_ID})")
    print(f"Dry-run: {args.dry_run}\n")

    ok = 0
    fail = 0
    for product in SPRING_PRODUCTS:
        success = add_product_to_bl(product, dry_run=args.dry_run)
        if success:
            ok += 1
        else:
            fail += 1
        if not args.dry_run:
            time.sleep(0.5)

    print(f"\nDone: {ok} OK, {fail} failed")

    # Save manifest for reference
    manifest_path = os.path.join(os.path.dirname(__file__), "spring_hats_sku_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump({
            "note": "Spring 2026 hat SKUs. Amazon listings use these SKUs. Printful products need to be created manually then synced via Baselinker.",
            "printful_catalog_ids": {
                "961": "Otto Cap 18-772 Garment Washed Dad Hat",
                "547": "Capstone Organic Bucket Hat",
                "379": "Big Accessories BX003 Bucket Hat",
                "396": "Otto Cap 104-1018 Distressed Dad Hat",
                "532": "Beechfield B682 Corduroy Cap",
            },
            "products": SPRING_PRODUCTS,
        }, f, ensure_ascii=False, indent=2)
    print(f"SKU manifest saved to {manifest_path}")


if __name__ == "__main__":
    main()
