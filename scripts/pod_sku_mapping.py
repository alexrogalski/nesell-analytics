#!/usr/bin/env python3
"""Build SKU → Printful sync_variant_id mapping from Printful store + Baselinker.

Why sync_variant_id, not variant_id?
  - variant_id = Printful catalog variant (generic, no design)
  - sync_variant_id = store-specific variant with embroidery design attached
  Orders using sync_variant_id automatically include the saved design files.

SKU format in Baselinker: PFT-{template_id}-{catalog_variant_id}
  e.g., PFT-100032925-12735 → catalog variant_id=12735 → sync_variant_id=4684333004

Output: data/pod_sku_map.json
  Format: {"PFT-100032925-12735": 4684333004, ...}

Usage:
  python scripts/pod_sku_mapping.py           # build/refresh mapping
  python scripts/pod_sku_mapping.py --verify  # verify and show current mapping
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from etl import config
from etl.printful_orders import parse_pft_sku

OUTPUT_FILE = ROOT / "data" / "pod_sku_map.json"
PRINTFUL_API_BASE = "https://api.printful.com"
PRINTFUL_INVENTORY_ID = 52954  # Baselinker inventory for Printful products


def _pf_get(path: str, params: dict | None = None) -> dict:
    """GET from Printful API v1."""
    headers = {
        "Authorization": f"Bearer {config.PRINTFUL_V1_TOKEN}",
        "X-PF-Store-Id": str(config.PRINTFUL_STORE_ID),
    }
    resp = requests.get(
        f"{PRINTFUL_API_BASE}{path}",
        headers=headers,
        params=params or {},
        timeout=30,
    )
    return resp.json()


def fetch_printful_store_variants() -> dict[int, int]:
    """Fetch all sync products from Printful store and return catalog_variant_id → sync_variant_id.

    Iterates all store products and their variants.
    """
    catalog_to_sync: dict[int, int] = {}

    # Paginate store products
    offset = 0
    limit = 100
    total_found = 0

    while True:
        data = _pf_get("/store/products", params={"offset": offset, "limit": limit})
        products = data.get("result", [])
        if not products:
            break

        for p in products:
            product_id = p["id"]
            # Fetch full product with variants
            detail = _pf_get(f"/store/products/{product_id}")
            full = detail.get("result", {})
            for sv in full.get("sync_variants", []):
                cat_vid = sv.get("variant_id")
                sync_vid = sv.get("id")
                if cat_vid and sync_vid:
                    catalog_to_sync[int(cat_vid)] = int(sync_vid)
                    total_found += 1
            time.sleep(0.3)

        offset += limit
        if len(products) < limit:
            break

    print(f"Fetched {total_found} sync variants from Printful store {config.PRINTFUL_STORE_ID}")
    return catalog_to_sync


def fetch_baselinker_pft_skus() -> dict[str, int]:
    """Fetch all PFT-* SKUs from Baselinker and return SKU → catalog_variant_id."""
    from etl.baselinker import bl_api

    all_ids: list[str] = []
    page = 1
    while True:
        data = bl_api("getInventoryProductsList", {
            "inventory_id": PRINTFUL_INVENTORY_ID,
            "page": page,
        })
        products = data.get("products", {})
        if not products:
            break
        all_ids.extend(products.keys())
        page += 1
        time.sleep(0.4)

    sku_to_catalog: dict[str, int] = {}
    for i in range(0, len(all_ids), 100):
        batch = all_ids[i:i + 100]
        data = bl_api("getInventoryProductsData", {
            "inventory_id": PRINTFUL_INVENTORY_ID,
            "products": batch,
        })
        for _pid, product in data.get("products", {}).items():
            # Check parent SKU
            sku = product.get("sku", "")
            if sku.startswith("PFT-"):
                _, vid = parse_pft_sku(sku)
                if vid is not None:
                    sku_to_catalog[sku] = vid

            # Check variant SKUs
            for _v, variant in product.get("variants", {}).items():
                vsku = variant.get("sku", "")
                if vsku.startswith("PFT-"):
                    _, vid = parse_pft_sku(vsku)
                    if vid is not None:
                        sku_to_catalog[vsku] = vid
        time.sleep(0.4)

    print(f"Found {len(sku_to_catalog)} PFT-* SKUs in Baselinker inventory")
    return sku_to_catalog


def build_mapping() -> None:
    """Build and save the complete SKU → sync_variant_id mapping file."""
    print("Step 1: Fetching Printful store sync variants...")
    catalog_to_sync = fetch_printful_store_variants()
    print(f"  catalog_variant_id → sync_variant_id: {len(catalog_to_sync)} entries")

    print("\nStep 2: Fetching Baselinker PFT-* SKUs...")
    sku_to_catalog = fetch_baselinker_pft_skus()
    print(f"  SKU → catalog_variant_id: {len(sku_to_catalog)} entries")

    print("\nStep 3: Building SKU → sync_variant_id mapping...")
    sku_map: dict[str, int] = {}
    unmapped: list[str] = []

    for sku, cat_vid in sku_to_catalog.items():
        sync_vid = catalog_to_sync.get(cat_vid)
        if sync_vid is not None:
            sku_map[sku] = sync_vid
        else:
            unmapped.append(f"{sku} (catalog_vid={cat_vid})")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(sku_map, indent=2, sort_keys=True))

    print(f"\nSaved {len(sku_map)} SKU → sync_variant_id mappings to {OUTPUT_FILE}")

    if unmapped:
        print(f"\nWARNING: {len(unmapped)} SKU(s) not found in Printful store (may not be synced):")
        for u in unmapped[:10]:
            print(f"  {u}")

    # Show sample
    sample = list(sku_map.items())[:5]
    print("\nSample mappings:")
    for sku, svid in sample:
        _, cat_vid = parse_pft_sku(sku)
        print(f"  {sku} → catalog_vid={cat_vid} → sync_variant_id={svid}")


def verify_mapping() -> None:
    """Load mapping file and display summary."""
    if not OUTPUT_FILE.exists():
        print(f"Mapping file not found: {OUTPUT_FILE}")
        print("Run without --verify first to build it.")
        sys.exit(1)

    sku_map = json.loads(OUTPUT_FILE.read_text())
    print(f"Loaded {len(sku_map)} SKU → sync_variant_id mappings from {OUTPUT_FILE}")

    print("\nAll mappings:")
    for sku, svid in sorted(sku_map.items()):
        _, cat_vid = parse_pft_sku(sku)
        print(f"  {sku} → catalog_vid={cat_vid} → sync_variant_id={svid}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build POD SKU → sync_variant_id mapping")
    parser.add_argument("--verify", action="store_true", help="Show current mapping file")
    args = parser.parse_args()

    if args.verify:
        verify_mapping()
    else:
        build_mapping()
