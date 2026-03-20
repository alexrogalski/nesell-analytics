#!/usr/bin/env python3.11
"""Propagate MEGA-BELGIUM listing from DE to 7 EU marketplaces.

Steps per marketplace:
1. PATCH parent title (localized)
2. PATCH each child: purchasable_offer (price), fulfillment_availability,
   color (localized), style_name (localized NF/FL)

Usage:
    python3.11 propagate_mega_belgium.py                # execute all
    python3.11 propagate_mega_belgium.py --dry-run      # preview only
    python3.11 propagate_mega_belgium.py --market FR     # single market
"""

import argparse
import json
import time
import sys
from datetime import datetime

# Add project to path
sys.path.insert(0, "/Users/alexanderrogalski/nesell-analytics")
from etl import config
from etl.amazon_api import headers, _refresh_token, api_get
from etl.fix_variations import (
    api_patch, SELLER_ID,
    COLOR_NORMALIZE_FR, COLOR_NORMALIZE_IT, COLOR_NORMALIZE_ES,
    COLOR_NORMALIZE_NL, COLOR_NORMALIZE_PL, COLOR_NORMALIZE_SE,
)

# ── Configuration ────────────────────────────────────────────────────

PARENT_SKU = "MEGA-BELGIUM"
DE_MKT_ID = "A1PA6795UKMFR9"

# NF prefix = PFT-93856238, FL prefix = PFT-93855556
NF_PREFIX = "PFT-93856238"
FL_PREFIX = "PFT-93855556"

# 11 variant suffixes (dad hat color IDs)
VARIANT_IDS = ["7853", "7854", "7855", "7856", "7857", "7858", "7859", "8745", "9794", "12735", "12736"]

# All 22 child SKUs
NF_SKUS = [f"{NF_PREFIX}-{vid}" for vid in VARIANT_IDS]
FL_SKUS = [f"{FL_PREFIX}-{vid}" for vid in VARIANT_IDS]
ALL_CHILD_SKUS = NF_SKUS + FL_SKUS

TARGETS = {
    "FR": {"mkt_id": "A13V1IB3VIYZZH", "currency": "EUR", "lang": "fr_FR"},
    "IT": {"mkt_id": "APJ6JRA9NG5V4",  "currency": "EUR", "lang": "it_IT"},
    "ES": {"mkt_id": "A1RKKUPIHCS9HS", "currency": "EUR", "lang": "es_ES"},
    "NL": {"mkt_id": "A1805IZSGTT6HS", "currency": "EUR", "lang": "nl_NL"},
    "PL": {"mkt_id": "A1C3SOZRARQ6R3", "currency": "PLN", "lang": "pl_PL"},
    "SE": {"mkt_id": "A2NODRKZP88ZB9", "currency": "SEK", "lang": "sv_SE"},
    "BE": {"mkt_id": "AMEN7PMS3EDWL",  "currency": "EUR", "lang": "fr_BE"},
}

# Prices per marketplace type
PRICES = {
    "EUR": {"NF": 24.99, "FL": 29.99},
    "PLN": {"NF": 109.99, "FL": 129.99},
    "SEK": {"NF": 279, "FL": 329},
}

# Localized parent titles
PARENT_TITLES = {
    "FR": "Casquette Brodee Make Belgium Great Again - Dad Hat Reglable en Coton Unisex Baseball Cap",
    "IT": "Cappellino Ricamato Make Belgium Great Again - Dad Hat Regolabile in Cotone Unisex Baseball Cap",
    "ES": "Gorra Bordada Make Belgium Great Again - Dad Hat Ajustable de Algodon Unisex Baseball Cap",
    "NL": "Geborduurde Pet Make Belgium Great Again - Verstelbare Dad Hat Katoen Unisex Baseball Cap",
    "PL": "Haftowana Czapka Make Belgium Great Again - Regulowana Dad Hat Bawelniana Unisex Baseball Cap",
    "SE": "Broderad Keps Make Belgium Great Again - Justerbar Dad Hat Bomull Unisex Baseball Cap",
    "BE": "Casquette Brodee Make Belgium Great Again - Dad Hat Reglable en Coton Unisex Baseball Cap",
}

# Localized style names
STYLE_NAMES = {
    "FR": {"NF": "sans drapeau", "FL": "avec drapeau"},
    "IT": {"NF": "senza bandiera", "FL": "con bandiera"},
    "ES": {"NF": "sin bandera", "FL": "con bandera"},
    "NL": {"NF": "zonder vlag", "FL": "met vlag"},
    "PL": {"NF": "bez flagi", "FL": "z flaga"},
    "SE": {"NF": "utan flagga", "FL": "med flagga"},
    "BE": {"NF": "sans drapeau", "FL": "avec drapeau"},
}

# Color maps per marketplace (variant_id -> localized color)
# Base English colors for reference
EN_COLORS = {
    "7853": "White", "7854": "Black", "7855": "Khaki", "7856": "Light Blue",
    "7857": "Navy", "7858": "Pink", "7859": "Stone", "8745": "Spruce",
    "9794": "Green Camo", "12735": "Cranberry", "12736": "Dark Grey",
}

# Build color maps by marketplace using fix_variations normalization dicts
def _build_color_map(norm_dict):
    """Map variant_id -> localized color using English->locale normalization."""
    return {vid: norm_dict.get(en_name, en_name) for vid, en_name in EN_COLORS.items()}

COLOR_MAPS = {
    "FR": _build_color_map(COLOR_NORMALIZE_FR),
    "IT": _build_color_map(COLOR_NORMALIZE_IT),
    "ES": _build_color_map(COLOR_NORMALIZE_ES),
    "NL": _build_color_map(COLOR_NORMALIZE_NL),
    "PL": _build_color_map(COLOR_NORMALIZE_PL),
    "SE": _build_color_map(COLOR_NORMALIZE_SE),
    "BE": _build_color_map(COLOR_NORMALIZE_FR),  # BE uses French colors
}


# ── Helpers ──────────────────────────────────────────────────────────

def extract_variant_id(sku):
    """Extract variant ID (last part after dash) from SKU like PFT-93856238-7853."""
    return sku.rsplit("-", 1)[-1]


def is_nf(sku):
    """Check if SKU is NF (no flag) variant."""
    return sku.startswith(NF_PREFIX)


def get_style_type(sku):
    """Return 'NF' or 'FL' based on SKU prefix."""
    return "NF" if is_nf(sku) else "FL"


def get_price(sku, currency):
    """Get correct price for this SKU based on NF/FL and currency."""
    style_type = get_style_type(sku)
    return PRICES[currency][style_type]


def get_color(sku, mkt_code):
    """Get localized color for this SKU on given marketplace."""
    vid = extract_variant_id(sku)
    return COLOR_MAPS[mkt_code].get(vid, f"UNKNOWN-{vid}")


def get_style(sku, mkt_code):
    """Get localized style name for this SKU on given marketplace."""
    style_type = get_style_type(sku)
    return STYLE_NAMES[mkt_code][style_type]


# ── PATCH Builders ───────────────────────────────────────────────────

def build_parent_patch(mkt_code, mkt_id, lang_tag):
    """Build PATCH body for parent listing title."""
    title = PARENT_TITLES[mkt_code]
    return {
        "productType": "HAT",
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/item_name",
                "value": [{"value": title, "language_tag": lang_tag, "marketplace_id": mkt_id}]
            }
        ]
    }


def build_child_patch(sku, mkt_code, mkt_id, lang_tag, currency):
    """Build PATCH body for a child listing: price, fulfillment, color, style."""
    price = get_price(sku, currency)
    color = get_color(sku, mkt_code)
    style = get_style(sku, mkt_code)

    patches = [
        # Price (purchasable_offer)
        {
            "op": "replace",
            "path": "/attributes/purchasable_offer",
            "value": [{
                "currency": currency,
                "our_price": [{
                    "schedule": [{
                        "value_with_tax": price
                    }]
                }],
                "marketplace_id": mkt_id,
            }]
        },
        # Fulfillment availability
        {
            "op": "replace",
            "path": "/attributes/fulfillment_availability",
            "value": [{
                "fulfillment_channel_code": "DEFAULT",
                "quantity": 999,
                "marketplace_id": mkt_id,
            }]
        },
        # Color
        {
            "op": "replace",
            "path": "/attributes/color",
            "value": [{"value": color, "language_tag": lang_tag, "marketplace_id": mkt_id}]
        },
        # Style
        {
            "op": "replace",
            "path": "/attributes/style_name",
            "value": [{"value": style, "language_tag": lang_tag, "marketplace_id": mkt_id}]
        },
    ]

    return {
        "productType": "HAT",
        "patches": patches,
    }


# ── Main Execution ───────────────────────────────────────────────────

def verify_children_on_de():
    """Verify all 22 children exist on DE marketplace."""
    print(f"\n{'='*70}")
    print(f"  Verifying {len(ALL_CHILD_SKUS)} children on DE ({DE_MKT_ID})")
    print(f"{'='*70}")

    found = []
    missing = []
    for sku in ALL_CHILD_SKUS:
        data = api_get(
            f"/listings/2021-08-01/items/{SELLER_ID}/{sku}",
            params={"marketplaceIds": DE_MKT_ID, "includedData": "summaries"}
        )
        if data and "errors" not in data:
            found.append(sku)
            print(f"  [OK] {sku}")
        else:
            missing.append(sku)
            print(f"  [MISSING] {sku}")
        time.sleep(0.5)

    print(f"\n  Found: {len(found)}/22, Missing: {len(missing)}")
    return found, missing


def propagate_to_marketplace(mkt_code, dry_run=False):
    """Propagate all children + parent title to a single marketplace."""
    t = TARGETS[mkt_code]
    mkt_id = t["mkt_id"]
    currency = t["currency"]
    lang_tag = t["lang"]

    print(f"\n{'='*70}")
    print(f"  Propagating to {mkt_code} ({mkt_id}) | {currency} | {lang_tag}")
    print(f"{'='*70}")

    success = 0
    errors = 0

    # 1. PATCH parent title
    print(f"\n  --- Parent: {PARENT_SKU} ---")
    parent_body = build_parent_patch(mkt_code, mkt_id, lang_tag)
    title_preview = PARENT_TITLES[mkt_code][:60] + "..."
    print(f"  Title: {title_preview}")

    if dry_run:
        print(f"  [DRY RUN] Would PATCH parent title")
    else:
        path = f"/listings/2021-08-01/items/{SELLER_ID}/{PARENT_SKU}"
        status, resp = api_patch(path, parent_body, params={"marketplaceIds": mkt_id})
        status_str = resp.get("status", "UNKNOWN") if isinstance(resp, dict) else "UNKNOWN"
        if status in (200, 202):
            print(f"  [{status}] Parent PATCH -> {status_str}")
            success += 1
        else:
            print(f"  [{status}] Parent PATCH FAILED: {json.dumps(resp)[:300]}")
            errors += 1
        time.sleep(0.5)

    # 2. PATCH each child
    for sku in ALL_CHILD_SKUS:
        vid = extract_variant_id(sku)
        style_type = get_style_type(sku)
        price = get_price(sku, currency)
        color = get_color(sku, mkt_code)
        style = get_style(sku, mkt_code)

        print(f"\n  [{style_type}] {sku}")
        print(f"    price={price} {currency}, color={color}, style={style}")

        if dry_run:
            print(f"    [DRY RUN] Would PATCH")
            success += 1
            continue

        child_body = build_child_patch(sku, mkt_code, mkt_id, lang_tag, currency)
        path = f"/listings/2021-08-01/items/{SELLER_ID}/{sku}"
        status, resp = api_patch(path, child_body, params={"marketplaceIds": mkt_id})
        status_str = resp.get("status", "UNKNOWN") if isinstance(resp, dict) else "UNKNOWN"

        if status in (200, 202):
            print(f"    [{status}] -> {status_str}")
            success += 1
        else:
            err_detail = json.dumps(resp)[:300] if resp else "no response"
            print(f"    [{status}] FAILED: {err_detail}")
            errors += 1

        time.sleep(0.5)

    return success, errors


def main():
    parser = argparse.ArgumentParser(description="Propagate MEGA-BELGIUM to EU marketplaces")
    parser.add_argument("--dry-run", action="store_true", help="Preview without executing PATCHes")
    parser.add_argument("--market", type=str, default=None,
                        help="Single marketplace code (FR, IT, ES, NL, PL, SE, BE)")
    parser.add_argument("--skip-verify", action="store_true",
                        help="Skip DE verification step")
    args = parser.parse_args()

    print(f"\n{'#'*70}")
    print(f"# MEGA-BELGIUM Propagation - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"# Markets: {args.market or 'ALL (FR, IT, ES, NL, PL, SE, BE)'}")
    print(f"{'#'*70}")

    # Verify children exist on DE
    if not args.skip_verify:
        found, missing = verify_children_on_de()
        if missing:
            print(f"\n  [ERROR] {len(missing)} children missing on DE. Cannot propagate.")
            print(f"  Missing: {missing}")
            if not args.dry_run:
                print("  Aborting. Use --skip-verify to override.")
                return
            print("  Continuing in dry-run mode anyway...")

    # Determine target markets
    if args.market:
        if args.market.upper() not in TARGETS:
            print(f"  [ERROR] Unknown market: {args.market}. Valid: {list(TARGETS.keys())}")
            return
        markets = [args.market.upper()]
    else:
        markets = ["FR", "IT", "ES", "NL", "PL", "SE", "BE"]

    total_success = 0
    total_errors = 0

    for mkt in markets:
        s, e = propagate_to_marketplace(mkt, dry_run=args.dry_run)
        total_success += s
        total_errors += e

    # Summary
    print(f"\n{'#'*70}")
    print(f"# SUMMARY")
    print(f"#   Markets processed: {len(markets)}")
    print(f"#   Successful PATCHes: {total_success}")
    print(f"#   Failed PATCHes: {total_errors}")
    print(f"#   Total operations: {total_success + total_errors}")
    print(f"{'#'*70}")

    # Price verification table
    print(f"\n  Price Verification:")
    print(f"  {'Market':<6} {'Currency':<6} {'NF':>8} {'FL':>8}")
    print(f"  {'-'*30}")
    for mkt in markets:
        cur = TARGETS[mkt]["currency"]
        nf_p = PRICES[cur]["NF"]
        fl_p = PRICES[cur]["FL"]
        print(f"  {mkt:<6} {cur:<6} {nf_p:>8} {fl_p:>8}")


if __name__ == "__main__":
    main()
