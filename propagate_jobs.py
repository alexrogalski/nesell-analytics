"""Propagation jobs: activate offers on missing marketplaces.

Job 1: MEGA-BRITAIN children -> BE (fr_BE)
Job 2: PFT-93855391 (Spain NF) -> all 8 markets
Job 3: PFT-90201053 (Sweden FL) -> all 8 markets
Job 4: PFT-90202876 (America FL) -> DE + 7 markets
"""
import sys
import time
sys.path.insert(0, "/Users/alexanderrogalski/nesell-analytics")

from etl.amazon_api import headers, _refresh_token, api_get
from etl.amazon_listings import (
    SELLER_ID, MARKETPLACE_IDS, LANG_TAGS, CURRENCIES,
    api_patch, patch_listing, put_listing, check_listing_exists,
    DAD_HAT_COLORS, DE_COLORS, SE_COLORS,
)
from etl.fix_variations import (
    COLOR_NORMALIZE_FR, COLOR_NORMALIZE_DE, COLOR_NORMALIZE_IT,
    COLOR_NORMALIZE_ES, COLOR_NORMALIZE_NL, COLOR_NORMALIZE_PL,
    COLOR_NORMALIZE_SE, COLOR_NORMALIZE_EN,
    STYLE_NAMES,
)

DELAY = 0.5

# ── Color maps per marketplace (English key -> localized) ──
# Using English as canonical key for all variant IDs
COLORS_EN = {
    "7853": "White", "7854": "Black", "7855": "Khaki",
    "7856": "Light Blue", "7857": "Navy", "7858": "Pink",
    "7859": "Stone", "8745": "Spruce", "9794": "Green Camo",
    "12735": "Cranberry", "12736": "Dark Grey",
}

# Build color lookup: mkt_code -> {variant_id: localized_color}
COLOR_BY_MKT = {}
for mkt in MARKETPLACE_IDS:
    COLOR_BY_MKT[mkt] = {}
    for vid, en_name in COLORS_EN.items():
        if mkt == "DE":
            COLOR_BY_MKT[mkt][vid] = COLOR_NORMALIZE_DE.get(en_name, en_name)
        elif mkt == "FR":
            COLOR_BY_MKT[mkt][vid] = COLOR_NORMALIZE_FR.get(en_name, en_name)
        elif mkt == "IT":
            COLOR_BY_MKT[mkt][vid] = COLOR_NORMALIZE_IT.get(en_name, en_name)
        elif mkt == "ES":
            COLOR_BY_MKT[mkt][vid] = COLOR_NORMALIZE_ES.get(en_name, en_name)
        elif mkt == "NL":
            COLOR_BY_MKT[mkt][vid] = COLOR_NORMALIZE_NL.get(en_name, en_name)
        elif mkt == "PL":
            COLOR_BY_MKT[mkt][vid] = COLOR_NORMALIZE_PL.get(en_name, en_name)
        elif mkt == "SE":
            COLOR_BY_MKT[mkt][vid] = COLOR_NORMALIZE_SE.get(en_name, en_name)
        elif mkt == "BE":
            COLOR_BY_MKT[mkt][vid] = COLOR_NORMALIZE_FR.get(en_name, en_name)

VARIANT_IDS = ["7853", "7854", "7855", "7856", "7857", "7858", "7859", "8745", "9794", "12735", "12736"]

# Prices
NF_PRICES = {"EUR": 24.99, "PLN": 109.99, "SEK": 279}
FL_PRICES = {"EUR": 29.99, "PLN": 129.99, "SEK": 329}


def build_offer_patch(mkt_code, price, lang_tag):
    """Build purchasable_offer patch for a marketplace."""
    currency = CURRENCIES[mkt_code]
    mkt_id = MARKETPLACE_IDS[mkt_code]
    return {
        "op": "replace",
        "path": "/attributes/purchasable_offer",
        "value": [{
            "marketplace_id": mkt_id,
            "currency": currency,
            "our_price": [{"schedule": [{"value_with_tax": price}]}],
        }]
    }


def build_fulfillment_patch(mkt_code):
    """Build fulfillment_availability patch."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    return {
        "op": "replace",
        "path": "/attributes/fulfillment_availability",
        "value": [{
            "fulfillment_channel_code": "DEFAULT",
            "quantity": 999,
            "marketplace_id": mkt_id,
        }]
    }


def build_color_patch(mkt_code, color_value):
    """Build color_name patch."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang_tag = LANG_TAGS[mkt_code]
    return {
        "op": "replace",
        "path": "/attributes/color",
        "value": [{
            "language_tag": lang_tag,
            "value": color_value,
            "marketplace_id": mkt_id,
        }]
    }


def build_style_patch(mkt_code, style_value):
    """Build style_name patch for MEGA listings with NF/FL."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang_tag = LANG_TAGS[mkt_code]
    return {
        "op": "replace",
        "path": "/attributes/style_name",
        "value": [{
            "language_tag": lang_tag,
            "value": style_value,
            "marketplace_id": mkt_id,
        }]
    }


def do_patch(sku, mkt_code, patches, label=""):
    """Execute a PATCH and print result."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    status, resp = patch_listing(sku, mkt_id, patches)
    issues = resp.get("issues", [])
    if issues:
        for iss in issues[:3]:
            print(f"    ISSUE: {iss.get('code','')}: {iss.get('message','')[:100]}")
    time.sleep(DELAY)
    return status, resp


# ══════════════════════════════════════════════════════════════
# JOB 1: MEGA-BRITAIN children -> BE
# ══════════════════════════════════════════════════════════════
def job1_mega_britain_be():
    print("\n" + "="*60)
    print("JOB 1: MEGA-BRITAIN children -> BE (fr_BE)")
    print("="*60)

    mkt = "BE"
    nf_parent = "PFT-93856169"  # NF children
    fl_parent = "PFT-93856110"  # FL children
    nf_price = NF_PRICES["EUR"]  # 24.99
    fl_price = FL_PRICES["EUR"]  # 29.99

    # Style names for BE (French)
    style_nf = STYLE_NAMES["BE"]["no_flag"]  # "sans Drapeau"
    style_fl = STYLE_NAMES["BE"]["flag"]      # "avec Drapeau"

    count = 0
    for vid in VARIANT_IDS:
        color = COLOR_BY_MKT[mkt][vid]

        # NF child
        sku_nf = f"{nf_parent}-{vid}"
        print(f"\n  NF: {sku_nf} ({color}, {nf_price} EUR)")
        patches_nf = [
            build_offer_patch(mkt, nf_price, LANG_TAGS[mkt]),
            build_fulfillment_patch(mkt),
            build_color_patch(mkt, color),
            build_style_patch(mkt, style_nf),
        ]
        do_patch(sku_nf, mkt, patches_nf)
        count += 1

        # FL child
        sku_fl = f"{fl_parent}-{vid}"
        print(f"  FL: {sku_fl} ({color}, {fl_price} EUR)")
        patches_fl = [
            build_offer_patch(mkt, fl_price, LANG_TAGS[mkt]),
            build_fulfillment_patch(mkt),
            build_color_patch(mkt, color),
            build_style_patch(mkt, style_fl),
        ]
        do_patch(sku_fl, mkt, patches_fl)
        count += 1

    print(f"\nJob 1 done: {count} children patched on BE")


# ══════════════════════════════════════════════════════════════
# JOB 2: PFT-93855391 (Spain NF) -> all 8 markets
# ══════════════════════════════════════════════════════════════
def job2_spain_nf():
    print("\n" + "="*60)
    print("JOB 2: PFT-93855391 (Spain NF) -> 8 markets")
    print("="*60)

    parent_sku = "PFT-93855391"
    count = 0

    for mkt_code in ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"]:
        currency = CURRENCIES[mkt_code]
        price = NF_PRICES[currency]
        print(f"\n  --- {mkt_code} ({price} {currency}) ---")

        for vid in VARIANT_IDS:
            sku = f"{parent_sku}-{vid}"
            color = COLOR_BY_MKT[mkt_code][vid]

            print(f"    {sku} -> {color}")
            patches = [
                build_offer_patch(mkt_code, price, LANG_TAGS[mkt_code]),
                build_fulfillment_patch(mkt_code),
                build_color_patch(mkt_code, color),
            ]
            do_patch(sku, mkt_code, patches)
            count += 1

    print(f"\nJob 2 done: {count} patches across 8 markets")


# ══════════════════════════════════════════════════════════════
# JOB 3: PFT-90201053 (Sweden FL) -> all 8 markets
# ══════════════════════════════════════════════════════════════
def job3_sweden_fl():
    print("\n" + "="*60)
    print("JOB 3: PFT-90201053 (Sweden FL) -> 8 markets")
    print("="*60)

    parent_sku = "PFT-90201053"
    count = 0

    for mkt_code in ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"]:
        currency = CURRENCIES[mkt_code]
        price = FL_PRICES[currency]
        print(f"\n  --- {mkt_code} ({price} {currency}) ---")

        for vid in VARIANT_IDS:
            sku = f"{parent_sku}-{vid}"
            color = COLOR_BY_MKT[mkt_code][vid]

            print(f"    {sku} -> {color}")
            patches = [
                build_offer_patch(mkt_code, price, LANG_TAGS[mkt_code]),
                build_fulfillment_patch(mkt_code),
                build_color_patch(mkt_code, color),
            ]
            do_patch(sku, mkt_code, patches)
            count += 1

    print(f"\nJob 3 done: {count} patches across 8 markets")


# ══════════════════════════════════════════════════════════════
# JOB 4: PFT-90202876 (America FL) -> DE + 7 markets
# ══════════════════════════════════════════════════════════════
def job4_america_fl():
    print("\n" + "="*60)
    print("JOB 4: PFT-90202876 (America FL) -> DE + 7 markets")
    print("="*60)

    parent_sku = "PFT-90202876"
    count = 0

    for mkt_code in ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"]:
        currency = CURRENCIES[mkt_code]
        price = FL_PRICES[currency]
        print(f"\n  --- {mkt_code} ({price} {currency}) ---")

        for vid in VARIANT_IDS:
            sku = f"{parent_sku}-{vid}"
            color = COLOR_BY_MKT[mkt_code][vid]

            # Try PATCH first (works if SKU exists on marketplace)
            print(f"    {sku} -> {color}")
            patches = [
                build_offer_patch(mkt_code, price, LANG_TAGS[mkt_code]),
                build_fulfillment_patch(mkt_code),
                build_color_patch(mkt_code, color),
            ]
            status, resp = do_patch(sku, mkt_code, patches)

            # If PATCH fails with 404 or issues indicating SKU not found,
            # we'll note it - but PATCH on Listings API usually creates
            # the offer even if SKU only exists on another marketplace
            if status == 404:
                print(f"    [WARN] {sku} not found on {mkt_code} - may need PUT")

            count += 1

    print(f"\nJob 4 done: {count} patches across 8 markets")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("Starting propagation jobs...")
    print(f"Seller ID: {SELLER_ID}")
    print(f"Delay between calls: {DELAY}s")

    job1_mega_britain_be()
    job2_spain_nf()
    job3_sweden_fl()
    job4_america_fl()

    print("\n" + "="*60)
    print("ALL JOBS COMPLETE")
    print("="*60)
