"""Amazon Listings API: create/update listings across EU marketplaces.

Usage:
    cd ~/nesell-analytics
    python3.11 -m etl.amazon_listings --task propagate_se
    python3.11 -m etl.amazon_listings --task create_parents
    python3.11 -m etl.amazon_listings --task trucker_propagate
    python3.11 -m etl.amazon_listings --task all
    python3.11 -m etl.amazon_listings --task all --dry-run
"""
import argparse
import json
import requests
import time
import sys
from datetime import datetime
from . import config
from .amazon_api import headers, _refresh_token, api_get


# ── Constants ─────────────────────────────────────────────────────────

SELLER_ID = "A1IZH6PW7A624A"
SE_MKT = "A2NODRKZP88ZB9"

MARKETPLACE_IDS = {
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS",
    "NL": "A1805IZSGTT6HS",
    "PL": "A1C3SOZRARQ6R3",
    "SE": "A2NODRKZP88ZB9",
    "BE": "AMEN7PMS3EDWL",
}

# Language tags for each marketplace
LANG_TAGS = {
    "DE": "de_DE",
    "FR": "fr_FR",
    "IT": "it_IT",
    "ES": "es_ES",
    "NL": "nl_NL",
    "PL": "pl_PL",
    "SE": "sv_SE",
    "BE": "nl_BE",
}

# Currency per marketplace
CURRENCIES = {
    "DE": "EUR", "FR": "EUR", "IT": "EUR", "ES": "EUR",
    "NL": "EUR", "PL": "PLN", "SE": "SEK", "BE": "EUR",
}

# headwear_size size_system per marketplace
SIZE_SYSTEMS = {
    "DE": "as3", "FR": "as4", "IT": "as6", "ES": "as4",
    "NL": "as3", "PL": "as3", "SE": "as3", "BE": "as4",
}

# Price in local currency (base price for dad hat)
DAD_HAT_PRICES = {
    "DE": 27.99, "FR": 27.99, "IT": 27.99, "ES": 27.99,
    "NL": 27.99, "PL": 129.99, "SE": 329.00, "BE": 27.99,
}

# Trucker hat prices
TRUCKER_PRICES = {
    "DE": 27.99, "FR": 27.99, "IT": 27.99, "ES": 27.99,
    "NL": 27.99, "PL": 129.99, "SE": 329.00, "BE": 27.99,
}

# Color suffix mapping (shared across dad hat families)
DAD_HAT_COLORS = {
    "7853": "White",
    "7854": "Black",
    "7855": "Khaki",
    "7856": "Light Blue",
    "7857": "Navy",
    "7858": "Pink",
    "7859": "Stone",
    "8745": "Spruce",
    "9794": "Green Camo",
    "12735": "Cranberry",
    "12736": "Dark Grey",
}

# Swedish color names
SE_COLORS = {
    "7853": "Vit",
    "7854": "Svart",
    "7855": "Beige",
    "7856": "Ljusblå",
    "7857": "Marinblå",
    "7858": "Rosa",
    "7859": "Stengrå",
    "8745": "Grangrön",
    "9794": "Grön kamouflage",
    "12735": "Tranbär",
    "12736": "Mörkgrå",
}

# German color names (from DE listings)
DE_COLORS = {
    "7853": "Weiss",
    "7854": "Schwarz",
    "7855": "Beige",
    "7856": "Hellblau",
    "7857": "Marineblau",
    "7858": "Rosa",
    "7859": "Steingrau",
    "8745": "Tannengruen",
    "9794": "Gruenes Tarnmuster",
    "12735": "Cranberry",
    "12736": "Dunkelgrau",
}

# Trucker color mappings
TRUCKER_COLORS_DE = {
    "8747": "Weiss",
    "8748": "Schwarz",
    "8749": "Anthrazit",
    "8750": "Grau",
    "8751": "Marineblau",
    "8752": "Rot",
    "8753": "Koenigsblau",
    "10933": "Braun/Beige",
    "10934": "Gruen/Weiss",
    "10935": "Graumeliert/Weiss",
    "10936": "Schwarz/Weiss",
    "10937": "Marineblau/Weiss",
    "10938": "Rot/Weiss",
    "10939": "Koenigsblau/Weiss",
    "10940": "Anthrazit/Weiss",
    "10941": "Tuerkis/Weiss",
    "12220": "Rost/Beige",
}

TRUCKER_COLORS_SE = {
    "8747": "Vit",
    "8748": "Svart",
    "8749": "Antracit",
    "8750": "Grå",
    "8751": "Marinblå",
    "8752": "Röd",
    "8753": "Kungsblå",
    "10933": "Brun/Beige",
    "10934": "Grön/Vit",
    "10935": "Gråmelerad/Vit",
    "10936": "Svart/Vit",
    "10937": "Marinblå/Vit",
    "10938": "Röd/Vit",
    "10939": "Kungsblå/Vit",
    "10940": "Antracit/Vit",
    "10941": "Turkos/Vit",
    "12220": "Rost/Beige",
}

# ── Correct Listing Architecture ──────────────────────────────────────
#
# The correct approach is ONE parent per country with STYLE_NAME/COLOR_NAME
# variation theme. Children are distinguished by:
#   - style_name: "mit Flagge" / "ohne Flagge" (or localized equivalents)
#   - color_name: the color of the hat (e.g. "Schwarz", "Weiss")
#
# This gives customers a single product page with a style dropdown (flag
# vs no-flag) and a color dropdown, maximizing reviews and ranking.
#
# Working example on DE:
#   Parent: MEGA-GERMANY-OLD (ASIN B0GS3K29WB)
#   Variation theme: STYLE_NAME/COLOR_NAME
#   Children: both "ohne Flagge" and "mit Flagge" styles, each in 11 colors
#
# DO NOT create separate NF (no-flag) and FL (flag) parent listings.
# That splits reviews, rankings, and confuses customers.
# ──────────────────────────────────────────────────────────────────────

# Unified parent SKUs - ONE parent per country with STYLE_NAME/COLOR_NAME variation.
# Working example: MEGA-GERMANY-OLD on DE has both "ohne Flagge" and "mit Flagge" styles.
# New countries should follow this pattern instead of creating separate NF/FL parents.
UNIFIED_PARENTS = {
    "Germany": "MEGA-GERMANY-OLD",
    # Other countries to be created following same pattern:
    # "France": "MEGA-FRANCE",
    # "Italy": "MEGA-ITALY",
    # "Poland": "MEGA-POLAND",
    # "Belgium": "MEGA-BELGIUM",
    # "Netherlands": "MEGA-NETHERLANDS",
    # "GB": "MEGA-GB",
    # "Spain": "MEGA-SPAIN",
    # "America": "MEGA-AMERICA",
    # "Sweden": "MEGA-SWEDEN",
}

# DEPRECATED: Country-specific data with separate nf_parent / fl_parent SKUs.
# This structure creates SEPARATE Amazon listings for flag and no-flag versions,
# which is the WRONG approach. Kept for backwards compatibility (used by
# audit_listings.py) but should NOT be used for new listing creation.
# Use UNIFIED_PARENTS above instead.
COUNTRY_DATA = {
    "France": {
        "nf_parent": "PFT-93856317",
        "fl_parent": "PFT-93854576",
        "mega_parent": "PFT-MEGA-FR",
        "country_en": "France",
        "se_keywords": "broderad keps frankrike patriot present baseball cap dad hat bomull justerbar make france great again flagga unisex",
    },
    "Italy": {
        "nf_parent": "PFT-93856295",
        "fl_parent": "PFT-93854948",
        "mega_parent": "PFT-MEGA-IT",
        "country_en": "Italy",
        "se_keywords": "broderad keps italien patriot present baseball cap dad hat bomull justerbar make italy great again flagga unisex",
    },
    "Poland": {
        "nf_parent": "PFT-93856269",
        "fl_parent": "PFT-93855132",
        "mega_parent": "PFT-MEGA-PL",
        "country_en": "Poland",
        "se_keywords": "broderad keps polen patriot present baseball cap dad hat bomull justerbar make poland great again flagga unisex",
    },
    "Belgium": {
        "nf_parent": "PFT-93856238",
        "fl_parent": "PFT-93855556",
        "mega_parent": "PFT-MEGA-BE2",
        "country_en": "Belgium",
        "se_keywords": "broderad keps belgien patriot present baseball cap dad hat bomull justerbar make belgium great again flagga unisex",
    },
    "Netherlands": {
        "nf_parent": "PFT-93856209",
        "fl_parent": "PFT-93855915",
        "mega_parent": "PFT-MEGA-NL2",
        "country_en": "The Netherlands",
        "se_keywords": "broderad keps nederländerna patriot present baseball cap dad hat bomull justerbar make netherlands great again flagga unisex",
    },
    "GB": {
        "nf_parent": "PFT-93856169",
        "fl_parent": "PFT-93856110",
        "mega_parent": "PFT-MEGA-GB",
        "country_en": "Great Britain",
        "se_keywords": "broderad keps storbritannien patriot present baseball cap dad hat bomull justerbar make great britain great again flagga unisex",
    },
    "Spain": {
        "nf_parent": "PFT-93855391",
        "fl_parent": None,
        "mega_parent": None,
        "country_en": "Spain",
        "se_keywords": "broderad keps spanien patriot present baseball cap dad hat bomull justerbar make spain great again unisex",
    },
    "Germany": {
        "nf_parent": "PFT-90034427",
        "fl_parent": None,
        "mega_parent": None,
        "country_en": "Germany",
        "se_keywords": "broderad keps tyskland patriot present baseball cap dad hat bomull justerbar make germany great again unisex",
    },
    "America": {
        "nf_parent": None,
        "fl_parent": "PFT-90202876",
        "mega_parent": None,
        "country_en": "America",
        "se_keywords": "broderad keps amerika usa patriot present baseball cap dad hat bomull justerbar make america great again flagga unisex",
    },
    "Sweden": {
        "nf_parent": None,
        "fl_parent": "PFT-90201053",
        "mega_parent": None,
        "country_en": "Sweden",
        "se_keywords": "broderad keps sverige patriot present baseball cap dad hat bomull justerbar make sweden great again flagga unisex",
    },
}

# NF families have these children (without 9794 for some)
NF_CHILDREN = ["7853", "7854", "7855", "7856", "7857", "7858", "7859", "8745", "9794", "12735", "12736"]
FL_CHILDREN = ["7853", "7854", "7855", "7856", "7857", "7858", "7859", "8745", "9794", "12735", "12736"]
TRUCKER_CHILDREN = list(TRUCKER_COLORS_DE.keys())


# ── API Helpers ──────────────────────────────────────────────────────

def api_put(path, body, params=None, retries=8):
    """PUT with retry for Listings API."""
    url = f"https://sellingpartnerapi-eu.amazon.com{path}"
    for attempt in range(retries):
        try:
            resp = requests.put(url, headers=headers(), json=body, params=params, timeout=30)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"    [ConnErr] retry in {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [429] rate limited, waiting {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            _refresh_token()
            time.sleep(3)
            continue
        if resp.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [{resp.status_code}] server error, retry in {wait}s")
            time.sleep(wait)
            continue
        return resp.status_code, resp.json()
    print(f"    [WARN] All {retries} attempts failed for PUT {path}")
    return 0, {}


def api_patch(path, body, params=None, retries=8):
    """PATCH with retry for Listings API."""
    url = f"https://sellingpartnerapi-eu.amazon.com{path}"
    for attempt in range(retries):
        try:
            resp = requests.patch(url, headers=headers(), json=body, params=params, timeout=30)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"    [ConnErr] retry in {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [429] rate limited, waiting {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            _refresh_token()
            time.sleep(3)
            continue
        if resp.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [{resp.status_code}] server error, retry in {wait}s")
            time.sleep(wait)
            continue
        return resp.status_code, resp.json()
    print(f"    [WARN] All {retries} attempts failed for PATCH {path}")
    return 0, {}


def put_listing(sku, marketplace_id, attributes, product_type="HAT", dry_run=False):
    """Create or update a listing using putListingsItem."""
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{sku}"
    body = {
        "productType": product_type,
        "requirements": "LISTING",
        "attributes": attributes,
    }
    if dry_run:
        print(f"  [DRY-RUN] PUT {sku} on {marketplace_id}")
        return 200, {"status": "DRY_RUN", "sku": sku}

    status, resp = api_put(path, body, params={"marketplaceIds": marketplace_id})
    status_str = resp.get("status", "UNKNOWN")
    issues = resp.get("issues", [])
    if issues:
        issue_msgs = [f"{i.get('code','')}: {i.get('message','')[:80]}" for i in issues[:3]]
        print(f"  [{status}] {sku} -> {status_str} | Issues: {'; '.join(issue_msgs)}")
    else:
        print(f"  [{status}] {sku} -> {status_str}")
    return status, resp


def patch_listing(sku, marketplace_id, patches, product_type="HAT", dry_run=False):
    """Update a listing using patchListingsItem."""
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{sku}"
    body = {
        "productType": product_type,
        "patches": patches,
    }
    if dry_run:
        print(f"  [DRY-RUN] PATCH {sku} on {marketplace_id}")
        return 200, {"status": "DRY_RUN", "sku": sku}

    status, resp = api_patch(path, body, params={"marketplaceIds": marketplace_id})
    status_str = resp.get("status", "UNKNOWN")
    print(f"  [{status}] PATCH {sku} -> {status_str}")
    return status, resp


def check_listing_exists(sku, marketplace_id):
    """Check if a listing already exists on a marketplace."""
    try:
        data = api_get(f'/listings/2021-08-01/items/{SELLER_ID}/{sku}',
                       params={'marketplaceIds': marketplace_id, 'includedData': 'summaries'})
        return 'errors' not in data and data != {}
    except Exception as e:
        print(f"    [WARN] check_listing_exists({sku}) failed: {e}")
        return False


def get_listing_full(sku, marketplace_id):
    """Get full listing attributes from a marketplace."""
    data = api_get(f'/listings/2021-08-01/items/{SELLER_ID}/{sku}',
                   params={
                       'marketplaceIds': marketplace_id,
                       'includedData': 'summaries,attributes,issues,offers,relationships'
                   })
    return data


# ── SE Listing Builders ──────────────────────────────────────────────

def build_se_dad_hat_parent_attrs(country_en, is_flag=False, style_value=None):
    """Build SE parent listing attributes for a dad hat."""
    mkt = SE_MKT
    lang = "sv_SE"

    flag_str = " med Flagga" if is_flag else ""
    flag_str_en = " with Flag" if is_flag else ""

    item_name = f"Broderad Keps Make {country_en} Great Again{flag_str} - Justerbar Dad Hat Unisex Bomull Baseball Keps"

    bullets = [
        f"PREMIUM MASKINBRODERI - Professionellt broderad text{flag_str} på framsidan. Broderier bleknar inte, flagnar inte och rivs inte som tryck",
        "BEKVÄM FÖR VARDAGEN - 100% bomull chino twill. Låg profil och förböjd skärm för klassisk Dad Hat-look",
        "JUSTERBAR PASSFORM - Metallspänne bak anpassar sig till alla storlekar. En storlek för dam och herr",
        "PERFEKT PRESENT - Idealisk för patrioter, födelsedagar, högtider eller som stiligt tillbehör",
        "MÅNGSIDIG - Perfekt för fritid, sport, resor, festivaler och dagligt bruk",
    ]

    description = (
        f"Denna högkvalitativa broderade keps med texten Make {country_en} Great Again{flag_str} "
        f"är den perfekta följeslagaren. 100% bomull chino twill, professionellt hållbart broderi, "
        f"justerbart metallspänne."
    )

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt}],
        "color": [{"value": "Flerfärgad", "language_tag": lang, "marketplace_id": mkt}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt}],
        "pattern": [{"value": "Bokstäver", "language_tag": lang, "marketplace_id": mkt}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt}],
        "model_name": [{"value": f"Make {country_en} Great Again Dad Hat", "language_tag": lang, "marketplace_id": mkt}],
        "age_range_description": [{"value": "Vuxen", "language_tag": lang, "marketplace_id": mkt}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mkt}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt} for b in bullets],
        "product_description": [{"value": description, "language_tag": lang, "marketplace_id": mkt}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt}],
        "generic_keyword": [{"value": COUNTRY_DATA.get(country_en, COUNTRY_DATA.get("France", {})).get("se_keywords", ""), "language_tag": lang, "marketplace_id": mkt}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt}],
        "headwear_size": [{"size": "one_size", "size_system": "as3", "size_class": "alpha", "marketplace_id": mkt}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt}],
        "list_price": [{"value_with_tax": DAD_HAT_PRICES["SE"], "currency": "SEK", "marketplace_id": mkt}],
        "batteries_required": [{"value": False, "marketplace_id": mkt}],
        "fabric_type": [{"value": "100% Cotton", "language_tag": lang, "marketplace_id": mkt}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt}],
        "material": [{"value": "Cotton", "language_tag": lang, "marketplace_id": mkt}],
        "style": [{"value": "Klassisk", "language_tag": lang, "marketplace_id": mkt}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mkt}],
        "care_instructions": [{"value": "Handtvätt", "language_tag": lang, "marketplace_id": mkt}],
        "unit_count": [{"type": {"value": "piece", "language_tag": lang}, "value": 1.0, "marketplace_id": mkt}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt}],
        "parentage_level": [{"marketplace_id": mkt, "value": "parent"}],
        "child_parent_sku_relationship": [{"marketplace_id": mkt, "child_relationship_type": "variation"}],
    }

    return attrs


def build_se_dad_hat_child_attrs(parent_sku, suffix, country_en, is_flag=False):
    """Build SE child listing attributes for a dad hat."""
    mkt = SE_MKT
    lang = "sv_SE"

    color_se = SE_COLORS.get(suffix, "Flerfärgad")
    color_de = DE_COLORS.get(suffix, color_se)  # Keep DE color as standardized value

    flag_str = " med Flagga" if is_flag else ""

    item_name = f"Broderad Keps Make {country_en} Great Again{flag_str} - Dad Hat Unisex Bomull - {color_se}"

    bullets = [
        f"PREMIUM MASKINBRODERI - Professionellt broderad text{flag_str} på framsidan. Broderier bleknar inte, flagnar inte och rivs inte som tryck",
        "BEKVÄM FÖR VARDAGEN - 100% bomull chino twill. Låg profil och förböjd skärm för klassisk Dad Hat-look",
        "JUSTERBAR PASSFORM - Metallspänne bak anpassar sig till alla storlekar. En storlek för dam och herr",
        "PERFEKT PRESENT - Idealisk för patrioter, födelsedagar, högtider eller som stiligt tillbehör",
        "MÅNGSIDIG - Perfekt för fritid, sport, resor, festivaler och dagligt bruk",
    ]

    description = (
        f"Denna högkvalitativa broderade keps med texten Make {country_en} Great Again{flag_str} "
        f"är den perfekta följeslagaren. 100% bomull chino twill, professionellt hållbart broderi, "
        f"justerbart metallspänne."
    )

    # Get keywords for this country
    country_key = country_en
    if country_en == "The Netherlands":
        country_key = "Netherlands"
    elif country_en == "Great Britain":
        country_key = "GB"
    keywords = COUNTRY_DATA.get(country_key, {}).get("se_keywords", "")

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt}],
        "color": [{"value": color_de, "language_tag": lang, "marketplace_id": mkt}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt}],
        "pattern": [{"value": "Bokstäver", "language_tag": lang, "marketplace_id": mkt}],
        "fulfillment_availability": [{"fulfillment_channel_code": "DEFAULT", "quantity": 999}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt}],
        "model_name": [{"value": f"Make {country_en} Great Again Dad Hat", "language_tag": lang, "marketplace_id": mkt}],
        "age_range_description": [{"value": "Vuxen", "language_tag": lang, "marketplace_id": mkt}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mkt}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt} for b in bullets],
        "product_description": [{"value": description, "language_tag": lang, "marketplace_id": mkt}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt}],
        "generic_keyword": [{"value": keywords, "language_tag": lang, "marketplace_id": mkt}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt}],
        "headwear_size": [{"size": "one_size", "size_system": "as3", "size_class": "alpha", "marketplace_id": mkt}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt}],
        "list_price": [{"value_with_tax": DAD_HAT_PRICES["SE"], "currency": "SEK", "marketplace_id": mkt}],
        "purchasable_offer": [{"currency": "SEK", "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": 279.0}]}], "marketplace_id": mkt}],
        "batteries_required": [{"value": False, "marketplace_id": mkt}],
        "fabric_type": [{"value": "100% Cotton", "language_tag": lang, "marketplace_id": mkt}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt}],
        "material": [{"value": "Cotton", "language_tag": lang, "marketplace_id": mkt}],
        "style": [{"value": "Klassisk", "language_tag": lang, "marketplace_id": mkt}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mkt}],
        "care_instructions": [{"value": "Handtvätt", "language_tag": lang, "marketplace_id": mkt}],
        "unit_count": [{"type": {"value": "piece", "language_tag": lang}, "value": 1.0, "marketplace_id": mkt}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt}],
        "parentage_level": [{"marketplace_id": mkt, "value": "child"}],
        "child_parent_sku_relationship": [{"marketplace_id": mkt, "child_relationship_type": "variation", "parent_sku": parent_sku}],
    }

    return attrs


def build_se_trucker_parent_attrs():
    """Build SE parent listing for Trucker cap."""
    mkt = SE_MKT
    lang = "sv_SE"

    item_name = "Broderad Trucker Keps Make America Great Again - Justerbar Snapback med Mesh Unisex Baseball Keps"

    bullets = [
        "PREMIUM MASKINBRODERI - Professionellt broderad text på framsidan. Broderier bleknar inte, flagnar inte och rivs inte som tryck",
        "ANDNINGSBAR TRUCKER KEPS - Strukturerad framsida i bomull-twill med luftgenomsläppligt mesh-nät bak för optimal ventilation",
        "JUSTERBAR PASSFORM - Snapback-spänne i plast bak för steglös storleksjustering. En storlek för dam och herr",
        "PERFEKT PRESENT - Idealisk för patrioter, födelsedagar, högtider eller som stiligt tillbehör",
        "MÅNGSIDIG - Perfekt för fritid, sport, resor, festivaler och dagligt bruk i alla väder",
    ]

    description = (
        "Denna högkvalitativa broderade Trucker Cap med texten Make America Great Again "
        "är den perfekta följeslagaren. Strukturerad framsida i bomull-twill kombinerat med "
        "andningsbart mesh-nät bak, professionellt hållbart broderi, justerbart snapback-spänne."
    )

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt}],
        "color": [{"value": "Flerfärgad", "language_tag": lang, "marketplace_id": mkt}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt}],
        "pattern": [{"value": "Bokstäver", "language_tag": lang, "marketplace_id": mkt}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt}],
        "model_name": [{"value": "Make America Great Again Trucker Cap", "language_tag": lang, "marketplace_id": mkt}],
        "age_range_description": [{"value": "Vuxen", "language_tag": lang, "marketplace_id": mkt}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mkt}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt} for b in bullets],
        "product_description": [{"value": description, "language_tag": lang, "marketplace_id": mkt}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt}],
        "generic_keyword": [{"value": "broderad keps amerika usa patriot present trucker cap snapback mesh bomull justerbar make america great again unisex", "language_tag": lang, "marketplace_id": mkt}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt}],
        "headwear_size": [{"size": "one_size", "size_system": "as3", "size_class": "alpha", "marketplace_id": mkt}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt}],
        "list_price": [{"value_with_tax": TRUCKER_PRICES["SE"], "currency": "SEK", "marketplace_id": mkt}],
        "batteries_required": [{"value": False, "marketplace_id": mkt}],
        "fabric_type": [{"value": "Bomull, Polyester Mesh", "language_tag": lang, "marketplace_id": mkt}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt}],
        "material": [{"value": "Bomull, Polyester", "language_tag": lang, "marketplace_id": mkt}],
        "style": [{"value": "Klassisk", "language_tag": lang, "marketplace_id": mkt}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mkt}],
        "care_instructions": [{"value": "Handtvätt", "language_tag": lang, "marketplace_id": mkt}],
        "unit_count": [{"type": {"value": "piece", "language_tag": lang}, "value": 1.0, "marketplace_id": mkt}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt}],
        "parentage_level": [{"marketplace_id": mkt, "value": "parent"}],
        "child_parent_sku_relationship": [{"marketplace_id": mkt, "child_relationship_type": "variation"}],
    }

    return attrs


def build_se_trucker_child_attrs(parent_sku, suffix):
    """Build SE child listing for Trucker cap."""
    mkt = SE_MKT
    lang = "sv_SE"

    color_se = TRUCKER_COLORS_SE.get(suffix, "Flerfärgad")
    color_de = TRUCKER_COLORS_DE.get(suffix, color_se)

    item_name = f"Broderad Trucker Keps Make America Great Again - Snapback med Mesh Unisex - {color_se}"

    bullets = [
        "PREMIUM MASKINBRODERI - Professionellt broderad text på framsidan. Broderier bleknar inte, flagnar inte och rivs inte som tryck",
        "ANDNINGSBAR TRUCKER KEPS - Strukturerad framsida i bomull-twill med luftgenomsläppligt mesh-nät bak för optimal ventilation",
        "JUSTERBAR PASSFORM - Snapback-spänne i plast bak för steglös storleksjustering. En storlek för dam och herr",
        "PERFEKT PRESENT - Idealisk för patrioter, födelsedagar, högtider eller som stiligt tillbehör",
        "MÅNGSIDIG - Perfekt för fritid, sport, resor, festivaler och dagligt bruk i alla väder",
    ]

    description = (
        "Denna högkvalitativa broderade Trucker Cap med texten Make America Great Again "
        "är den perfekta följeslagaren. Strukturerad framsida i bomull-twill kombinerat med "
        "andningsbart mesh-nät bak, professionellt hållbart broderi, justerbart snapback-spänne."
    )

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt}],
        "color": [{"value": color_de, "language_tag": lang, "marketplace_id": mkt}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt}],
        "pattern": [{"value": "Bokstäver", "language_tag": lang, "marketplace_id": mkt}],
        "fulfillment_availability": [{"fulfillment_channel_code": "DEFAULT", "quantity": 999}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt}],
        "model_name": [{"value": "Make America Great Again Trucker Cap", "language_tag": lang, "marketplace_id": mkt}],
        "age_range_description": [{"value": "Vuxen", "language_tag": lang, "marketplace_id": mkt}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mkt}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt} for b in bullets],
        "product_description": [{"value": description, "language_tag": lang, "marketplace_id": mkt}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt}],
        "generic_keyword": [{"value": "broderad keps amerika usa patriot present trucker cap snapback mesh bomull justerbar make america great again unisex", "language_tag": lang, "marketplace_id": mkt}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt}],
        "headwear_size": [{"size": "one_size", "size_system": "as3", "size_class": "alpha", "marketplace_id": mkt}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt}],
        "list_price": [{"value_with_tax": TRUCKER_PRICES["SE"], "currency": "SEK", "marketplace_id": mkt}],
        "purchasable_offer": [{"currency": "SEK", "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": 279.0}]}], "marketplace_id": mkt}],
        "batteries_required": [{"value": False, "marketplace_id": mkt}],
        "fabric_type": [{"value": "Bomull, Polyester Mesh", "language_tag": lang, "marketplace_id": mkt}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt}],
        "material": [{"value": "Bomull, Polyester", "language_tag": lang, "marketplace_id": mkt}],
        "style": [{"value": "Klassisk", "language_tag": lang, "marketplace_id": mkt}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mkt}],
        "care_instructions": [{"value": "Handtvätt", "language_tag": lang, "marketplace_id": mkt}],
        "unit_count": [{"type": {"value": "piece", "language_tag": lang}, "value": 1.0, "marketplace_id": mkt}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt}],
        "parentage_level": [{"marketplace_id": mkt, "value": "child"}],
        "child_parent_sku_relationship": [{"marketplace_id": mkt, "child_relationship_type": "variation", "parent_sku": parent_sku}],
    }

    return attrs


# ── Localized listing builders for other marketplaces (Task 3: Trucker) ──

# Language-specific translations for trucker caps
TRUCKER_TRANSLATIONS = {
    "FR": {
        "item_name": "Casquette Trucker Brodée Make America Great Again - Snapback Réglable avec Mesh Unisexe",
        "bullets": [
            "BRODERIE MACHINE PREMIUM - Texte brodé professionnellement sur le devant. La broderie ne décolore pas, ne s'écaille pas et ne se déchire pas comme les impressions",
            "CASQUETTE TRUCKER RESPIRANTE - Face avant structurée en coton twill avec filet mesh respirant à l'arrière pour une ventilation optimale",
            "TAILLE AJUSTABLE - Fermeture snapback en plastique à l'arrière pour un ajustement sans étape. Taille unique pour femme et homme",
            "CADEAU PARFAIT - Idéal pour les patriotes, anniversaires, fêtes ou simplement comme accessoire tendance",
            "POLYVALENTE - Parfaite pour les loisirs, le sport, les voyages, les festivals et un usage quotidien",
        ],
        "description": "Cette casquette trucker brodée de qualité avec le texte Make America Great Again est le compagnon parfait. Face avant en coton twill, broderie durable, fermeture snapback réglable.",
        "pattern": "Lettres",
        "age": "Adulte",
        "care": "Lavage à la main",
        "fabric": "Coton, Polyester Mesh",
        "material": "Coton, Polyester",
        "style": "Classique",
        "keywords": "casquette brodée amérique usa patriote cadeau trucker cap snapback mesh coton réglable make america great again unisexe",
        "unit_type": "pièce",
    },
    "IT": {
        "item_name": "Cappellino Trucker Ricamato Make America Great Again - Snapback Regolabile con Mesh Unisex",
        "bullets": [
            "RICAMO A MACCHINA PREMIUM - Testo ricamato professionalmente sulla parte anteriore. Il ricamo non sbiadisce, non si stacca e non si strappa come le stampe",
            "CAPPELLINO TRUCKER TRASPIRANTE - Parte anteriore strutturata in cotone twill con rete mesh traspirante sul retro per una ventilazione ottimale",
            "TAGLIA REGOLABILE - Chiusura snapback in plastica sul retro per una regolazione continua. Taglia unica per donna e uomo",
            "REGALO PERFETTO - Ideale per patrioti, compleanni, festività o semplicemente come accessorio di tendenza",
            "VERSATILE - Perfetto per tempo libero, sport, viaggi, festival e uso quotidiano",
        ],
        "description": "Questo cappellino trucker ricamato di qualità con il testo Make America Great Again è il compagno perfetto. Parte anteriore in cotone twill, ricamo durevole, chiusura snapback regolabile.",
        "pattern": "Lettere",
        "age": "Adulto",
        "care": "Lavaggio a mano",
        "fabric": "Cotone, Poliestere Mesh",
        "material": "Cotone, Poliestere",
        "style": "Classico",
        "keywords": "cappellino ricamato america usa patriota regalo trucker cap snapback mesh cotone regolabile make america great again unisex",
        "unit_type": "pezzo",
    },
    "ES": {
        "item_name": "Gorra Trucker Bordada Make America Great Again - Snapback Ajustable con Malla Unisex",
        "bullets": [
            "BORDADO A MÁQUINA PREMIUM - Texto bordado profesionalmente en la parte delantera. El bordado no destiñe, no se descascarilla y no se rompe como las impresiones",
            "GORRA TRUCKER TRANSPIRABLE - Parte delantera estructurada en algodón twill con malla transpirable en la parte trasera para una ventilación óptima",
            "TALLA AJUSTABLE - Cierre snapback de plástico en la parte trasera para un ajuste continuo. Talla única para mujer y hombre",
            "REGALO PERFECTO - Ideal para patriotas, cumpleaños, fiestas o simplemente como accesorio de moda",
            "VERSÁTIL - Perfecta para ocio, deporte, viajes, festivales y uso diario",
        ],
        "description": "Esta gorra trucker bordada de calidad con el texto Make America Great Again es el compañero perfecto. Parte delantera en algodón twill, bordado duradero, cierre snapback ajustable.",
        "pattern": "Letras",
        "age": "Adulto",
        "care": "Lavado a mano",
        "fabric": "Algodón, Poliéster Mesh",
        "material": "Algodón, Poliéster",
        "style": "Clásico",
        "keywords": "gorra bordada américa usa patriota regalo trucker cap snapback malla algodón ajustable make america great again unisex",
        "unit_type": "pieza",
    },
    "NL": {
        "item_name": "Geborduurde Trucker Pet Make America Great Again - Verstelbare Snapback met Mesh Unisex",
        "bullets": [
            "PREMIUM MACHINEBORDUURWERK - Professioneel geborduurde tekst aan de voorkant. Borduurwerk vervaagt niet, bladdert niet af en scheurt niet zoals prints",
            "ADEMENDE TRUCKER PET - Gestructureerde voorkant van katoenen twill met luchtdoorlatend mesh-net achter voor optimale ventilatie",
            "VERSTELBARE PASVORM - Kunststof snapback-sluiting achter voor traploos instellen. One size voor dames en heren",
            "PERFECT CADEAU - Ideaal voor patriotten, verjaardagen, feestdagen of gewoon als stijlvol accessoire",
            "VEELZIJDIG - Perfect voor vrije tijd, sport, reizen, festivals en dagelijks gebruik",
        ],
        "description": "Deze hoogwaardige geborduurde Trucker Cap met de tekst Make America Great Again is de perfecte metgezel. Gestructureerde voorkant van katoenen twill, duurzaam borduurwerk, verstelbare snapback-sluiting.",
        "pattern": "Letters",
        "age": "Volwassene",
        "care": "Handwas",
        "fabric": "Katoen, Polyester Mesh",
        "material": "Katoen, Polyester",
        "style": "Klassiek",
        "keywords": "geborduurde pet amerika usa patriot cadeau trucker cap snapback mesh katoen verstelbaar make america great again unisex",
        "unit_type": "stuk",
    },
    "PL": {
        "item_name": "Haftowana Czapka Trucker Make America Great Again - Regulowana Snapback z Siatką Unisex",
        "bullets": [
            "HAFT MASZYNOWY PREMIUM - Profesjonalnie wyhaftowany napis na przodzie. Haft nie blaknie, nie łuszczy się i nie rwie jak nadruki",
            "ODDYCHAJĄCA CZAPKA TRUCKER - Strukturowany przód z bawełnianego twill z przepuszczalną siatką z tyłu dla optymalnej wentylacji",
            "REGULOWANY ROZMIAR - Plastikowe zapięcie snapback z tyłu do płynnej regulacji. Rozmiar uniwersalny dla kobiet i mężczyzn",
            "IDEALNY PREZENT - Idealny dla patriotów, na urodziny, święta lub po prostu jako stylowy dodatek",
            "WSZECHSTRONNA - Idealna na co dzień, sport, podróże, festiwale i codzienne użytkowanie",
        ],
        "description": "Ta wysokiej jakości haftowana czapka Trucker z napisem Make America Great Again to idealny towarzysz. Strukturowany przód z bawełnianego twill, trwały haft, regulowane zapięcie snapback.",
        "pattern": "Litery",
        "age": "Dorosły",
        "care": "Pranie ręczne",
        "fabric": "Bawełna, Poliester Mesh",
        "material": "Bawełna, Poliester",
        "style": "Klasyczny",
        "keywords": "haftowana czapka ameryka usa patriota prezent trucker cap snapback siatka bawełna regulowana make america great again unisex",
        "unit_type": "sztuka",
    },
    "SE": {
        "item_name": "Broderad Trucker Keps Make America Great Again - Justerbar Snapback med Mesh Unisex",
        "bullets": [
            "PREMIUM MASKINBRODERI - Professionellt broderad text på framsidan. Broderier bleknar inte, flagnar inte och rivs inte som tryck",
            "ANDNINGSBAR TRUCKER KEPS - Strukturerad framsida i bomull-twill med luftgenomsläppligt mesh-nät bak för optimal ventilation",
            "JUSTERBAR PASSFORM - Snapback-spänne i plast bak för steglös storleksjustering. En storlek för dam och herr",
            "PERFEKT PRESENT - Idealisk för patrioter, födelsedagar, högtider eller som stiligt tillbehör",
            "MÅNGSIDIG - Perfekt för fritid, sport, resor, festivaler och dagligt bruk i alla väder",
        ],
        "description": "Denna högkvalitativa broderade Trucker Cap med texten Make America Great Again är den perfekta följeslagaren. Strukturerad framsida i bomull-twill, andningsbart mesh-nät bak, justerbart snapback-spänne.",
        "pattern": "Bokstäver",
        "age": "Vuxen",
        "care": "Handtvätt",
        "fabric": "Bomull, Polyester Mesh",
        "material": "Bomull, Polyester",
        "style": "Klassisk",
        "keywords": "broderad keps amerika usa patriot present trucker cap snapback mesh bomull justerbar make america great again unisex",
        "unit_type": "piece",
    },
    "BE": {
        "item_name": "Geborduurde Trucker Pet Make America Great Again - Verstelbare Snapback met Mesh Unisex",
        "bullets": [
            "PREMIUM MACHINEBORDUURWERK - Professioneel geborduurde tekst aan de voorkant. Borduurwerk vervaagt niet, bladdert niet af en scheurt niet zoals prints",
            "ADEMENDE TRUCKER PET - Gestructureerde voorkant van katoenen twill met luchtdoorlatend mesh-net achter voor optimale ventilatie",
            "VERSTELBARE PASVORM - Kunststof snapback-sluiting achter voor traploos instellen. One size voor dames en heren",
            "PERFECT CADEAU - Ideaal voor patriotten, verjaardagen, feestdagen of gewoon als stijlvol accessoire",
            "VEELZIJDIG - Perfect voor vrije tijd, sport, reizen, festivals en dagelijks gebruik",
        ],
        "description": "Deze hoogwaardige geborduurde Trucker Cap met de tekst Make America Great Again is de perfecte metgezel. Gestructureerde voorkant van katoenen twill, duurzaam borduurwerk, verstelbare snapback-sluiting.",
        "pattern": "Letters",
        "age": "Volwassene",
        "care": "Handwas",
        "fabric": "Katoen, Polyester Mesh",
        "material": "Katoen, Polyester",
        "style": "Klassiek",
        "keywords": "geborduurde pet amerika usa patriot cadeau trucker cap snapback mesh katoen verstelbaar make america great again unisex",
        "unit_type": "stuk",
    },
}

# Trucker color translations per marketplace
TRUCKER_COLORS = {
    "FR": {
        "8747": "Blanc", "8748": "Noir", "8749": "Anthracite", "8750": "Gris",
        "8751": "Bleu Marine", "8752": "Rouge", "8753": "Bleu Royal",
        "10933": "Marron/Beige", "10934": "Vert/Blanc", "10935": "Gris Chiné/Blanc",
        "10936": "Noir/Blanc", "10937": "Bleu Marine/Blanc", "10938": "Rouge/Blanc",
        "10939": "Bleu Royal/Blanc", "10940": "Anthracite/Blanc", "10941": "Turquoise/Blanc",
        "12220": "Rouille/Beige",
    },
    "IT": {
        "8747": "Bianco", "8748": "Nero", "8749": "Antracite", "8750": "Grigio",
        "8751": "Blu Marina", "8752": "Rosso", "8753": "Blu Reale",
        "10933": "Marrone/Beige", "10934": "Verde/Bianco", "10935": "Grigio Melange/Bianco",
        "10936": "Nero/Bianco", "10937": "Blu Marina/Bianco", "10938": "Rosso/Bianco",
        "10939": "Blu Reale/Bianco", "10940": "Antracite/Bianco", "10941": "Turchese/Bianco",
        "12220": "Ruggine/Beige",
    },
    "ES": {
        "8747": "Blanco", "8748": "Negro", "8749": "Antracita", "8750": "Gris",
        "8751": "Azul Marino", "8752": "Rojo", "8753": "Azul Real",
        "10933": "Marrón/Beige", "10934": "Verde/Blanco", "10935": "Gris Jaspeado/Blanco",
        "10936": "Negro/Blanco", "10937": "Azul Marino/Blanco", "10938": "Rojo/Blanco",
        "10939": "Azul Real/Blanco", "10940": "Antracita/Blanco", "10941": "Turquesa/Blanco",
        "12220": "Óxido/Beige",
    },
    "NL": {
        "8747": "Wit", "8748": "Zwart", "8749": "Antraciet", "8750": "Grijs",
        "8751": "Marineblauw", "8752": "Rood", "8753": "Koningsblauw",
        "10933": "Bruin/Beige", "10934": "Groen/Wit", "10935": "Grijsgemêleerd/Wit",
        "10936": "Zwart/Wit", "10937": "Marineblauw/Wit", "10938": "Rood/Wit",
        "10939": "Koningsblauw/Wit", "10940": "Antraciet/Wit", "10941": "Turquoise/Wit",
        "12220": "Roest/Beige",
    },
    "PL": {
        "8747": "Biały", "8748": "Czarny", "8749": "Antracytowy", "8750": "Szary",
        "8751": "Granatowy", "8752": "Czerwony", "8753": "Królewski Niebieski",
        "10933": "Brązowy/Beżowy", "10934": "Zielony/Biały", "10935": "Szary Melanż/Biały",
        "10936": "Czarny/Biały", "10937": "Granatowy/Biały", "10938": "Czerwony/Biały",
        "10939": "Królewski Niebieski/Biały", "10940": "Antracytowy/Biały", "10941": "Turkusowy/Biały",
        "12220": "Rdzawy/Beżowy",
    },
    "SE": TRUCKER_COLORS_SE,
    "BE": {
        "8747": "Wit", "8748": "Zwart", "8749": "Antraciet", "8750": "Grijs",
        "8751": "Marineblauw", "8752": "Rood", "8753": "Koningsblauw",
        "10933": "Bruin/Beige", "10934": "Groen/Wit", "10935": "Grijsgemêleerd/Wit",
        "10936": "Zwart/Wit", "10937": "Marineblauw/Wit", "10938": "Rood/Wit",
        "10939": "Koningsblauw/Wit", "10940": "Antraciet/Wit", "10941": "Turquoise/Wit",
        "12220": "Roest/Beige",
    },
}


def build_trucker_listing_for_mkt(mkt_code, suffix, parent_sku, is_parent=False):
    """Build trucker listing for any marketplace."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang = LANG_TAGS[mkt_code]
    currency = CURRENCIES[mkt_code]
    price = TRUCKER_PRICES[mkt_code]
    trans = TRUCKER_TRANSLATIONS[mkt_code]

    if is_parent:
        color_value = "Mehrfarbig" if mkt_code == "DE" else ("Flerfärgad" if mkt_code == "SE" else "Multicolor")
        item_name = trans["item_name"]
    else:
        color_value = TRUCKER_COLORS.get(mkt_code, TRUCKER_COLORS_DE).get(suffix, TRUCKER_COLORS_DE.get(suffix, ""))
        item_name = f"{trans['item_name']} - {color_value}"

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt_id}],
        "color": [{"value": color_value, "language_tag": lang, "marketplace_id": mkt_id}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt_id}],
        "pattern": [{"value": trans["pattern"], "language_tag": lang, "marketplace_id": mkt_id}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt_id}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "model_name": [{"value": "Make America Great Again Trucker Cap", "language_tag": lang, "marketplace_id": mkt_id}],
        "age_range_description": [{"value": trans["age"], "language_tag": lang, "marketplace_id": mkt_id}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mkt_id}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt_id} for b in trans["bullets"]],
        "product_description": [{"value": trans["description"], "language_tag": lang, "marketplace_id": mkt_id}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt_id}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt_id}],
        "generic_keyword": [{"value": trans["keywords"], "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS.get(mkt_code, "as3"), "size_class": "alpha", "marketplace_id": mkt_id}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt_id}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt_id}],
        "list_price": [{"value_with_tax": price, "currency": currency, "marketplace_id": mkt_id}],
        "batteries_required": [{"value": False, "marketplace_id": mkt_id}],
        "fabric_type": [{"value": trans["fabric"], "language_tag": lang, "marketplace_id": mkt_id}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt_id}],
        "material": [{"value": trans["material"], "language_tag": lang, "marketplace_id": mkt_id}],
        "style": [{"value": trans["style"], "language_tag": lang, "marketplace_id": mkt_id}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mkt_id}],
        "care_instructions": [{"value": trans["care"], "language_tag": lang, "marketplace_id": mkt_id}],
        "unit_count": [{"type": {"value": "Count", "language_tag": "en_US"}, "value": 1.0, "marketplace_id": mkt_id}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt_id}],
    }

    if is_parent:
        attrs["parentage_level"] = [{"marketplace_id": mkt_id, "value": "parent"}]
        attrs["child_parent_sku_relationship"] = [{"marketplace_id": mkt_id, "child_relationship_type": "variation"}]
    else:
        attrs["fulfillment_availability"] = [{"fulfillment_channel_code": "DEFAULT", "quantity": 999}]
        sale_price = round(price * 0.85, 2)  # ~15% discount as sale price
        attrs["purchasable_offer"] = [{"currency": currency, "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": sale_price}]}], "marketplace_id": mkt_id}]
        attrs["parentage_level"] = [{"marketplace_id": mkt_id, "value": "child"}]
        attrs["child_parent_sku_relationship"] = [{"marketplace_id": mkt_id, "child_relationship_type": "variation", "parent_sku": parent_sku}]

    return attrs


# ── Task Functions ────────────────────────────────────────────────────

def task_propagate_se(dry_run=False):
    """Task 1: Propagate missing listings to Sweden.

    WARNING: This function creates separate NF and FL parent listings per country,
    which is the OLD (incorrect) approach. The correct architecture is ONE parent
    per country with STYLE_NAME/COLOR_NAME variation theme (see UNIFIED_PARENTS).
    Do not use this for new listing creation. Use UNIFIED_PARENTS pattern instead.
    """
    print("=" * 70)
    print("TASK 1: Propagate missing MAGA listings to Sweden (SE)")
    print("WARNING: This task uses deprecated separate NF/FL parents.")
    print("         New listings should use UNIFIED_PARENTS with STYLE_NAME/COLOR_NAME.")
    print("=" * 70)

    created = 0
    skipped = 0
    errors = 0

    # Process each country family
    for country_name, data in COUNTRY_DATA.items():
        # NF parent
        nf_parent = data.get("nf_parent")
        if nf_parent:
            print(f"\n--- {country_name} NF ({nf_parent}) ---")

            # Check if parent already exists on SE
            if check_listing_exists(nf_parent, SE_MKT):
                print(f"  Parent {nf_parent} already exists on SE, skipping")
                skipped += 1
            else:
                country_en = data["country_en"]
                attrs = build_se_dad_hat_parent_attrs(country_en, is_flag=False)
                status, resp = put_listing(nf_parent, SE_MKT, attrs, dry_run=dry_run)
                if status in (200, 202):
                    created += 1
                else:
                    errors += 1
                time.sleep(1)

            # Children
            for suffix in NF_CHILDREN:
                child_sku = f"{nf_parent}-{suffix}"
                if check_listing_exists(child_sku, SE_MKT):
                    print(f"  Child {child_sku} already exists on SE, skipping")
                    skipped += 1
                    continue

                attrs = build_se_dad_hat_child_attrs(nf_parent, suffix, data["country_en"], is_flag=False)
                status, resp = put_listing(child_sku, SE_MKT, attrs, dry_run=dry_run)
                if status in (200, 202):
                    created += 1
                else:
                    errors += 1
                time.sleep(1)

        # FL parent
        fl_parent = data.get("fl_parent")
        if fl_parent:
            print(f"\n--- {country_name} FL ({fl_parent}) ---")

            if check_listing_exists(fl_parent, SE_MKT):
                print(f"  Parent {fl_parent} already exists on SE, skipping")
                skipped += 1
            else:
                country_en = data["country_en"]
                attrs = build_se_dad_hat_parent_attrs(country_en, is_flag=True)
                status, resp = put_listing(fl_parent, SE_MKT, attrs, dry_run=dry_run)
                if status in (200, 202):
                    created += 1
                else:
                    errors += 1
                time.sleep(1)

            # Children - check which suffixes actually exist on DE first
            for suffix in FL_CHILDREN:
                child_sku = f"{fl_parent}-{suffix}"
                if check_listing_exists(child_sku, SE_MKT):
                    print(f"  Child {child_sku} already exists on SE, skipping")
                    skipped += 1
                    continue

                # Verify child exists on DE before creating on SE
                if not check_listing_exists(child_sku, MARKETPLACE_IDS["DE"]):
                    print(f"  Child {child_sku} does not exist on DE, skipping")
                    skipped += 1
                    continue

                attrs = build_se_dad_hat_child_attrs(fl_parent, suffix, data["country_en"], is_flag=True)
                status, resp = put_listing(child_sku, SE_MKT, attrs, dry_run=dry_run)
                if status in (200, 202):
                    created += 1
                else:
                    errors += 1
                time.sleep(1)

    # Trucker family
    print(f"\n--- Trucker (PFT-90229846) ---")
    trucker_parent = "PFT-90229846"
    if check_listing_exists(trucker_parent, SE_MKT):
        print(f"  Parent {trucker_parent} already exists on SE, skipping")
        skipped += 1
    else:
        attrs = build_se_trucker_parent_attrs()
        status, resp = put_listing(trucker_parent, SE_MKT, attrs, dry_run=dry_run)
        if status in (200, 202):
            created += 1
        else:
            errors += 1
        time.sleep(1)

    for suffix in TRUCKER_CHILDREN:
        child_sku = f"{trucker_parent}-{suffix}"
        if check_listing_exists(child_sku, SE_MKT):
            print(f"  Child {child_sku} already exists on SE, skipping")
            skipped += 1
            continue

        attrs = build_se_trucker_child_attrs(trucker_parent, suffix)
        status, resp = put_listing(child_sku, SE_MKT, attrs, dry_run=dry_run)
        if status in (200, 202):
            created += 1
        else:
            errors += 1
        time.sleep(1)

    # MEGA parents for SE
    mega_parents = {
        "PFT-MEGA-FR": "France",
        "PFT-MEGA-IT": "Italy",
        "PFT-MEGA-PL": "Poland",
        "PFT-MEGA-BE2": "Belgium",
        "PFT-MEGA-NL2": "Netherlands",
        "PFT-MEGA-GB": "Great Britain",
    }

    print(f"\n--- MEGA Parents ---")
    for mega_sku, country_en in mega_parents.items():
        if check_listing_exists(mega_sku, SE_MKT):
            print(f"  {mega_sku} already exists on SE, skipping")
            skipped += 1
            continue

        # MEGA parents use STYLE_NAME/COLOR_NAME variation theme
        attrs = build_se_dad_hat_parent_attrs(country_en, is_flag=False)
        attrs["variation_theme"] = [{"name": "STYLE_NAME/COLOR_NAME"}]
        status, resp = put_listing(mega_sku, SE_MKT, attrs, dry_run=dry_run)
        if status in (200, 202):
            created += 1
        else:
            errors += 1
        time.sleep(1)

    print(f"\n{'='*70}")
    print(f"SE Propagation complete: {created} created, {skipped} skipped, {errors} errors")
    print(f"{'='*70}")
    return created, skipped, errors


def task_create_parents(dry_run=False):
    """Task 2: Create missing parent SKUs for orphaned families.

    WARNING: This function creates separate NF/FL parent listings, which is
    the OLD (incorrect) approach. The correct architecture is ONE unified parent
    per country with STYLE_NAME/COLOR_NAME variation theme (see UNIFIED_PARENTS).
    Do not create new separate parents. Use UNIFIED_PARENTS pattern instead.
    """
    print("=" * 70)
    print("TASK 2: Create missing parent SKUs on DE")
    print("WARNING: This task uses deprecated separate NF/FL parents.")
    print("         New listings should use UNIFIED_PARENTS with STYLE_NAME/COLOR_NAME.")
    print("=" * 70)

    created = 0
    errors = 0
    de_mkt = MARKETPLACE_IDS["DE"]
    lang = "de_DE"

    orphans = [
        {
            "parent_sku": "PFT-82980216",
            "country_en": "Europe",
            "item_name_de": "Bestickte Kappe Make Europe Great Again mit Flagge - Verstellbare Dad Hat aus Baumwolle Unisex Baseball Cap",
            "model_name": "Make Europe Great Again Dad Hat",
            "keywords": "bestickte kappe europa eu patriot geschenk baseball cap dad hat baumwolle verstellbar make europe great again flagge unisex",
            "is_flag": True,
            "children": ["8745"],
        },
        {
            "parent_sku": "PFT-93856269",
            "country_en": "Poland",
            "item_name_de": "Bestickte Kappe Make Poland Great Again - Verstellbare Dad Hat aus Baumwolle Unisex Baseball Cap",
            "model_name": "Make Poland Great Again Dad Hat",
            "keywords": "bestickte kappe polen patriot geschenk baseball cap dad hat baumwolle verstellbar make poland great again unisex",
            "is_flag": False,
            "children": NF_CHILDREN,
        },
        {
            "parent_sku": "PFT-93856295",
            "country_en": "Italy",
            "item_name_de": "Bestickte Kappe Make Italy Great Again - Verstellbare Dad Hat aus Baumwolle Unisex Baseball Cap",
            "model_name": "Make Italy Great Again Dad Hat",
            "keywords": "bestickte kappe italien patriot geschenk baseball cap dad hat baumwolle verstellbar make italy great again unisex",
            "is_flag": False,
            "children": NF_CHILDREN,
        },
    ]

    for orphan in orphans:
        parent_sku = orphan["parent_sku"]
        print(f"\n--- Creating parent {parent_sku} ({orphan['country_en']}) ---")

        # Check if parent exists
        if check_listing_exists(parent_sku, de_mkt):
            print(f"  Parent {parent_sku} already exists on DE, skipping")
            continue

        flag_str = " mit Flagge" if orphan["is_flag"] else ""

        bullets = [
            "PREMIUM MASCHINENSTICKEREI - Professionell gestickter Schriftzug auf der Vorderseite. Stickerei verblasst nicht, blättert nicht ab und reißt nicht wie Drucke",
            "BEQUEM FÜR DEN ALLTAG - Aus 100% weicher Baumwolle Chino Twill. Niedriges Profil und vorgebogener Schirm für den klassischen Dad-Hat-Look",
            "VERSTELLBARE PASSFORM - Metallschnallenverschluss hinten passt sich jedem Kopfumfang an. Einheitsgröße für Damen und Herren",
            "PERFEKTES GESCHENK - Ideales Geschenk für Patrioten, Geburtstage, Feiertage oder einfach als Statement-Accessoire",
            "VIELSEITIG EINSETZBAR - Perfekt für Freizeit, Sport, Reisen, Festivals und den täglichen Gebrauch bei jedem Wetter",
        ]

        description = (
            f"Diese hochwertige bestickte Baseball Cap mit dem Schriftzug Make {orphan['country_en']} Great Again{flag_str} "
            f"ist der perfekte Begleiter für jeden Tag. Gefertigt aus 100% Baumwolle Chino Twill bietet sie "
            f"hervorragenden Tragekomfort und Atmungsaktivität."
        )

        attrs = {
            "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": de_mkt}],
            "color": [{"value": "Mehrfarbig", "language_tag": lang, "marketplace_id": de_mkt}],
            "variation_theme": [{"name": "COLOR"}],
            "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": de_mkt}],
            "pattern": [{"value": "Buchstabenmuster", "language_tag": lang, "marketplace_id": de_mkt}],
            "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": de_mkt}],
            "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": de_mkt}],
            "model_name": [{"value": orphan["model_name"], "language_tag": lang, "marketplace_id": de_mkt}],
            "age_range_description": [{"value": "Erwachsener", "language_tag": lang, "marketplace_id": de_mkt}],
            "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": de_mkt}],
            "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": de_mkt} for b in bullets],
            "product_description": [{"value": description, "language_tag": lang, "marketplace_id": de_mkt}],
            "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": de_mkt}],
            "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": de_mkt}],
            "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": de_mkt}],
            "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": de_mkt}],
            "generic_keyword": [{"value": orphan["keywords"], "language_tag": lang, "marketplace_id": de_mkt}],
            "gpsr_safety_attestation": [{"value": True, "marketplace_id": de_mkt}],
            "headwear_size": [{"size": "one_size", "size_system": "as3", "size_class": "alpha", "marketplace_id": de_mkt}],
            "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": de_mkt}],
            "country_of_origin": [{"value": "CN", "marketplace_id": de_mkt}],
            "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": de_mkt}],
            "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": de_mkt}],
            "item_name": [{"value": orphan["item_name_de"], "language_tag": lang, "marketplace_id": de_mkt}],
            "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": de_mkt}],
            "list_price": [{"value_with_tax": DAD_HAT_PRICES["DE"], "currency": "EUR", "marketplace_id": de_mkt}],
            "batteries_required": [{"value": False, "marketplace_id": de_mkt}],
            "fabric_type": [{"value": "100% Cotton", "language_tag": lang, "marketplace_id": de_mkt}],
            "condition_type": [{"value": "new_new", "marketplace_id": de_mkt}],
            "material": [{"value": "Baumwolle", "language_tag": lang, "marketplace_id": de_mkt}],
            "style": [{"value": "Klassisch", "language_tag": lang, "marketplace_id": de_mkt}],
            "hat_form_type": [{"value": "baseball_cap", "marketplace_id": de_mkt}],
            "care_instructions": [{"value": "Handwäsche", "language_tag": lang, "marketplace_id": de_mkt}],
            "unit_count": [{"type": {"value": "stück", "language_tag": lang}, "value": 1.0, "marketplace_id": de_mkt}],
            "target_gender": [{"value": "unisex", "marketplace_id": de_mkt}],
            "parentage_level": [{"marketplace_id": de_mkt, "value": "parent"}],
            "child_parent_sku_relationship": [{"marketplace_id": de_mkt, "child_relationship_type": "variation"}],
        }

        status, resp = put_listing(parent_sku, de_mkt, attrs, dry_run=dry_run)
        if status in (200, 202):
            created += 1
        else:
            errors += 1
        time.sleep(2)

        # Now link existing children to this parent via PATCH
        print(f"  Linking {len(orphan['children'])} children to parent {parent_sku}...")
        for suffix in orphan["children"]:
            child_sku = f"{parent_sku}-{suffix}"
            if not check_listing_exists(child_sku, de_mkt):
                print(f"    Child {child_sku} does not exist on DE, skipping")
                continue

            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/child_parent_sku_relationship",
                    "value": [{"marketplace_id": de_mkt, "child_relationship_type": "variation", "parent_sku": parent_sku}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/parentage_level",
                    "value": [{"marketplace_id": de_mkt, "value": "child"}]
                },
            ]

            status, resp = patch_listing(child_sku, de_mkt, patches, dry_run=dry_run)
            if status in (200, 202):
                created += 1
            else:
                errors += 1
            time.sleep(1)

    print(f"\n{'='*70}")
    print(f"Parent creation complete: {created} created/linked, {errors} errors")
    print(f"{'='*70}")
    return created, errors


def task_trucker_propagate(dry_run=False):
    """Task 3: Propagate Trucker hat to all 7 other EU marketplaces."""
    print("=" * 70)
    print("TASK 3: Propagate Trucker hat to FR, IT, ES, NL, PL, SE, BE")
    print("=" * 70)

    trucker_parent = "PFT-90229846"
    target_mkts = ["FR", "IT", "ES", "NL", "PL", "SE", "BE"]

    created = 0
    skipped = 0
    errors = 0

    # First, get images from DE parent and children to replicate
    print("\nFetching image URLs from DE listings...")
    de_images = {}
    for suffix in TRUCKER_CHILDREN:
        child_sku = f"{trucker_parent}-{suffix}"
        data = get_listing_full(child_sku, MARKETPLACE_IDS["DE"])
        attrs = data.get("attributes", {})
        images = {}
        if "main_product_image_locator" in attrs:
            images["main"] = attrs["main_product_image_locator"][0].get("media_location", "")
        for i in range(1, 8):
            key = f"other_product_image_locator_{i}"
            if key in attrs:
                images[f"other_{i}"] = attrs[key][0].get("media_location", "")
        de_images[suffix] = images
        time.sleep(0.5)

    print(f"  Collected images for {len(de_images)} children")

    for mkt_code in target_mkts:
        mkt_id = MARKETPLACE_IDS[mkt_code]
        print(f"\n{'='*50}")
        print(f"  Marketplace: {mkt_code} ({mkt_id})")
        print(f"{'='*50}")

        # Parent
        if check_listing_exists(trucker_parent, mkt_id):
            print(f"  Parent {trucker_parent} already exists on {mkt_code}, skipping")
            skipped += 1
        else:
            attrs = build_trucker_listing_for_mkt(mkt_code, None, trucker_parent, is_parent=True)
            status, resp = put_listing(trucker_parent, mkt_id, attrs, dry_run=dry_run)
            if status in (200, 202):
                created += 1
            else:
                errors += 1
            time.sleep(1.5)

        # Children
        for suffix in TRUCKER_CHILDREN:
            child_sku = f"{trucker_parent}-{suffix}"

            if check_listing_exists(child_sku, mkt_id):
                print(f"  Child {child_sku} already exists on {mkt_code}, skipping")
                skipped += 1
                continue

            attrs = build_trucker_listing_for_mkt(mkt_code, suffix, trucker_parent, is_parent=False)

            # Add images from DE
            imgs = de_images.get(suffix, {})
            if imgs.get("main"):
                attrs["main_product_image_locator"] = [{"media_location": imgs["main"], "marketplace_id": mkt_id}]
            for i in range(1, 8):
                key = f"other_{i}"
                if imgs.get(key):
                    attrs[f"other_product_image_locator_{i}"] = [{"media_location": imgs[key], "marketplace_id": mkt_id}]

            status, resp = put_listing(child_sku, mkt_id, attrs, dry_run=dry_run)
            if status in (200, 202):
                created += 1
            else:
                errors += 1
            time.sleep(1)

    print(f"\n{'='*70}")
    print(f"Trucker propagation complete: {created} created, {skipped} skipped, {errors} errors")
    print(f"{'='*70}")
    return created, skipped, errors


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Amazon Listings Management")
    parser.add_argument("--task", required=True,
                       choices=["propagate_se", "create_parents", "trucker_propagate", "all"],
                       help="Task to run")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually create listings")
    args = parser.parse_args()

    print(f"\n{'#'*70}")
    print(f"# Amazon Listings Manager - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Task: {args.task} {'(DRY RUN)' if args.dry_run else ''}")
    print(f"{'#'*70}\n")

    if args.task in ("create_parents", "all"):
        task_create_parents(dry_run=args.dry_run)
        print()

    if args.task in ("propagate_se", "all"):
        task_propagate_se(dry_run=args.dry_run)
        print()

    if args.task in ("trucker_propagate", "all"):
        task_trucker_propagate(dry_run=args.dry_run)
        print()

    print("\nAll tasks complete!")


if __name__ == "__main__":
    main()
