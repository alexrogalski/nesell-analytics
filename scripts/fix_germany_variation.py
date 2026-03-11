#!/usr/bin/env python3.11
"""
Fix Germany listing to use proper 2-dimensional variation:
  Dropdown 1: Style (mit Flagge / ohne Flagge)
  Dropdown 2: Color (Schwarz, Weiss, Navy, etc.)

Currently all children have color values like "Schwarz - ohne Flagge" in a single
COLOR variation theme. This script converts to a 2D variation (e.g. STYLE_NAME/COLOR)
so customers see TWO separate dropdowns on the product page.

Steps:
  1. Research: fetch HAT product type definition to find available 2D variation themes
  2. Inspect: read current state of parent + children on DE
  3. Update parent: change variation_theme to 2D theme
  4. Update old children: set color="{pure color}", style_name="ohne Flagge"
  5. Update new children: set color="{pure color}", style_name="mit Flagge"
  6. Repeat for all 8 EU marketplaces

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/fix_germany_variation.py --step research    # discover themes
  python3.11 scripts/fix_germany_variation.py --step inspect     # current state
  python3.11 scripts/fix_germany_variation.py --step update      # apply changes
  python3.11 scripts/fix_germany_variation.py --step update --marketplace DE  # DE only
  python3.11 scripts/fix_germany_variation.py --dry-run          # preview without API calls
"""

import argparse
import json
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests

# ── Credentials ──────────────────────────────────────────────────────
KEYS_DIR = Path.home() / ".keys"
AMZ_CREDS = json.loads((KEYS_DIR / "amazon-sp-api.json").read_text())
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"

# ── Constants ────────────────────────────────────────────────────────

OLD_PARENT_SKU = "MEGA-GERMANY-OLD"

# Old children (without flag) - 11 SKUs
OLD_CHILDREN = [
    "Y5-IW2B-Y8AD",
    "ZU-EMW9-S0MS",
    "OV-J9GP-55Y0",
    "WN-HSFK-JBNT",
    "QL-PMAX-ACJ5",
    "Y0-SU3Z-EWFH",
    "AO-6Q5S-1H0K",
    "9C-WZW7-8BRO",
    "E3-XAV1-K2X8",
    "S1-R7LS-OW9V",
    "SU-HHEA-WWB5",
]

# New children (with flag) - 11 SKUs (including 8745 Spruce and 12735 Cranberry)
NEW_CHILD_SUFFIXES = [
    "7853", "7854", "7855", "7856", "7857",
    "7858", "7859", "8745", "9794", "12735", "12736"
]
NEW_CHILDREN = [f"PFT-100032925-{s}" for s in NEW_CHILD_SUFFIXES]

# DO NOT TOUCH
PROTECTED_SKUS = ["PFT-90034427-12735", "PFT-90034427-8745"]

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

LANG_TAGS = {
    "DE": "de_DE", "FR": "fr_FR", "IT": "it_IT", "ES": "es_ES",
    "NL": "nl_NL", "PL": "pl_PL", "SE": "sv_SE", "BE": "nl_BE",
}

# Localized flag labels for the STYLE dimension
FLAG_LABELS = {
    "DE": {"with": "mit Flagge",     "without": "ohne Flagge"},
    "FR": {"with": "avec Drapeau",   "without": "sans Drapeau"},
    "IT": {"with": "con Bandiera",   "without": "senza Bandiera"},
    "ES": {"with": "con Bandera",    "without": "sin Bandera"},
    "NL": {"with": "met Vlag",       "without": "zonder Vlag"},
    "PL": {"with": "z Flaga",        "without": "bez Flagi"},
    "SE": {"with": "med Flagga",     "without": "utan Flagga"},
    "BE": {"with": "avec Drapeau",   "without": "sans Drapeau"},
}

# Color names for NEW children (with flag) per marketplace
# Key = variant suffix (from PFT-100032925-{suffix})
NEW_COLORS = {
    "DE": {
        "7853": "Weiss", "7854": "Schwarz", "7855": "Beige", "7856": "Hellblau",
        "7857": "Marineblau", "7858": "Rosa", "7859": "Steingrau",
        "8745": "Fichte", "9794": "Gruenes Tarnmuster", "12735": "Cranberry",
        "12736": "Dunkelgrau",
    },
    "FR": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Sapin", "9794": "Camouflage Vert", "12735": "Canneberge",
        "12736": "Gris Fonce",
    },
    "IT": {
        "7853": "Bianco", "7854": "Nero", "7855": "Beige", "7856": "Azzurro",
        "7857": "Blu Navy", "7858": "Rosa", "7859": "Grigio Pietra",
        "8745": "Abete", "9794": "Mimetico Verde", "12735": "Mirtillo Rosso",
        "12736": "Grigio Scuro",
    },
    "ES": {
        "7853": "Blanco", "7854": "Negro", "7855": "Beige", "7856": "Azul Claro",
        "7857": "Azul Marino", "7858": "Rosa", "7859": "Gris Piedra",
        "8745": "Abeto", "9794": "Camuflaje Verde", "12735": "Arandano",
        "12736": "Gris Oscuro",
    },
    "NL": {
        "7853": "Wit", "7854": "Zwart", "7855": "Beige", "7856": "Lichtblauw",
        "7857": "Marineblauw", "7858": "Roze", "7859": "Steengrijs",
        "8745": "Spar", "9794": "Groen Camouflage", "12735": "Cranberry",
        "12736": "Donkergrijs",
    },
    "PL": {
        "7853": "Bialy", "7854": "Czarny", "7855": "Bezowy", "7856": "Jasnoniebieski",
        "7857": "Granatowy", "7858": "Rozowy", "7859": "Szary Kamien",
        "8745": "Swierk", "9794": "Zielony Kamuflaz", "12735": "Zurawina",
        "12736": "Ciemnoszary",
    },
    "SE": {
        "7853": "Vit", "7854": "Svart", "7855": "Beige", "7856": "Ljusbla",
        "7857": "Marinbla", "7858": "Rosa", "7859": "Stengra",
        "8745": "Gran", "9794": "Gron kamouflage", "12735": "Tranbar",
        "12736": "Morkgra",
    },
    "BE": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Sapin", "9794": "Camouflage Vert", "12735": "Canneberge",
        "12736": "Gris Fonce",
    },
}

SLEEP_BETWEEN = 0.6


# ── Amazon SP-API Client ─────────────────────────────────────────────

class AmazonAPI:
    def __init__(self, creds):
        self.creds = creds
        self._token = None
        self._token_time = 0

    def _refresh(self):
        r = requests.post("https://api.amazon.com/auth/o2/token", data={
            "grant_type": "refresh_token",
            "refresh_token": self.creds["refresh_token"],
            "client_id": self.creds["client_id"],
            "client_secret": self.creds["client_secret"],
        })
        data = r.json()
        self._token = data.get("access_token")
        self._token_time = time.time()
        if not self._token:
            print(f"  [AUTH ERROR] {data}")
            sys.exit(1)
        print(f"  Token obtained: {self._token[:20]}...")

    def _headers(self):
        if not self._token or time.time() - self._token_time > 3000:
            self._refresh()
        return {"x-amz-access-token": self._token, "Content-Type": "application/json"}

    def get(self, path, params=None, retries=5):
        """Generic GET with retry."""
        url = f"{AMZ_BASE}{path}"
        for attempt in range(retries):
            try:
                r = requests.get(url, headers=self._headers(), params=params, timeout=30)
            except Exception as e:
                print(f"    [GET] connection error: {e}, retry {attempt+1}/{retries}")
                time.sleep(3)
                continue
            if r.status_code == 429:
                wait = min(3 * (2 ** attempt), 30)
                print(f"    [GET] 429 rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(1)
                continue
            if r.status_code >= 500:
                time.sleep(5)
                continue
            return r.status_code, r.json() if r.text else {}
        return 0, {}

    def get_listing(self, sku, mp_id, retries=5):
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
        params = {"marketplaceIds": mp_id, "issueLocale": "en_US",
                  "includedData": "summaries,attributes,issues"}
        for attempt in range(retries):
            try:
                r = requests.get(url, headers=self._headers(), params=params, timeout=30)
            except Exception as e:
                print(f"    [GET {sku}] connection error: {e}, retry {attempt+1}/{retries}")
                time.sleep(3)
                continue
            if r.status_code == 429:
                wait = min(3 * (2 ** attempt), 30)
                print(f"    [GET {sku}] 429 rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(1)
                continue
            if r.status_code >= 500:
                time.sleep(5)
                continue
            return r.status_code, r.json() if r.text else {}
        return 0, {}

    def patch_listing(self, sku, mp_id, patches, product_type="HAT", retries=8):
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
        body = {"productType": product_type, "patches": patches}
        params = {"marketplaceIds": mp_id, "issueLocale": "en_US"}
        for attempt in range(retries):
            try:
                r = requests.patch(url, headers=self._headers(), json=body,
                                   params=params, timeout=30)
            except Exception as e:
                print(f"    [PATCH {sku}] connection error: {e}, retry {attempt+1}/{retries}")
                time.sleep(5 * (attempt + 1))
                continue
            if r.status_code == 429:
                wait = min(5 * (2 ** attempt), 60)
                print(f"    [PATCH {sku}] 429 rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(3)
                continue
            if r.status_code >= 500:
                time.sleep(5 * (attempt + 1))
                continue
            resp = r.json() if r.text else {}
            return r.status_code, resp
        return 0, {"error": "retries exhausted"}

    def put_listing(self, sku, mp_id, body, retries=8):
        """PUT (full replacement) for parent listings that need variation_theme change."""
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
        params = {"marketplaceIds": mp_id, "issueLocale": "en_US"}
        for attempt in range(retries):
            try:
                r = requests.put(url, headers=self._headers(), json=body,
                                 params=params, timeout=30)
            except Exception as e:
                print(f"    [PUT {sku}] connection error: {e}, retry {attempt+1}/{retries}")
                time.sleep(5 * (attempt + 1))
                continue
            if r.status_code == 429:
                wait = min(5 * (2 ** attempt), 60)
                print(f"    [PUT {sku}] 429 rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(3)
                continue
            if r.status_code >= 500:
                time.sleep(5 * (attempt + 1))
                continue
            resp = r.json() if r.text else {}
            return r.status_code, resp
        return 0, {"error": "retries exhausted"}


# ── Logging ──────────────────────────────────────────────────────────

def log_result(results, action, sku, mkt, status_code, resp):
    """Append to results list and print."""
    issues = resp.get("issues", []) if isinstance(resp, dict) else []
    errors = [i for i in issues if i.get("severity") == "ERROR"]
    warnings = [i for i in issues if i.get("severity") == "WARNING"]
    status = resp.get("status", "?") if isinstance(resp, dict) else "?"

    raw_errors = resp.get("errors", []) if isinstance(resp, dict) else []
    if raw_errors and not errors:
        errors = raw_errors

    entry = {
        "action": action,
        "sku": sku,
        "marketplace": mkt,
        "http_status": status_code,
        "api_status": status,
        "errors": [f"{e.get('code','')}: {e.get('message','')}" for e in errors],
        "warnings": [f"{w.get('code','')}: {w.get('message','')}" for w in warnings],
    }
    results.append(entry)

    error_str = ""
    if entry["errors"]:
        error_str = f" | ERRORS: {'; '.join(entry['errors'][:3])}"
    elif entry["warnings"]:
        error_str = f" | WARN: {'; '.join(entry['warnings'][:2])}"

    icon = "OK" if status_code in (200, 204) and not entry["errors"] else "ERR"
    print(f"  [{icon}] {action} {sku} on {mkt}: HTTP {status_code} -> {status}{error_str}")


# ═══════════════════════════════════════════════════════════════════════
# STEP 1: RESEARCH - Fetch product type definition for HAT
# ═══════════════════════════════════════════════════════════════════════

def step_research(api):
    """Fetch the HAT product type definition from Amazon to discover 2D variation themes."""
    print("\n" + "=" * 70)
    print("  STEP: RESEARCH HAT PRODUCT TYPE DEFINITION")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    # Get the product type definition for HAT
    path = "/definitions/2020-09-01/productTypes/HAT"
    params = {
        "marketplaceIds": de_mp,
        "requirements": "LISTING",
        "locale": "de_DE",
    }

    print(f"\n  Fetching HAT product type definition...")
    code, data = api.get(path, params)
    print(f"  HTTP {code}")

    if code != 200:
        print(f"  ERROR: {json.dumps(data, indent=2)[:500]}")
        return

    # Save full definition for analysis
    output_path = Path(__file__).parent / "hat_product_type_definition.json"
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Full definition saved to: {output_path}")

    # Extract variation_theme info
    schema = data.get("schema", {})
    properties = schema.get("properties", {})

    # Look for variation_theme
    vt = properties.get("variation_theme", {})
    print(f"\n  --- variation_theme ---")
    if vt:
        items = vt.get("items", {})
        vt_props = items.get("properties", {})
        # The variation_theme property usually has a name subfield with enum
        name_prop = vt_props.get("name", {})
        enum_values = name_prop.get("enum", [])
        print(f"  Allowed variation_theme names ({len(enum_values)}):")
        for v in sorted(enum_values):
            print(f"    - {v}")
    else:
        print("  variation_theme not found in schema properties!")

    # Look for attributes that could serve as 2D dimensions
    print(f"\n  --- Key attributes ---")
    for attr_name in ["style_name", "pattern", "size", "color", "material",
                       "style", "design", "theme", "model_name",
                       "item_name", "department_name"]:
        attr = properties.get(attr_name, {})
        if attr:
            items = attr.get("items", {})
            props = items.get("properties", {}) if items else {}
            value_prop = props.get("value", {})
            has_enum = "enum" in value_prop
            enum_count = len(value_prop.get("enum", []))
            print(f"  {attr_name}: exists=True, has_enum={has_enum}, enum_count={enum_count}")
            if has_enum and enum_count < 30:
                print(f"    enum values: {value_prop['enum'][:20]}")
        else:
            print(f"  {attr_name}: exists=False")

    print("\n  Research complete. Examine hat_product_type_definition.json for full details.\n")


# ═══════════════════════════════════════════════════════════════════════
# STEP 2: INSPECT - Read current state
# ═══════════════════════════════════════════════════════════════════════

def step_inspect(api):
    """Inspect parent and all children on DE to understand current state."""
    print("\n" + "=" * 70)
    print("  STEP: INSPECT CURRENT STATE (DE)")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    # Parent
    print(f"\n  --- Parent: {OLD_PARENT_SKU} ---")
    code, data = api.get_listing(OLD_PARENT_SKU, de_mp)
    print(f"  HTTP {code}")
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {json.dumps(attrs.get('variation_theme', []))}")
        print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
        print(f"  color: {json.dumps(attrs.get('color', []))}")
        print(f"  style_name: {json.dumps(attrs.get('style_name', []))}")
        print(f"  size: {json.dumps(attrs.get('size', []))}")
        summaries = data.get("summaries", [])
        if summaries:
            s = summaries[0]
            print(f"  ASIN: {s.get('asin','?')}")
            print(f"  status: {json.dumps(s.get('status', []))}")
        issues = data.get("issues", [])
        if issues:
            print(f"  Issues ({len(issues)}):")
            for issue in issues[:5]:
                print(f"    [{issue.get('severity','')}] {issue.get('code','')}: {issue.get('message','')[:100]}")
    time.sleep(SLEEP_BETWEEN)

    # Old children (sample)
    for sku in OLD_CHILDREN[:3]:
        print(f"\n  --- Old child: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {json.dumps(attrs.get('color', []))}")
            print(f"  style_name: {json.dumps(attrs.get('style_name', []))}")
            print(f"  size: {json.dumps(attrs.get('size', []))}")
            print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
            print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                s = summaries[0]
                print(f"  ASIN: {s.get('asin','?')}, status: {json.dumps(s.get('status', []))}")
        time.sleep(SLEEP_BETWEEN)

    # New children (sample)
    for sku in NEW_CHILDREN[:3]:
        print(f"\n  --- New child: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {json.dumps(attrs.get('color', []))}")
            print(f"  style_name: {json.dumps(attrs.get('style_name', []))}")
            print(f"  size: {json.dumps(attrs.get('size', []))}")
            print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
            print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                s = summaries[0]
                print(f"  ASIN: {s.get('asin','?')}, status: {json.dumps(s.get('status', []))}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Inspect complete.\n")


# ═══════════════════════════════════════════════════════════════════════
# STEP 3: UPDATE - Apply 2D variation
# ═══════════════════════════════════════════════════════════════════════

def strip_flag_suffix(color_value, mkt_code):
    """Remove any existing ' - mit Flagge' / ' - ohne Flagge' suffix."""
    for suffix_key in ("without", "with"):
        suffix = f" - {FLAG_LABELS[mkt_code][suffix_key]}"
        if color_value.endswith(suffix):
            return color_value[:-len(suffix)]
    return color_value


def step_update(api, results, marketplaces=None, dry_run=False, theme_name="STYLE_NAME/COLOR_NAME"):
    """
    Apply 2D variation theme to the parent + update all children.

    For HAT product type on Amazon EU:
    - STYLE_NAME maps to 'style' attribute (free text, max 120 chars)
    - COLOR_NAME maps to 'color' attribute (free text, max 1000 chars)
    - The first part of the theme becomes the first dropdown on the product page
    """
    if marketplaces is None:
        marketplaces = list(MARKETPLACE_IDS.keys())

    # Map theme component names to actual attribute names in the schema
    THEME_TO_ATTR = {
        "STYLE_NAME": "style",
        "COLOR_NAME": "color",
        "COLOR": "color",
        "SIZE_NAME": "size",
        "SIZE": "size",
        "PATTERN_NAME": "pattern",
        "PATTERN": "pattern",
    }

    theme_parts = theme_name.split("/")
    if len(theme_parts) != 2:
        print(f"  ERROR: Expected 2D theme, got: {theme_name}")
        sys.exit(1)

    # For STYLE_NAME/COLOR_NAME: style_dim_attr="style", color_dim_attr="color"
    style_dim_attr = THEME_TO_ATTR.get(theme_parts[0], theme_parts[0].lower())
    color_dim_attr = THEME_TO_ATTR.get(theme_parts[1], theme_parts[1].lower())

    print(f"\n" + "=" * 70)
    print(f"  STEP: UPDATE TO 2D VARIATION ({theme_name})")
    print(f"  Style/flag dimension attribute: '{style_dim_attr}' (values: mit/ohne Flagge)")
    print(f"  Color dimension attribute: '{color_dim_attr}' (values: Schwarz, Weiss, etc.)")
    print(f"  Marketplaces: {', '.join(marketplaces)}")
    print(f"  Dry run: {dry_run}")
    print("=" * 70)

    for mkt_code in marketplaces:
        mp_id = MARKETPLACE_IDS[mkt_code]
        lang = LANG_TAGS[mkt_code]
        flag_with = FLAG_LABELS[mkt_code]["with"]
        flag_without = FLAG_LABELS[mkt_code]["without"]
        mkt_colors = NEW_COLORS.get(mkt_code, NEW_COLORS["DE"])

        print(f"\n  ===== {mkt_code} (mp={mp_id}) =====")
        print(f"  '{style_dim_attr}' values: '{flag_with}' / '{flag_without}'")

        # 3a. Update parent variation_theme
        print(f"\n  --- Updating parent {OLD_PARENT_SKU} variation_theme -> {theme_name} ---")
        parent_patches = [{
            "op": "replace",
            "path": "/attributes/variation_theme",
            "value": [{
                "name": theme_name,
                "marketplace_id": mp_id
            }]
        }]

        if dry_run:
            print(f"  [DRY] Would PATCH parent variation_theme to {theme_name}")
        else:
            code, resp = api.patch_listing(OLD_PARENT_SKU, mp_id, parent_patches)
            log_result(results, "update_parent_theme", OLD_PARENT_SKU, mkt_code, code, resp)
            time.sleep(SLEEP_BETWEEN)

        # 3b. Update old children: clean color + add style = "ohne Flagge"
        print(f"\n  --- Updating {len(OLD_CHILDREN)} old children ('{flag_without}') ---")

        # First pass on DE: cache the DE colors for fallback on other marketplaces
        de_mp = MARKETPLACE_IDS["DE"]

        for sku in OLD_CHILDREN:
            # Read current color from this marketplace
            code, data = api.get_listing(sku, mp_id)
            clean_color = ""
            if code == 200 and data:
                attrs = data.get("attributes", {})
                color_list = attrs.get(color_dim_attr, [])
                if color_list:
                    raw_color = color_list[0].get("value", "")
                    clean_color = strip_flag_suffix(raw_color, mkt_code)

            # If no color on this marketplace, try DE as fallback
            if not clean_color and mkt_code != "DE":
                code_de, data_de = api.get_listing(sku, de_mp)
                if code_de == 200 and data_de:
                    attrs_de = data_de.get("attributes", {})
                    color_list_de = attrs_de.get(color_dim_attr, [])
                    if color_list_de:
                        raw_de = color_list_de[0].get("value", "")
                        clean_color = strip_flag_suffix(raw_de, "DE")
                        print(f"  [INFO] {sku} on {mkt_code}: using DE color '{clean_color}' as fallback")
                time.sleep(SLEEP_BETWEEN)

            if not clean_color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color found (even from DE)")
                time.sleep(SLEEP_BETWEEN)
                continue

            # Build patches: set color + set style/flag dimension
            child_patches = [
                {
                    "op": "replace",
                    "path": f"/attributes/{color_dim_attr}",
                    "value": [{"value": clean_color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": f"/attributes/{style_dim_attr}",
                    "value": [{"value": flag_without, "language_tag": lang, "marketplace_id": mp_id}]
                }
            ]

            if dry_run:
                print(f"  [DRY] {sku}: {color_dim_attr}='{clean_color}', {style_dim_attr}='{flag_without}'")
            else:
                code, resp = api.patch_listing(sku, mp_id, child_patches)
                log_result(results, "update_old_child", sku, mkt_code, code, resp)
            time.sleep(SLEEP_BETWEEN)

        # 3c. Update new children: clean color + set style = "mit Flagge"
        print(f"\n  --- Updating {len(NEW_CHILDREN)} new children ('{flag_with}') ---")

        for suffix_id in NEW_CHILD_SUFFIXES:
            sku = f"PFT-100032925-{suffix_id}"
            base_color = mkt_colors.get(suffix_id, "")

            if not base_color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color mapping")
                continue

            # Read current to get actual color and strip suffix if needed
            code, data = api.get_listing(sku, mp_id)
            if code == 200 and data:
                attrs = data.get("attributes", {})
                color_list = attrs.get(color_dim_attr, [])
                if color_list:
                    raw_color = color_list[0].get("value", "")
                    clean_color = strip_flag_suffix(raw_color, mkt_code)
                    if clean_color:
                        base_color = clean_color  # Use actual API color, cleaned

            child_patches = [
                {
                    "op": "replace",
                    "path": f"/attributes/{color_dim_attr}",
                    "value": [{"value": base_color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": f"/attributes/{style_dim_attr}",
                    "value": [{"value": flag_with, "language_tag": lang, "marketplace_id": mp_id}]
                }
            ]

            if dry_run:
                print(f"  [DRY] {sku}: {color_dim_attr}='{base_color}', {style_dim_attr}='{flag_with}'")
            else:
                code, resp = api.patch_listing(sku, mp_id, child_patches)
                log_result(results, "update_new_child", sku, mkt_code, code, resp)
            time.sleep(SLEEP_BETWEEN)


# ═══════════════════════════════════════════════════════════════════════
# STEP 4: VERIFY - Check the result
# ═══════════════════════════════════════════════════════════════════════

def step_verify(api, marketplaces=None):
    """Verify the 2D variation is correctly set up."""
    if marketplaces is None:
        marketplaces = ["DE"]

    print("\n" + "=" * 70)
    print("  STEP: VERIFY 2D VARIATION")
    print("=" * 70)

    for mkt_code in marketplaces:
        mp_id = MARKETPLACE_IDS[mkt_code]
        print(f"\n  ===== {mkt_code} =====")

        # Check parent
        code, data = api.get_listing(OLD_PARENT_SKU, mp_id)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            vt = attrs.get("variation_theme", [])
            print(f"  Parent variation_theme: {json.dumps(vt)}")
        time.sleep(SLEEP_BETWEEN)

        # Check sample old child
        sku = OLD_CHILDREN[0]
        code, data = api.get_listing(sku, mp_id)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  Old child {sku}:")
            print(f"    color: {json.dumps(attrs.get('color', []))}")
            print(f"    style: {json.dumps(attrs.get('style', []))}")
            print(f"    style_name: {json.dumps(attrs.get('style_name', []))}")
            print(f"    size: {json.dumps(attrs.get('size', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                print(f"    ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
        time.sleep(SLEEP_BETWEEN)

        # Check sample new child
        sku = NEW_CHILDREN[0]
        code, data = api.get_listing(sku, mp_id)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  New child {sku}:")
            print(f"    color: {json.dumps(attrs.get('color', []))}")
            print(f"    style: {json.dumps(attrs.get('style', []))}")
            print(f"    style_name: {json.dumps(attrs.get('style_name', []))}")
            print(f"    size: {json.dumps(attrs.get('size', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                print(f"    ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Verify complete.\n")


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Fix Germany listing to use 2D variation")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without making API calls")
    parser.add_argument("--step", choices=["research", "inspect", "update", "verify"],
                        help="Run only a specific step")
    parser.add_argument("--marketplace", type=str, default=None,
                        help="Run on specific marketplace only (e.g. DE)")
    parser.add_argument("--theme", type=str, default=None,
                        help="Override variation theme (e.g. STYLE_NAME/COLOR, PATTERN/COLOR)")
    args = parser.parse_args()

    marketplaces = None
    if args.marketplace:
        marketplaces = [args.marketplace.upper()]
        if marketplaces[0] not in MARKETPLACE_IDS:
            print(f"  ERROR: Unknown marketplace {marketplaces[0]}")
            sys.exit(1)

    print("=" * 70)
    print("  FIX GERMANY VARIATION -> 2D (Flag x Color)")
    print(f"  Parent: {OLD_PARENT_SKU}")
    print(f"  Old children (ohne Flagge): {len(OLD_CHILDREN)} SKUs")
    print(f"  New children (mit Flagge): {len(NEW_CHILDREN)} SKUs")
    print(f"  Dry run: {args.dry_run}")
    if marketplaces:
        print(f"  Marketplace filter: {', '.join(marketplaces)}")
    else:
        print(f"  Marketplaces: ALL ({', '.join(MARKETPLACE_IDS.keys())})")
    print("=" * 70)

    # Safety check
    for sku in PROTECTED_SKUS:
        assert sku not in NEW_CHILDREN, f"SAFETY: {sku} is in NEW_CHILDREN!"
        assert sku not in OLD_CHILDREN, f"SAFETY: {sku} is in OLD_CHILDREN!"
    print("  Safety check passed.")

    api = AmazonAPI(AMZ_CREDS)
    results = []

    if args.step == "research":
        step_research(api)
        return

    if args.step == "inspect":
        step_inspect(api)
        return

    if args.step == "verify":
        step_verify(api, marketplaces)
        return

    if args.step == "update" or args.step is None:
        # Determine theme
        theme = args.theme
        if not theme:
            # Default: we need to discover this from research step first
            print("\n  No --theme specified. Running research first to discover available themes...")
            step_research(api)

            # Try to determine the best theme from the saved definition
            defn_path = Path(__file__).parent / "hat_product_type_definition.json"
            if defn_path.exists():
                defn = json.loads(defn_path.read_text())
                schema = defn.get("schema", {})
                properties = schema.get("properties", {})
                vt = properties.get("variation_theme", {})
                items = vt.get("items", {})
                vt_props = items.get("properties", {})
                name_prop = vt_props.get("name", {})
                enum_values = name_prop.get("enum", [])

                # Preference order for 2D themes with COLOR
                preferred = [
                    "STYLE_NAME/COLOR",
                    "STYLE/COLOR",
                    "PATTERN/COLOR",
                    "SIZE/COLOR",
                    "MATERIAL/COLOR",
                ]
                for pref in preferred:
                    if pref in enum_values:
                        theme = pref
                        print(f"\n  Auto-selected theme: {theme}")
                        break

                if not theme:
                    # Check for any theme with COLOR as second dimension
                    color_themes = [v for v in enum_values if "/COLOR" in v or "COLOR/" in v]
                    if color_themes:
                        theme = color_themes[0]
                        print(f"\n  Auto-selected first COLOR theme: {theme}")
                    else:
                        print(f"\n  ERROR: No suitable 2D variation theme found!")
                        print(f"  Available themes: {enum_values}")
                        print(f"  Re-run with --theme to specify manually.")
                        sys.exit(1)

        print(f"\n  Using variation theme: {theme}")

        # Run inspect first
        step_inspect(api)

        # Then update
        step_update(api, results, marketplaces, args.dry_run, theme)

        # Verify on DE
        if not args.dry_run:
            print("\n  Waiting 5s before verification...")
            time.sleep(5)
            step_verify(api, marketplaces or ["DE"])

        # Report
        print_report(results)


def print_report(results):
    """Print summary report."""
    print("\n" + "=" * 70)
    print("  FINAL REPORT")
    print("=" * 70)

    if not results:
        print("  No API calls were made (dry run or inspect only).")
        return

    actions = {}
    errors_list = []
    for r in results:
        action = r["action"]
        actions.setdefault(action, {"total": 0, "ok": 0, "err": 0})
        actions[action]["total"] += 1
        if r["http_status"] in (200, 204) and not r["errors"]:
            actions[action]["ok"] += 1
        else:
            actions[action]["err"] += 1
            if r["errors"]:
                errors_list.append(r)

    for action, counts in actions.items():
        print(f"  {action}: {counts['ok']}/{counts['total']} OK, {counts['err']} errors")

    if errors_list:
        print(f"\n  --- ERRORS ({len(errors_list)}) ---")
        for r in errors_list:
            print(f"  {r['action']} {r['sku']} on {r['marketplace']}: {'; '.join(r['errors'][:3])}")

    total_ok = sum(c["ok"] for c in actions.values())
    total_err = sum(c["err"] for c in actions.values())
    print(f"\n  Summary: {total_ok} OK, {total_err} errors out of {len(results)} API calls")

    # Save results
    results_path = Path(__file__).parent / "fix_germany_variation_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"  Results saved to: {results_path}")


if __name__ == "__main__":
    main()
