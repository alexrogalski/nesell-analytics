#!/usr/bin/env python3.11
"""
Merge Europe "with EU flag" and "without flag" listings into a single
2D variation family (STYLE_NAME/COLOR_NAME) under a new parent MEGA-EUROPE.

With EU flag:   PFT-82980216-8745 (1 active child, Spruce)
Without flag:   6843674_12735, 6843674_12736, 6843674_7853, 6843674_7854,
                6843674_7855, 6843674_7856, 6843674_7857, 6843674_7858,
                6843674_7859, 6843674_9794, PFT-88471944-8745  (11 SKUs)

New parent:     MEGA-EUROPE (productType=HAT, variation_theme=STYLE_NAME/COLOR_NAME)
Old parents to deactivate: PFT-82980216, PFT-88471944, 6843674

Steps:
  1. Inspect current state on DE
  2. Create new parent MEGA-EUROPE on all 8 EU marketplaces
  3. Re-link no-flag children: style="ohne Flagge", color=read from API
  4. Re-link flag child:      style="mit EU-Flagge", color=read from API
  5. Deactivate old parents on all 8 marketplaces

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/merge_europe.py --dry-run          # preview all
  python3.11 scripts/merge_europe.py --step inspect      # inspect DE
  python3.11 scripts/merge_europe.py --step create-parent
  python3.11 scripts/merge_europe.py --step relink-noflag
  python3.11 scripts/merge_europe.py --step relink-flag
  python3.11 scripts/merge_europe.py --step deactivate
  python3.11 scripts/merge_europe.py                     # run all steps
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

NEW_PARENT_SKU = "MEGA-EUROPE"
VARIATION_THEME = "STYLE_NAME/COLOR_NAME"
PRODUCT_TYPE = "HAT"

# Old parents to deactivate
OLD_PARENTS = ["PFT-82980216", "PFT-88471944", "6843674"]

# No-flag children (11 SKUs)
NOFLAG_CHILDREN = [
    "6843674_12735",
    "6843674_12736",
    "6843674_7853",
    "6843674_7854",
    "6843674_7855",
    "6843674_7856",
    "6843674_7857",
    "6843674_7858",
    "6843674_7859",
    "6843674_9794",
    "PFT-88471944-8745",
]

# Flag children (1 SKU)
FLAG_CHILDREN = [
    "PFT-82980216-8745",
]

ALL_CHILDREN = NOFLAG_CHILDREN + FLAG_CHILDREN

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

# Localized style labels (EU-specific phrasing)
FLAG_LABELS = {
    "DE": {"with": "mit EU-Flagge",     "without": "ohne Flagge"},
    "FR": {"with": "avec Drapeau UE",   "without": "sans Drapeau"},
    "IT": {"with": "con Bandiera UE",   "without": "senza Bandiera"},
    "ES": {"with": "con Bandera UE",    "without": "sin Bandera"},
    "NL": {"with": "met EU-Vlag",       "without": "zonder Vlag"},
    "PL": {"with": "z Flaga UE",        "without": "bez Flagi"},
    "SE": {"with": "med EU-Flagga",     "without": "utan Flagga"},
    "BE": {"with": "avec Drapeau UE",   "without": "sans Drapeau"},
}

# Known color names per variant suffix (same Printful color IDs across all products)
# Used as fallback when API does not return a color value
KNOWN_COLORS = {
    "DE": {
        "7853": "Weiss", "7854": "Schwarz", "7855": "Beige", "7856": "Hellblau",
        "7857": "Marineblau", "7858": "Rosa", "7859": "Steingrau",
        "8745": "Fichte", "9794": "Gruenes Tarnmuster",
        "12735": "Cranberry", "12736": "Dunkelgrau",
    },
    "FR": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Sapin", "9794": "Camouflage Vert",
        "12735": "Canneberge", "12736": "Gris Fonce",
    },
    "IT": {
        "7853": "Bianco", "7854": "Nero", "7855": "Beige", "7856": "Azzurro",
        "7857": "Blu Navy", "7858": "Rosa", "7859": "Grigio Pietra",
        "8745": "Abete", "9794": "Mimetico Verde",
        "12735": "Mirtillo Rosso", "12736": "Grigio Scuro",
    },
    "ES": {
        "7853": "Blanco", "7854": "Negro", "7855": "Beige", "7856": "Azul Claro",
        "7857": "Azul Marino", "7858": "Rosa", "7859": "Gris Piedra",
        "8745": "Abeto", "9794": "Camuflaje Verde",
        "12735": "Arandano", "12736": "Gris Oscuro",
    },
    "NL": {
        "7853": "Wit", "7854": "Zwart", "7855": "Beige", "7856": "Lichtblauw",
        "7857": "Marineblauw", "7858": "Roze", "7859": "Steengrijs",
        "8745": "Spar", "9794": "Groen Camouflage",
        "12735": "Cranberry", "12736": "Donkergrijs",
    },
    "PL": {
        "7853": "Bialy", "7854": "Czarny", "7855": "Bezowy", "7856": "Jasnoniebieski",
        "7857": "Granatowy", "7858": "Rozowy", "7859": "Szary Kamien",
        "8745": "Swierk", "9794": "Zielony Kamuflaz",
        "12735": "Zurawina", "12736": "Ciemnoszary",
    },
    "SE": {
        "7853": "Vit", "7854": "Svart", "7855": "Beige", "7856": "Ljusbla",
        "7857": "Marinbla", "7858": "Rosa", "7859": "Stengra",
        "8745": "Gran", "9794": "Gron kamouflage",
        "12735": "Tranbar", "12736": "Morkgra",
    },
    "BE": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Sapin", "9794": "Camouflage Vert",
        "12735": "Canneberge", "12736": "Gris Fonce",
    },
}

SLEEP_BETWEEN = 0.6


def extract_suffix(sku):
    """Extract the Printful variant suffix from a SKU.
    6843674_7853 -> 7853, PFT-88471944-8745 -> 8745, PFT-82980216-8745 -> 8745
    """
    if "_" in sku:
        return sku.split("_")[-1]
    elif "-" in sku:
        return sku.split("-")[-1]
    return sku


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

    def put_listing(self, sku, mp_id, body, retries=8):
        """PUT (full create/replacement) for parent listings."""
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

    def delete_listing(self, sku, mp_id, retries=5):
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
        params = {"marketplaceIds": mp_id}
        for attempt in range(retries):
            try:
                r = requests.delete(url, headers=self._headers(), params=params, timeout=30)
            except Exception as e:
                print(f"    [DELETE {sku}] connection error: {e}, retry {attempt+1}/{retries}")
                time.sleep(5)
                continue
            if r.status_code == 429:
                wait = min(3 * (2 ** attempt), 30)
                print(f"    [DELETE {sku}] 429, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(2)
                continue
            resp = {}
            try:
                resp = r.json()
            except Exception:
                pass
            return r.status_code, resp
        return 0, {"error": "retries exhausted"}


# ── Report Helpers ────────────────────────────────────────────────────

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


def strip_flag_suffix(color_value, mkt_code):
    """Remove any existing flag suffix from a color value (from previous 1D merge attempts)."""
    for suffix_key in ("without", "with"):
        for labels in FLAG_LABELS.values():
            suffix = f" - {labels[suffix_key]}"
            if color_value.endswith(suffix):
                return color_value[:-len(suffix)]
    # Also strip generic patterns like " - ohne Flagge", " - mit Flagge"
    import re
    color_value = re.sub(r'\s*-\s*(mit|ohne|avec|sans|con|senza|met|zonder|med|utan|z|bez)\s+.*$',
                          '', color_value, flags=re.IGNORECASE)
    return color_value.strip()


def resolve_color(api, sku, mkt_code, mp_id):
    """Read color value for a child from the API. Falls back to known colors."""
    suffix = extract_suffix(sku)

    # Try API first
    code, data = api.get_listing(sku, mp_id)
    if code == 200 and data:
        attrs = data.get("attributes", {})
        color_list = attrs.get("color", [])
        if color_list:
            raw = color_list[0].get("value", "")
            clean = strip_flag_suffix(raw, mkt_code)
            if clean:
                return clean

    # Fallback to known colors
    mkt_colors = KNOWN_COLORS.get(mkt_code, KNOWN_COLORS["DE"])
    fallback = mkt_colors.get(suffix, "")
    if fallback:
        print(f"    [FALLBACK] {sku} on {mkt_code}: using known color '{fallback}' for suffix {suffix}")
    else:
        print(f"    [WARN] {sku} on {mkt_code}: no color found (suffix={suffix})")
    return fallback


# ═══════════════════════════════════════════════════════════════════════
# STEP: INSPECT
# ═══════════════════════════════════════════════════════════════════════

def step_inspect(api):
    """Inspect current state of parents and children on DE."""
    print("\n" + "=" * 70)
    print("  STEP: INSPECT CURRENT STATE (DE)")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    # Check new parent (may not exist yet)
    print(f"\n  --- New Parent (target): {NEW_PARENT_SKU} ---")
    code, data = api.get_listing(NEW_PARENT_SKU, de_mp)
    print(f"  HTTP {code}")
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {json.dumps(attrs.get('variation_theme', []))}")
        print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}")
            print(f"  status: {json.dumps(summaries[0].get('status', []))}")
    time.sleep(SLEEP_BETWEEN)

    # Check old parents
    for parent_sku in OLD_PARENTS:
        print(f"\n  --- Old Parent (to deactivate): {parent_sku} ---")
        code, data = api.get_listing(parent_sku, de_mp)
        print(f"  HTTP {code}")
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  variation_theme: {json.dumps(attrs.get('variation_theme', []))}")
            print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                print(f"  ASIN: {summaries[0].get('asin','?')}")
                print(f"  status: {json.dumps(summaries[0].get('status', []))}")
        time.sleep(SLEEP_BETWEEN)

    # Check no-flag children (sample)
    print(f"\n  --- No-flag children (sample, {len(NOFLAG_CHILDREN)} total) ---")
    for sku in NOFLAG_CHILDREN[:4]:
        print(f"\n  Child: {sku}")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {json.dumps(attrs.get('color', []))}")
            print(f"  style: {json.dumps(attrs.get('style', []))}")
            print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
            print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                s = summaries[0]
                print(f"  ASIN: {s.get('asin','?')}, status: {json.dumps(s.get('status', []))}")
        else:
            print(f"  HTTP {code} (not found / error)")
        time.sleep(SLEEP_BETWEEN)

    # Check flag child
    print(f"\n  --- Flag child ---")
    for sku in FLAG_CHILDREN:
        print(f"\n  Child: {sku}")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {json.dumps(attrs.get('color', []))}")
            print(f"  style: {json.dumps(attrs.get('style', []))}")
            print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
            print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                s = summaries[0]
                print(f"  ASIN: {s.get('asin','?')}, status: {json.dumps(s.get('status', []))}")
        else:
            print(f"  HTTP {code} (not found / error)")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Inspect complete.\n")


# ═══════════════════════════════════════════════════════════════════════
# STEP: CREATE PARENT
# ═══════════════════════════════════════════════════════════════════════

def step_create_parent(api, results, dry_run=False):
    """Create/update MEGA-EUROPE parent on all 8 marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: CREATE PARENT ({NEW_PARENT_SKU})")
    print("=" * 70)

    # Localized parent titles
    PARENT_TITLES = {
        "DE": "Europa Kappe Trucker Cap Unisex Basecap Herren Damen",
        "FR": "Casquette Europe Trucker Cap Unisexe Homme Femme",
        "IT": "Cappellino Europa Trucker Cap Unisex Uomo Donna",
        "ES": "Gorra Europa Trucker Cap Unisex Hombre Mujer",
        "NL": "Europa Pet Trucker Cap Unisex Heren Dames",
        "PL": "Czapka Europa Trucker Cap Unisex Meska Damska",
        "SE": "Europa Keps Trucker Cap Unisex Herr Dam",
        "BE": "Casquette Europe Trucker Cap Unisexe Homme Femme",
    }

    PARENT_BULLETS = {
        "DE": ["Hochwertige Trucker Cap mit Europa Design", "Verstellbarer Snapback Verschluss", "Atmungsaktives Mesh Material"],
        "FR": ["Casquette trucker de qualite avec design Europe", "Fermeture snapback reglable", "Matiere mesh respirante"],
        "IT": ["Cappellino trucker di qualita con design Europa", "Chiusura snapback regolabile", "Materiale mesh traspirante"],
        "ES": ["Gorra trucker de calidad con diseno Europa", "Cierre snapback ajustable", "Material mesh transpirable"],
        "NL": ["Hoogwaardige trucker cap met Europa design", "Verstelbare snapback sluiting", "Ademend mesh materiaal"],
        "PL": ["Wysokiej jakosci czapka trucker z designem Europa", "Regulowane zapiecie snapback", "Oddychajacy material mesh"],
        "SE": ["Hogkvalitativ trucker keps med Europa design", "Justerbart snapback spanne", "Andningsbart mesh material"],
        "BE": ["Casquette trucker de qualite avec design Europe", "Fermeture snapback reglable", "Matiere mesh respirante"],
    }

    PARENT_DESCRIPTIONS = {
        "DE": "Europa Trucker Cap in verschiedenen Farben und Stilen. Hochwertige Verarbeitung mit verstellbarem Snapback Verschluss.",
        "FR": "Casquette Trucker Europe disponible en plusieurs couleurs et styles. Fabrication de qualite avec fermeture snapback reglable.",
        "IT": "Cappellino Trucker Europa disponibile in vari colori e stili. Lavorazione di qualita con chiusura snapback regolabile.",
        "ES": "Gorra Trucker Europa disponible en varios colores y estilos. Fabricacion de calidad con cierre snapback ajustable.",
        "NL": "Europa Trucker Cap verkrijgbaar in verschillende kleuren en stijlen. Hoogwaardige afwerking met verstelbare snapback sluiting.",
        "PL": "Czapka Trucker Europa dostepna w roznych kolorach i stylach. Wysokiej jakosci wykonanie z regulowanym zapieciem snapback.",
        "SE": "Europa Trucker Keps finns i olika farger och stilar. Hogkvalitativt utforande med justerbart snapback spanne.",
        "BE": "Casquette Trucker Europe disponible en plusieurs couleurs et styles. Fabrication de qualite avec fermeture snapback reglable.",
    }

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]

        body = {
            "productType": PRODUCT_TYPE,
            "requirements": "LISTING",
            "attributes": {
                "item_name": [{"value": PARENT_TITLES[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
                "parentage_level": [{"value": "parent", "marketplace_id": mp_id}],
                "variation_theme": [{"name": VARIATION_THEME, "marketplace_id": mp_id}],
                "brand": [{"value": "nesell", "language_tag": lang}],
                "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mp_id} for b in PARENT_BULLETS[mkt_code]],
                "product_description": [{"value": PARENT_DESCRIPTIONS[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
                "country_of_origin": [{"value": "US", "marketplace_id": mp_id}],
                "material": [{"value": "Polyester", "language_tag": lang}],
                "fabric_type": [{"value": "Polyester", "language_tag": lang, "marketplace_id": mp_id}],
                "batteries_required": [{"value": False, "marketplace_id": mp_id}],
                "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mp_id}],
                "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mp_id}],
                "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mp_id}],
                "gpsr_safety_attestation": [{"value": True, "marketplace_id": mp_id}],
                "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mp_id}],
                "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mp_id}],
                "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mp_id}],
            }
        }

        if dry_run:
            print(f"  [DRY] Would PUT {NEW_PARENT_SKU} on {mkt_code} (variation_theme={VARIATION_THEME})")
            total += 1
            continue

        code, resp = api.put_listing(NEW_PARENT_SKU, mp_id, body)
        log_result(results, "create_parent", NEW_PARENT_SKU, mkt_code, code, resp)
        total += 1
        time.sleep(SLEEP_BETWEEN)

    print(f"\n  Created parent on {total} marketplaces.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# STEP: RE-LINK NO-FLAG CHILDREN
# ═══════════════════════════════════════════════════════════════════════

def step_relink_noflag(api, results, dry_run=False):
    """Re-link no-flag children to MEGA-EUROPE with style='ohne Flagge'."""
    print("\n" + "=" * 70)
    print(f"  STEP: RE-LINK {len(NOFLAG_CHILDREN)} NO-FLAG CHILDREN")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        style_value = FLAG_LABELS[mkt_code]["without"]
        print(f"\n  ===== {mkt_code}: style='{style_value}' =====")

        for sku in NOFLAG_CHILDREN:
            color = resolve_color(api, sku, mkt_code, mp_id)
            time.sleep(SLEEP_BETWEEN)

            if not color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color resolved")
                continue

            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/child_parent_sku_relationship",
                    "value": [{
                        "child_relationship_type": "variation",
                        "parent_sku": NEW_PARENT_SKU,
                        "marketplace_id": mp_id
                    }]
                },
                {
                    "op": "replace",
                    "path": "/attributes/parentage_level",
                    "value": [{"marketplace_id": mp_id, "value": "child"}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/color",
                    "value": [{"value": color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": style_value, "language_tag": lang, "marketplace_id": mp_id}]
                },
            ]

            if dry_run:
                print(f"  [DRY] {sku}: parent={NEW_PARENT_SKU}, color='{color}', style='{style_value}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "relink_noflag", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Re-linked {total} no-flag children.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# STEP: RE-LINK FLAG CHILDREN
# ═══════════════════════════════════════════════════════════════════════

def step_relink_flag(api, results, dry_run=False):
    """Re-link flag child(ren) to MEGA-EUROPE with style='mit EU-Flagge'."""
    print("\n" + "=" * 70)
    print(f"  STEP: RE-LINK {len(FLAG_CHILDREN)} FLAG CHILDREN")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        style_value = FLAG_LABELS[mkt_code]["with"]
        print(f"\n  ===== {mkt_code}: style='{style_value}' =====")

        for sku in FLAG_CHILDREN:
            color = resolve_color(api, sku, mkt_code, mp_id)
            time.sleep(SLEEP_BETWEEN)

            if not color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color resolved")
                continue

            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/child_parent_sku_relationship",
                    "value": [{
                        "child_relationship_type": "variation",
                        "parent_sku": NEW_PARENT_SKU,
                        "marketplace_id": mp_id
                    }]
                },
                {
                    "op": "replace",
                    "path": "/attributes/parentage_level",
                    "value": [{"marketplace_id": mp_id, "value": "child"}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/color",
                    "value": [{"value": color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": style_value, "language_tag": lang, "marketplace_id": mp_id}]
                },
            ]

            if dry_run:
                print(f"  [DRY] {sku}: parent={NEW_PARENT_SKU}, color='{color}', style='{style_value}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "relink_flag", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Re-linked {total} flag children.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# STEP: DEACTIVATE OLD PARENTS
# ═══════════════════════════════════════════════════════════════════════

def step_deactivate(api, results, dry_run=False):
    """Deactivate old parent SKUs on all 8 marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: DEACTIVATE OLD PARENTS ({', '.join(OLD_PARENTS)})")
    print("=" * 70)

    total = 0
    for parent_sku in OLD_PARENTS:
        for mkt_code, mp_id in MARKETPLACE_IDS.items():
            if dry_run:
                print(f"  [DRY] Would DELETE {parent_sku} on {mkt_code}")
                total += 1
                continue

            code, resp = api.delete_listing(parent_sku, mp_id)
            log_result(results, "deactivate_parent", parent_sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Deactivated {total} parent/marketplace combos.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Merge Europe flag + no-flag into MEGA-EUROPE 2D variation")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without making API calls")
    parser.add_argument("--step",
                        choices=["inspect", "create-parent", "relink-noflag", "relink-flag", "deactivate"],
                        help="Run only a specific step")
    args = parser.parse_args()

    print("=" * 70)
    print("  MERGE EUROPE LISTINGS -> MEGA-EUROPE (2D: STYLE_NAME/COLOR_NAME)")
    print(f"  New parent: {NEW_PARENT_SKU}")
    print(f"  No-flag children: {len(NOFLAG_CHILDREN)} SKUs")
    print(f"  Flag children: {len(FLAG_CHILDREN)} SKUs")
    print(f"  Old parents to deactivate: {', '.join(OLD_PARENTS)}")
    print(f"  Variation theme: {VARIATION_THEME}")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Marketplaces: {', '.join(MARKETPLACE_IDS.keys())}")
    print("=" * 70)

    api = AmazonAPI(AMZ_CREDS)
    results = []

    if args.step == "inspect" or args.step is None:
        step_inspect(api)
        if args.step == "inspect":
            return

    if args.step == "create-parent" or args.step is None:
        step_create_parent(api, results, args.dry_run)
        if args.step == "create-parent":
            print_report(results)
            return

    if args.step == "relink-noflag" or args.step is None:
        step_relink_noflag(api, results, args.dry_run)
        if args.step == "relink-noflag":
            print_report(results)
            return

    if args.step == "relink-flag" or args.step is None:
        step_relink_flag(api, results, args.dry_run)
        if args.step == "relink-flag":
            print_report(results)
            return

    if args.step == "deactivate" or args.step is None:
        step_deactivate(api, results, args.dry_run)
        if args.step == "deactivate":
            print_report(results)
            return

    # Full run
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
    print(f"\n  Summary:")
    print(f"    Parent created on marketplaces: {sum(1 for r in results if r['action']=='create_parent' and r['http_status'] in (200,204) and not r['errors'])}")
    print(f"    No-flag children re-linked: {sum(1 for r in results if r['action']=='relink_noflag' and r['http_status'] in (200,204) and not r['errors'])}")
    print(f"    Flag children re-linked: {sum(1 for r in results if r['action']=='relink_flag' and r['http_status'] in (200,204) and not r['errors'])}")
    print(f"    Old parents deactivated: {sum(1 for r in results if r['action']=='deactivate_parent' and r['http_status'] in (200,204) and not r['errors'])}")
    print(f"    Total: {total_ok} OK, {total_err} errors out of {len(results)} API calls")

    # Save results
    results_path = Path(__file__).parent / "merge_europe_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"  Results saved to: {results_path}")


if __name__ == "__main__":
    main()
