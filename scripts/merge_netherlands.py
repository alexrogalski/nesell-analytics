#!/usr/bin/env python3.11
"""
Merge Netherlands flag + no-flag listings into one 2D variation (STYLE_NAME/COLOR_NAME).

- With flag: Parent PFT-93855915, 11 children
- Without flag: Parent PFT-93856209, 11 children
- Actual parent used: PFT-MEGA-NL2 (already existed with STYLE_NAME/COLOR_NAME theme, ASIN B0GQTRSHHP)
- productType: HAT, variation_theme: STYLE_NAME/COLOR_NAME

NOTE: Originally tried creating MEGA-NETHERLANDS parent but it failed (missing required fields).
      Discovered PFT-MEGA-NL2 already existed and was working, so all children were relinked there.
      The broken MEGA-NETHERLANDS was cleaned up (deleted).

Result (2026-03-11):
  - 176/176 children relinked to PFT-MEGA-NL2 (22 children x 8 marketplaces, 0 errors)
  - Flag children: style = localized "mit Flagge", color = pure color
  - No-flag children: style = localized "ohne Flagge", color = pure color
  - Old parents PFT-93855915 + PFT-93856209 deactivated (16/16 OK)
  - Broken MEGA-NETHERLANDS deleted (8/8 OK)

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/merge_netherlands.py --dry-run
  python3.11 scripts/merge_netherlands.py --step inspect
  python3.11 scripts/merge_netherlands.py --step create-parent
  python3.11 scripts/merge_netherlands.py --step relink
  python3.11 scripts/merge_netherlands.py --step deactivate-parents
  python3.11 scripts/merge_netherlands.py                          # full run
"""

import argparse
import json
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests

# -- Credentials -------------------------------------------------------------
KEYS_DIR = Path.home() / ".keys"
AMZ_CREDS = json.loads((KEYS_DIR / "amazon-sp-api.json").read_text())
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"

# -- Constants ----------------------------------------------------------------

NEW_PARENT_SKU = "MEGA-NETHERLANDS"
FLAG_PARENT_SKU = "PFT-93855915"       # with flag (to deactivate)
NOFLAG_PARENT_SKU = "PFT-93856209"     # without flag (to deactivate)

VARIATION_THEME = "STYLE_NAME/COLOR_NAME"
PRODUCT_TYPE = "HAT"

# Shared variant suffixes (both families use the same 11)
CHILD_SUFFIXES = [
    "12735", "12736", "7853", "7854", "7855",
    "7856", "7857", "7858", "7859", "8745", "9794",
]

FLAG_CHILDREN = [f"PFT-93855915-{s}" for s in CHILD_SUFFIXES]
NOFLAG_CHILDREN = [f"PFT-93856209-{s}" for s in CHILD_SUFFIXES]

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

# Localized style values (flag vs no-flag)
STYLE_LABELS = {
    "DE": {"with": "mit Flagge",     "without": "ohne Flagge"},
    "FR": {"with": "avec Drapeau",   "without": "sans Drapeau"},
    "IT": {"with": "con Bandiera",   "without": "senza Bandiera"},
    "ES": {"with": "con Bandera",    "without": "sin Bandera"},
    "NL": {"with": "met Vlag",       "without": "zonder Vlag"},
    "PL": {"with": "z Flaga",        "without": "bez Flagi"},
    "SE": {"with": "med Flagga",     "without": "utan Flagga"},
    "BE": {"with": "avec Drapeau",   "without": "sans Drapeau"},
}

# Color names per marketplace per suffix (same Printful variants as Germany)
COLORS = {
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


# -- Amazon SP-API Client ----------------------------------------------------

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


# -- Logging ------------------------------------------------------------------

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


# =============================================================================
# STEP: INSPECT
# =============================================================================

def step_inspect(api):
    """Inspect current state of both parents and sample children on DE."""
    print("\n" + "=" * 70)
    print("  STEP: INSPECT CURRENT STATE (DE)")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    for label, sku in [("Flag parent", FLAG_PARENT_SKU),
                       ("No-flag parent", NOFLAG_PARENT_SKU),
                       ("New parent (MEGA)", NEW_PARENT_SKU)]:
        print(f"\n  --- {label}: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
        print(f"  HTTP {code}")
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  variation_theme: {json.dumps(attrs.get('variation_theme', []))}")
            print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                s = summaries[0]
                print(f"  ASIN: {s.get('asin','?')}")
                print(f"  status: {json.dumps(s.get('status', []))}")
        elif code == 404:
            print(f"  Not found (expected for MEGA if not yet created)")
        time.sleep(SLEEP_BETWEEN)

    # Sample flag children
    for sku in FLAG_CHILDREN[:2]:
        print(f"\n  --- Flag child: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {json.dumps(attrs.get('color', []))}")
            print(f"  style: {json.dumps(attrs.get('style', []))}")
            print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
            print(f"  child_parent_sku: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                print(f"  ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
        time.sleep(SLEEP_BETWEEN)

    # Sample no-flag children
    for sku in NOFLAG_CHILDREN[:2]:
        print(f"\n  --- No-flag child: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {json.dumps(attrs.get('color', []))}")
            print(f"  style: {json.dumps(attrs.get('style', []))}")
            print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
            print(f"  child_parent_sku: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                print(f"  ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Inspect complete.\n")


# =============================================================================
# STEP: CREATE PARENT
# =============================================================================

def step_create_parent(api, results, dry_run=False):
    """Create MEGA-NETHERLANDS parent with 2D variation_theme on all 8 marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: CREATE PARENT {NEW_PARENT_SKU}")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        print(f"\n  --- {mkt_code} ---")

        # Build parent listing body via PUT
        body = {
            "productType": PRODUCT_TYPE,
            "requirements": "LISTING",
            "attributes": {
                "merchant_suggested_asin": [{"value": "0", "marketplace_id": mp_id}],
                "condition_type": [{"value": "new_new", "marketplace_id": mp_id}],
                "parentage_level": [{"value": "parent", "marketplace_id": mp_id}],
                "variation_theme": [{"name": VARIATION_THEME, "marketplace_id": mp_id}],
                "item_name": [{"value": "Make Netherlands Great Again Hat", "language_tag": lang, "marketplace_id": mp_id}],
                "brand": [{"value": "Generic", "language_tag": lang, "marketplace_id": mp_id}],
            }
        }

        if dry_run:
            print(f"  [DRY] Would PUT parent {NEW_PARENT_SKU} with theme={VARIATION_THEME}")
            total += 1
            continue

        code, resp = api.put_listing(NEW_PARENT_SKU, mp_id, body)
        log_result(results, "create_parent", NEW_PARENT_SKU, mkt_code, code, resp)
        total += 1
        time.sleep(SLEEP_BETWEEN)

    print(f"\n  Created parent on {total} marketplaces.")
    return total


# =============================================================================
# STEP: RELINK CHILDREN
# =============================================================================

def step_relink_children(api, results, dry_run=False):
    """Relink all 22 children to MEGA-NETHERLANDS with style + color attributes."""
    print("\n" + "=" * 70)
    print(f"  STEP: RELINK ALL CHILDREN TO {NEW_PARENT_SKU}")
    print("=" * 70)

    total = 0

    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        mkt_colors = COLORS.get(mkt_code, COLORS["DE"])
        style_with = STYLE_LABELS[mkt_code]["with"]
        style_without = STYLE_LABELS[mkt_code]["without"]

        print(f"\n  ===== {mkt_code}: style='{style_with}'/'{style_without}' =====")

        # -- Flag children (style = "mit Flagge" etc.) --
        print(f"\n  --- Flag children ({len(CHILD_SUFFIXES)} SKUs) ---")
        for suffix_id in CHILD_SUFFIXES:
            sku = f"PFT-93855915-{suffix_id}"
            color = mkt_colors.get(suffix_id, "")
            if not color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color mapping")
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
                    "value": [{"value": "child", "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/color",
                    "value": [{"value": color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": style_with, "language_tag": lang, "marketplace_id": mp_id}]
                },
            ]

            if dry_run:
                print(f"  [DRY] {sku}: parent={NEW_PARENT_SKU}, color='{color}', style='{style_with}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "relink_flag_child", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

        # -- No-flag children (style = "ohne Flagge" etc.) --
        print(f"\n  --- No-flag children ({len(CHILD_SUFFIXES)} SKUs) ---")
        for suffix_id in CHILD_SUFFIXES:
            sku = f"PFT-93856209-{suffix_id}"
            color = mkt_colors.get(suffix_id, "")
            if not color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color mapping")
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
                    "value": [{"value": "child", "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/color",
                    "value": [{"value": color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": style_without, "language_tag": lang, "marketplace_id": mp_id}]
                },
            ]

            if dry_run:
                print(f"  [DRY] {sku}: parent={NEW_PARENT_SKU}, color='{color}', style='{style_without}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "relink_noflag_child", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Relinked {total} children across all marketplaces.")
    return total


# =============================================================================
# STEP: DEACTIVATE OLD PARENTS
# =============================================================================

def step_deactivate_parents(api, results, dry_run=False):
    """Deactivate both old parents on all 8 marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: DEACTIVATE OLD PARENTS")
    print(f"    {FLAG_PARENT_SKU} (flag)")
    print(f"    {NOFLAG_PARENT_SKU} (no-flag)")
    print("=" * 70)

    total = 0
    for parent_sku, label in [(FLAG_PARENT_SKU, "flag_parent"),
                               (NOFLAG_PARENT_SKU, "noflag_parent")]:
        for mkt_code, mp_id in MARKETPLACE_IDS.items():
            if dry_run:
                print(f"  [DRY] Would DELETE {parent_sku} on {mkt_code}")
                total += 1
                continue

            code, resp = api.delete_listing(parent_sku, mp_id)
            log_result(results, f"deactivate_{label}", parent_sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Deactivated parents on {total} marketplace-slots.")
    return total


# =============================================================================
# STEP: VERIFY
# =============================================================================

def step_verify(api):
    """Verify the 2D variation is correctly set up on DE."""
    print("\n" + "=" * 70)
    print("  STEP: VERIFY (DE)")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    # Check new parent
    print(f"\n  --- New parent: {NEW_PARENT_SKU} ---")
    code, data = api.get_listing(NEW_PARENT_SKU, de_mp)
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {json.dumps(attrs.get('variation_theme', []))}")
        print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
    else:
        print(f"  HTTP {code}")
    time.sleep(SLEEP_BETWEEN)

    # Check sample flag child
    sku = FLAG_CHILDREN[0]
    print(f"\n  --- Flag child: {sku} ---")
    code, data = api.get_listing(sku, de_mp)
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  color: {json.dumps(attrs.get('color', []))}")
        print(f"  style: {json.dumps(attrs.get('style', []))}")
        print(f"  child_parent_sku: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
    time.sleep(SLEEP_BETWEEN)

    # Check sample no-flag child
    sku = NOFLAG_CHILDREN[0]
    print(f"\n  --- No-flag child: {sku} ---")
    code, data = api.get_listing(sku, de_mp)
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  color: {json.dumps(attrs.get('color', []))}")
        print(f"  style: {json.dumps(attrs.get('style', []))}")
        print(f"  child_parent_sku: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
    time.sleep(SLEEP_BETWEEN)

    # Check old parents are gone
    for parent_sku in [FLAG_PARENT_SKU, NOFLAG_PARENT_SKU]:
        print(f"\n  --- Old parent (should be gone): {parent_sku} ---")
        code, data = api.get_listing(parent_sku, de_mp)
        if code == 404:
            print(f"  Deleted (404). Good.")
        elif code == 200 and data:
            summaries = data.get("summaries", [])
            if summaries:
                status = summaries[0].get("status", [])
                print(f"  Still exists! ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(status)}")
            else:
                print(f"  HTTP 200 but no summaries (may be inactive)")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Verify complete.\n")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Merge Netherlands flag/no-flag into 2D variation (STYLE_NAME/COLOR_NAME)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without making API calls")
    parser.add_argument("--step",
                        choices=["inspect", "create-parent", "relink", "deactivate-parents", "verify"],
                        help="Run only a specific step")
    args = parser.parse_args()

    print("=" * 70)
    print("  MERGE NETHERLANDS FLAG + NO-FLAG LISTINGS")
    print(f"  New parent (create): {NEW_PARENT_SKU}")
    print(f"  Flag parent (deactivate): {FLAG_PARENT_SKU}")
    print(f"  No-flag parent (deactivate): {NOFLAG_PARENT_SKU}")
    print(f"  Flag children: {len(FLAG_CHILDREN)} SKUs")
    print(f"  No-flag children: {len(NOFLAG_CHILDREN)} SKUs")
    print(f"  Variation theme: {VARIATION_THEME}")
    print(f"  Product type: {PRODUCT_TYPE}")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Marketplaces: {', '.join(MARKETPLACE_IDS.keys())}")
    print("=" * 70)

    api = AmazonAPI(AMZ_CREDS)
    results = []

    if args.step == "inspect":
        step_inspect(api)
        return

    if args.step == "verify":
        step_verify(api)
        return

    if args.step == "create-parent":
        step_create_parent(api, results, args.dry_run)
        print_report(results)
        return

    if args.step == "relink":
        step_relink_children(api, results, args.dry_run)
        print_report(results)
        return

    if args.step == "deactivate-parents":
        step_deactivate_parents(api, results, args.dry_run)
        print_report(results)
        return

    # -- Full run --
    step_inspect(api)
    step_create_parent(api, results, args.dry_run)
    step_relink_children(api, results, args.dry_run)
    step_deactivate_parents(api, results, args.dry_run)

    if not args.dry_run:
        print("\n  Waiting 5s before verification...")
        time.sleep(5)
        step_verify(api)

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

    # Summaries
    parent_created = sum(1 for r in results
                         if r["action"] == "create_parent"
                         and r["http_status"] in (200, 204) and not r["errors"])
    flag_relinked = sum(1 for r in results
                        if r["action"] == "relink_flag_child"
                        and r["http_status"] in (200, 204) and not r["errors"])
    noflag_relinked = sum(1 for r in results
                          if r["action"] == "relink_noflag_child"
                          and r["http_status"] in (200, 204) and not r["errors"])
    deactivated = sum(1 for r in results
                      if r["action"].startswith("deactivate_")
                      and r["http_status"] in (200, 204) and not r["errors"])

    total_ok = sum(c["ok"] for c in actions.values())
    total_err = sum(c["err"] for c in actions.values())

    print(f"\n  Summary:")
    print(f"    Parent created on marketplaces: {parent_created}")
    print(f"    Flag children relinked: {flag_relinked}")
    print(f"    No-flag children relinked: {noflag_relinked}")
    print(f"    Old parents deactivated: {deactivated}")
    print(f"    Total: {total_ok} OK, {total_err} errors out of {len(results)} API calls")

    # Save results
    results_path = Path(__file__).parent / "merge_netherlands_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"  Results saved to: {results_path}")


if __name__ == "__main__":
    main()
