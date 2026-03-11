#!/usr/bin/env python3.11
"""
Merge Belgium flag + no-flag listings into one 2D variation (STYLE_NAME/COLOR_NAME).

With flag:    Parent PFT-93855556, 11 children
Without flag: Parent PFT-93856238, 11 children
New parent:   MEGA-BELGIUM, productType: HAT, variation_theme: STYLE_NAME/COLOR_NAME

Steps:
  1. Inspect: read current state of both parents + sample children on DE
  2. Create parent: PUT MEGA-BELGIUM as parent with STYLE_NAME/COLOR_NAME theme
  3. Relink flag children: set child_parent_sku_relationship -> MEGA-BELGIUM,
     style = "mit Flagge" (localized), color = pure color
  4. Relink no-flag children: same but style = "ohne Flagge" (localized)
  5. Deactivate old parents (PFT-93855556, PFT-93856238) on all 8 marketplaces

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/merge_belgium.py --dry-run
  python3.11 scripts/merge_belgium.py --step inspect
  python3.11 scripts/merge_belgium.py --step create-parent
  python3.11 scripts/merge_belgium.py --step relink
  python3.11 scripts/merge_belgium.py --step deactivate
  python3.11 scripts/merge_belgium.py --step verify
  python3.11 scripts/merge_belgium.py                      # full run
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

NEW_PARENT_SKU = "MEGA-BELGIUM"
FLAG_PARENT_SKU = "PFT-93855556"       # with flag (to deactivate)
NOFLAG_PARENT_SKU = "PFT-93856238"     # without flag (to deactivate)

# Child variant suffixes (shared between both families)
CHILD_SUFFIXES = [
    "12735", "12736", "7853", "7854", "7855",
    "7856", "7857", "7858", "7859", "8745", "9794",
]

FLAG_CHILDREN = [f"PFT-93855556-{s}" for s in CHILD_SUFFIXES]
NOFLAG_CHILDREN = [f"PFT-93856238-{s}" for s in CHILD_SUFFIXES]

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

# Localized style values (for the STYLE dimension dropdown)
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

# Color names per marketplace per variant suffix
# These are used for BOTH flag and no-flag children (same colors, different style)
COLORS = {
    "DE": {
        "7853": "Weiss", "7854": "Schwarz", "7855": "Beige", "7856": "Hellblau",
        "7857": "Marineblau", "7858": "Rosa", "7859": "Steingrau",
        "8745": "Tannengruen", "9794": "Gruenes Tarnmuster",
        "12735": "Cranberry", "12736": "Dunkelgrau",
    },
    "FR": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Vert Sapin", "9794": "Camouflage Vert",
        "12735": "Canneberge", "12736": "Gris Fonce",
    },
    "IT": {
        "7853": "Bianco", "7854": "Nero", "7855": "Beige", "7856": "Azzurro",
        "7857": "Blu Navy", "7858": "Rosa", "7859": "Grigio Pietra",
        "8745": "Verde Abete", "9794": "Mimetico Verde",
        "12735": "Mirtillo Rosso", "12736": "Grigio Scuro",
    },
    "ES": {
        "7853": "Blanco", "7854": "Negro", "7855": "Beige", "7856": "Azul Claro",
        "7857": "Azul Marino", "7858": "Rosa", "7859": "Gris Piedra",
        "8745": "Verde Abeto", "9794": "Camuflaje Verde",
        "12735": "Arandano", "12736": "Gris Oscuro",
    },
    "NL": {
        "7853": "Wit", "7854": "Zwart", "7855": "Beige", "7856": "Lichtblauw",
        "7857": "Marineblauw", "7858": "Roze", "7859": "Steengrijs",
        "8745": "Spargroen", "9794": "Groen Camouflage",
        "12735": "Cranberry", "12736": "Donkergrijs",
    },
    "PL": {
        "7853": "Bialy", "7854": "Czarny", "7855": "Bezowy", "7856": "Jasnoniebieski",
        "7857": "Granatowy", "7858": "Rozowy", "7859": "Szary Kamien",
        "8745": "Ciemnozielony", "9794": "Zielony Kamuflaz",
        "12735": "Zurawinowy", "12736": "Ciemnoszary",
    },
    "SE": {
        "7853": "Vit", "7854": "Svart", "7855": "Beige", "7856": "Ljusbla",
        "7857": "Marinbla", "7858": "Rosa", "7859": "Stengra",
        "8745": "Grangren", "9794": "Gron kamouflage",
        "12735": "Tranbar", "12736": "Morkgra",
    },
    "BE": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Vert Sapin", "9794": "Camouflage Vert",
        "12735": "Canneberge", "12736": "Gris Fonce",
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

    for parent_sku, label in [(FLAG_PARENT_SKU, "Flag parent"),
                               (NOFLAG_PARENT_SKU, "No-flag parent"),
                               (NEW_PARENT_SKU, "New merged parent")]:
        print(f"\n  --- {label}: {parent_sku} ---")
        code, data = api.get_listing(parent_sku, de_mp)
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
            issues = data.get("issues", [])
            if issues:
                print(f"  Issues ({len(issues)}):")
                for issue in issues[:3]:
                    print(f"    [{issue.get('severity','')}] {issue.get('code','')}: {issue.get('message','')[:100]}")
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
            print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
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
            print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                print(f"  ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Inspect complete.\n")


# =============================================================================
# STEP: CREATE PARENT
# =============================================================================

def step_create_parent(api, results, dry_run=False):
    """Create MEGA-BELGIUM parent with STYLE_NAME/COLOR_NAME variation theme on all marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: CREATE PARENT ({NEW_PARENT_SKU}) ON ALL MARKETPLACES")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]

        body = {
            "productType": "HAT",
            "requirements": "LISTING",
            "attributes": {
                "merchant_suggested_asin": [{"value": "new", "marketplace_id": mp_id}],
                "condition_type": [{"value": "new_new", "marketplace_id": mp_id}],
                "parentage_level": [{"value": "parent", "marketplace_id": mp_id}],
                "variation_theme": [{"name": "STYLE_NAME/COLOR_NAME", "marketplace_id": mp_id}],
                "item_name": [{"value": "Make Belgium Great Again Cap", "language_tag": lang, "marketplace_id": mp_id}],
                "brand": [{"value": "Generic", "language_tag": lang, "marketplace_id": mp_id}],
            }
        }

        if dry_run:
            print(f"  [DRY] Would PUT {NEW_PARENT_SKU} on {mkt_code} (parent, STYLE_NAME/COLOR_NAME)")
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
    """
    Re-link all 22 children to MEGA-BELGIUM with proper 2D variation attributes.
    Flag children:    style = "mit Flagge" (localized), color = pure color
    No-flag children: style = "ohne Flagge" (localized), color = pure color
    """
    print("\n" + "=" * 70)
    print("  STEP: RELINK ALL CHILDREN TO MEGA-BELGIUM")
    print("=" * 70)

    total = 0

    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        flag_with = FLAG_LABELS[mkt_code]["with"]
        flag_without = FLAG_LABELS[mkt_code]["without"]
        mkt_colors = COLORS.get(mkt_code, COLORS["DE"])

        print(f"\n  ===== {mkt_code} =====")
        print(f"  style values: '{flag_with}' / '{flag_without}'")

        # --- Flag children ---
        print(f"\n  --- {mkt_code}: {len(FLAG_CHILDREN)} flag children (style='{flag_with}') ---")
        for suffix_id in CHILD_SUFFIXES:
            sku = f"PFT-93855556-{suffix_id}"
            base_color = mkt_colors.get(suffix_id, "")
            if not base_color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color mapping for suffix {suffix_id}")
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
                    "value": [{"value": base_color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": flag_with, "language_tag": lang, "marketplace_id": mp_id}]
                }
            ]

            if dry_run:
                print(f"  [DRY] {sku}: parent={NEW_PARENT_SKU}, color='{base_color}', style='{flag_with}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "relink_flag_child", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

        # --- No-flag children ---
        print(f"\n  --- {mkt_code}: {len(NOFLAG_CHILDREN)} no-flag children (style='{flag_without}') ---")
        for suffix_id in CHILD_SUFFIXES:
            sku = f"PFT-93856238-{suffix_id}"
            base_color = mkt_colors.get(suffix_id, "")
            if not base_color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color mapping for suffix {suffix_id}")
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
                    "value": [{"value": base_color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": flag_without, "language_tag": lang, "marketplace_id": mp_id}]
                }
            ]

            if dry_run:
                print(f"  [DRY] {sku}: parent={NEW_PARENT_SKU}, color='{base_color}', style='{flag_without}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "relink_noflag_child", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Re-linked {total} children across all marketplaces.")
    return total


# =============================================================================
# STEP: DEACTIVATE OLD PARENTS
# =============================================================================

def step_deactivate_old_parents(api, results, dry_run=False):
    """Deactivate PFT-93855556 and PFT-93856238 parents on all 8 marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: DEACTIVATE OLD PARENTS")
    print(f"  Flag parent: {FLAG_PARENT_SKU}")
    print(f"  No-flag parent: {NOFLAG_PARENT_SKU}")
    print("=" * 70)

    total = 0
    for parent_sku in [FLAG_PARENT_SKU, NOFLAG_PARENT_SKU]:
        for mkt_code, mp_id in MARKETPLACE_IDS.items():
            if dry_run:
                print(f"  [DRY] Would DELETE {parent_sku} on {mkt_code}")
                total += 1
                continue

            code, resp = api.delete_listing(parent_sku, mp_id)
            log_result(results, "deactivate_old_parent", parent_sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Deactivated {total} parent-marketplace combinations.")
    return total


# =============================================================================
# STEP: VERIFY
# =============================================================================

def step_verify(api):
    """Verify the merged 2D variation is correctly set up on DE."""
    print("\n" + "=" * 70)
    print("  STEP: VERIFY 2D VARIATION ON DE")
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
            s = summaries[0]
            print(f"  ASIN: {s.get('asin','?')}, status: {json.dumps(s.get('status', []))}")
        issues = data.get("issues", [])
        if issues:
            err_count = sum(1 for i in issues if i.get("severity") == "ERROR")
            warn_count = sum(1 for i in issues if i.get("severity") == "WARNING")
            print(f"  Issues: {err_count} errors, {warn_count} warnings")
            for issue in issues[:5]:
                print(f"    [{issue.get('severity','')}] {issue.get('code','')}: {issue.get('message','')[:120]}")
    else:
        print(f"  Not found or error (HTTP {code})")
    time.sleep(SLEEP_BETWEEN)

    # Sample flag child
    sku = FLAG_CHILDREN[0]
    print(f"\n  --- Flag child: {sku} ---")
    code, data = api.get_listing(sku, de_mp)
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  color: {json.dumps(attrs.get('color', []))}")
        print(f"  style: {json.dumps(attrs.get('style', []))}")
        print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
    time.sleep(SLEEP_BETWEEN)

    # Sample no-flag child
    sku = NOFLAG_CHILDREN[0]
    print(f"\n  --- No-flag child: {sku} ---")
    code, data = api.get_listing(sku, de_mp)
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  color: {json.dumps(attrs.get('color', []))}")
        print(f"  style: {json.dumps(attrs.get('style', []))}")
        print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
    time.sleep(SLEEP_BETWEEN)

    # Check old parents are gone
    for parent_sku, label in [(FLAG_PARENT_SKU, "Old flag parent"),
                               (NOFLAG_PARENT_SKU, "Old no-flag parent")]:
        print(f"\n  --- {label}: {parent_sku} ---")
        code, data = api.get_listing(parent_sku, de_mp)
        if code == 200 and data:
            summaries = data.get("summaries", [])
            if summaries:
                status = summaries[0].get("status", [])
                print(f"  Still exists! status: {json.dumps(status)}")
            else:
                print(f"  HTTP 200 but no summaries (possibly deleted)")
        elif code == 404:
            print(f"  Confirmed deleted (404)")
        else:
            print(f"  HTTP {code}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Verify complete.\n")


# =============================================================================
# REPORT
# =============================================================================

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

    # Expected counts
    print(f"\n  Expected:")
    print(f"    Parent created on 8 marketplaces")
    print(f"    Flag children re-linked: 11 x 8 = 88")
    print(f"    No-flag children re-linked: 11 x 8 = 88")
    print(f"    Old parents deactivated: 2 x 8 = 16")
    print(f"    Total expected: 200 API calls")

    # Save results
    results_path = Path(__file__).parent / "merge_belgium_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"\n  Results saved to: {results_path}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Merge Belgium flag + no-flag listings into 2D variation (STYLE_NAME/COLOR_NAME)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without making API calls")
    parser.add_argument("--step",
                        choices=["inspect", "create-parent", "relink", "deactivate", "verify"],
                        help="Run only a specific step")
    args = parser.parse_args()

    print("=" * 70)
    print("  MERGE BELGIUM LISTINGS -> 2D VARIATION (STYLE_NAME/COLOR_NAME)")
    print(f"  New parent: {NEW_PARENT_SKU}")
    print(f"  Flag parent (deactivate): {FLAG_PARENT_SKU}")
    print(f"  No-flag parent (deactivate): {NOFLAG_PARENT_SKU}")
    print(f"  Flag children: {len(FLAG_CHILDREN)} SKUs")
    print(f"  No-flag children: {len(NOFLAG_CHILDREN)} SKUs")
    print(f"  Total children: {len(FLAG_CHILDREN) + len(NOFLAG_CHILDREN)}")
    print(f"  Variation theme: STYLE_NAME/COLOR_NAME")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Marketplaces: {', '.join(MARKETPLACE_IDS.keys())}")
    print("=" * 70)

    api = AmazonAPI(AMZ_CREDS)
    results = []

    if args.step == "inspect":
        step_inspect(api)
        return

    if args.step == "create-parent":
        step_create_parent(api, results, args.dry_run)
        print_report(results)
        return

    if args.step == "relink":
        step_relink_children(api, results, args.dry_run)
        print_report(results)
        return

    if args.step == "deactivate":
        step_deactivate_old_parents(api, results, args.dry_run)
        print_report(results)
        return

    if args.step == "verify":
        step_verify(api)
        return

    # Full run: all steps
    step_inspect(api)

    step_create_parent(api, results, args.dry_run)

    # Wait a bit for parent to propagate before relinking children
    if not args.dry_run:
        print("\n  Waiting 5s for parent to propagate...")
        time.sleep(5)

    step_relink_children(api, results, args.dry_run)

    # Wait before deactivating old parents
    if not args.dry_run:
        print("\n  Waiting 5s before deactivating old parents...")
        time.sleep(5)

    step_deactivate_old_parents(api, results, args.dry_run)

    # Verify
    if not args.dry_run:
        print("\n  Waiting 5s before verification...")
        time.sleep(5)
        step_verify(api)

    print_report(results)


if __name__ == "__main__":
    main()
