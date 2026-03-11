#!/usr/bin/env python3.11
"""
Merge "Make Germany Great Again with Flag" variants (PFT-100032925-*) into the
existing "without Flag" parent listing (MEGA-GERMANY-OLD) on all 8 EU marketplaces.

Result: ONE listing page where customer chooses from combined color variants:
  - "Schwarz - ohne Flagge" / "Schwarz - mit Flagge" etc.

Strategy: append " - ohne Flagge" / " - mit Flagge" to color attribute values
to differentiate the two groups within a single COLOR variation family.

Steps:
  1. Get LWA token + inspect current state
  2. Read current color for each old child from API
  3. Update old children color: "{color} - ohne Flagge" (localized per marketplace)
  4. Re-link new children to old parent: set parentage_level=child +
     child_parent_sku_relationship + color="{color} - mit Flagge"
  5. Deactivate the separate PFT-100032925 parent on all 8 marketplaces

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/merge_germany_families.py
  python3.11 scripts/merge_germany_families.py --dry-run
  python3.11 scripts/merge_germany_families.py --step inspect
  python3.11 scripts/merge_germany_families.py --step update-old
  python3.11 scripts/merge_germany_families.py --step relink-new
  python3.11 scripts/merge_germany_families.py --step deactivate-parent
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
NEW_PARENT_SKU = "PFT-100032925"

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
    "S1-R7LS-OW9V",   # inactive (Cranberry, qty=0)
    "SU-HHEA-WWB5",   # inactive (Tannengruen, qty=0)
]

# New children (with flag) - 9 active variant suffixes from PFT-100032925
NEW_CHILD_SUFFIXES = ["7853", "7854", "7855", "7856", "7857", "7858", "7859", "9794", "12736"]
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

# Localized flag suffixes
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
COLORS = {
    "DE": {
        "7853": "Weiss", "7854": "Schwarz", "7855": "Beige", "7856": "Hellblau",
        "7857": "Marineblau", "7858": "Rosa", "7859": "Steingrau",
        "9794": "Gruenes Tarnmuster", "12736": "Dunkelgrau",
    },
    "FR": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "9794": "Camouflage Vert", "12736": "Gris Fonce",
    },
    "IT": {
        "7853": "Bianco", "7854": "Nero", "7855": "Beige", "7856": "Azzurro",
        "7857": "Blu Navy", "7858": "Rosa", "7859": "Grigio Pietra",
        "9794": "Mimetico Verde", "12736": "Grigio Scuro",
    },
    "ES": {
        "7853": "Blanco", "7854": "Negro", "7855": "Beige", "7856": "Azul Claro",
        "7857": "Azul Marino", "7858": "Rosa", "7859": "Gris Piedra",
        "9794": "Camuflaje Verde", "12736": "Gris Oscuro",
    },
    "NL": {
        "7853": "Wit", "7854": "Zwart", "7855": "Beige", "7856": "Lichtblauw",
        "7857": "Marineblauw", "7858": "Roze", "7859": "Steengrijs",
        "9794": "Groen Camouflage", "12736": "Donkergrijs",
    },
    "PL": {
        "7853": "Bialy", "7854": "Czarny", "7855": "Bezowy", "7856": "Jasnoniebieski",
        "7857": "Granatowy", "7858": "Rozowy", "7859": "Szary Kamien",
        "9794": "Zielony Kamuflaz", "12736": "Ciemnoszary",
    },
    "SE": {
        "7853": "Vit", "7854": "Svart", "7855": "Beige", "7856": "Ljusbla",
        "7857": "Marinbla", "7858": "Rosa", "7859": "Stengra",
        "9794": "Gron kamouflage", "12736": "Morkgra",
    },
    "BE": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "9794": "Camouflage Vert", "12736": "Gris Fonce",
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

    # Also catch raw API errors (like 400 InvalidInput)
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
# STEP: INSPECT
# ═══════════════════════════════════════════════════════════════════════

def step_inspect(api):
    """Inspect the current parent and sample children on DE."""
    print("\n" + "=" * 70)
    print("  STEP: INSPECT CURRENT STATE (DE)")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    print(f"\n  --- Old Parent: {OLD_PARENT_SKU} ---")
    code, data = api.get_listing(OLD_PARENT_SKU, de_mp)
    print(f"  HTTP {code}")
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {attrs.get('variation_theme', [])}")
        print(f"  parentage_level: {attrs.get('parentage_level', [])}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}")
            print(f"  status: {summaries[0].get('status','?')}")
    time.sleep(SLEEP_BETWEEN)

    print(f"\n  --- New Parent (to deactivate): {NEW_PARENT_SKU} ---")
    code, data = api.get_listing(NEW_PARENT_SKU, de_mp)
    print(f"  HTTP {code}")
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {attrs.get('variation_theme', [])}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}")
    time.sleep(SLEEP_BETWEEN)

    # Check a few old children
    for sku in OLD_CHILDREN[:3]:
        print(f"\n  --- Old child: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {attrs.get('color', [])}")
            print(f"  child_parent_sku_relationship: {attrs.get('child_parent_sku_relationship', [])}")
        time.sleep(SLEEP_BETWEEN)

    # Check a few new children
    for sku in NEW_CHILDREN[:2]:
        print(f"\n  --- New child: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {attrs.get('color', [])}")
            print(f"  parentage_level: {attrs.get('parentage_level', [])}")
            print(f"  child_parent_sku_relationship: {attrs.get('child_parent_sku_relationship', [])}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Inspect complete.\n")


# ═══════════════════════════════════════════════════════════════════════
# STEP: READ OLD CHILDREN COLORS
# ═══════════════════════════════════════════════════════════════════════

def read_old_children_colors(api, mkt_code, mp_id):
    """Read current color values for old children from API.
    Returns dict: SKU -> color_value (without any existing suffix)."""
    colors = {}
    for sku in OLD_CHILDREN:
        code, data = api.get_listing(sku, mp_id)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            color_list = attrs.get("color", [])
            if color_list:
                raw_color = color_list[0].get("value", "")
                # Strip existing " - ohne Flagge" suffix if already applied
                for suffix_key in ("without", "with"):
                    suffix = f" - {FLAG_LABELS[mkt_code][suffix_key]}"
                    if raw_color.endswith(suffix):
                        raw_color = raw_color[:-len(suffix)]
                        break
                colors[sku] = raw_color
            else:
                colors[sku] = ""
                print(f"  [WARN] No color found for {sku} on {mkt_code}")
        else:
            colors[sku] = ""
            print(f"  [WARN] Could not read {sku} on {mkt_code}: HTTP {code}")
        time.sleep(SLEEP_BETWEEN)
    return colors


# ═══════════════════════════════════════════════════════════════════════
# STEP: UPDATE OLD CHILDREN COLORS
# ═══════════════════════════════════════════════════════════════════════

def step_update_old_children(api, results, dry_run=False):
    """Update old children color to '{color} - ohne Flagge' on all marketplaces."""
    print("\n" + "=" * 70)
    print("  STEP: UPDATE OLD CHILDREN COLOR -> '{color} - ohne Flagge'")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        suffix = f" - {FLAG_LABELS[mkt_code]['without']}"
        lang = LANG_TAGS[mkt_code]
        print(f"\n  --- {mkt_code}: reading current colors ---")

        # Read current colors from API
        sku_colors = read_old_children_colors(api, mkt_code, mp_id)

        print(f"  --- {mkt_code}: updating with suffix '{suffix}' ---")
        for sku in OLD_CHILDREN:
            base_color = sku_colors.get(sku, "")
            if not base_color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color found")
                continue
            new_color = f"{base_color}{suffix}"

            patches = [{
                "op": "replace",
                "path": "/attributes/color",
                "value": [{"value": new_color, "language_tag": lang, "marketplace_id": mp_id}]
            }]
            if dry_run:
                print(f"  [DRY] {sku} on {mkt_code}: '{base_color}' -> '{new_color}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "update_color_without_flag", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Updated {total} old children across all marketplaces.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# STEP: RE-LINK NEW CHILDREN
# ═══════════════════════════════════════════════════════════════════════

def step_relink_new_children(api, results, dry_run=False):
    """Re-link PFT-100032925-* children to old parent + set color='{color} - mit Flagge'."""
    print("\n" + "=" * 70)
    print("  STEP: RE-LINK NEW CHILDREN + COLOR='{color} - mit Flagge'")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        flag_suffix = f" - {FLAG_LABELS[mkt_code]['with']}"
        lang = LANG_TAGS[mkt_code]
        mkt_colors = COLORS.get(mkt_code, COLORS["DE"])
        parent_sku = OLD_PARENT_SKU
        print(f"\n  --- {mkt_code}: parent={parent_sku}, suffix='{flag_suffix}' ---")

        for suffix_id in NEW_CHILD_SUFFIXES:
            sku = f"PFT-100032925-{suffix_id}"
            base_color = mkt_colors.get(suffix_id, "")
            new_color = f"{base_color}{flag_suffix}"

            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/child_parent_sku_relationship",
                    "value": [{
                        "child_relationship_type": "variation",
                        "parent_sku": parent_sku,
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
                    "value": [{"value": new_color, "language_tag": lang, "marketplace_id": mp_id}]
                }
            ]
            if dry_run:
                print(f"  [DRY] {sku} on {mkt_code}: color='{new_color}', parent={parent_sku}")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "relink_to_old_parent", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Re-linked {total} new children across all marketplaces.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# STEP: DEACTIVATE NEW PARENT
# ═══════════════════════════════════════════════════════════════════════

def step_deactivate_new_parent(api, results, dry_run=False):
    """Deactivate the PFT-100032925 parent on all 8 marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: DEACTIVATE NEW PARENT ({NEW_PARENT_SKU}) ON ALL MARKETPLACES")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        if dry_run:
            print(f"  [DRY] Would DELETE {NEW_PARENT_SKU} on {mkt_code}")
            total += 1
            continue

        code, resp = api.delete_listing(NEW_PARENT_SKU, mp_id)
        log_result(results, "deactivate_parent", NEW_PARENT_SKU, mkt_code, code, resp)
        total += 1
        time.sleep(SLEEP_BETWEEN)

    print(f"\n  Deactivated parent on {total} marketplaces.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Merge Germany Flag variants into old parent listing")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen without making API calls")
    parser.add_argument("--step", choices=["inspect", "update-old", "relink-new", "deactivate-parent"],
                        help="Run only a specific step")
    args = parser.parse_args()

    print("=" * 70)
    print("  MERGE GERMANY FAMILIES (v2 - color-based)")
    print(f"  Old parent (keep): {OLD_PARENT_SKU}")
    print(f"  New parent (deactivate): {NEW_PARENT_SKU}")
    print(f"  Old children (color += ' - ohne Flagge'): {len(OLD_CHILDREN)} SKUs")
    print(f"  New children (relink + color += ' - mit Flagge'): {len(NEW_CHILDREN)} SKUs")
    print(f"  Protected (do NOT touch): {PROTECTED_SKUS}")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Marketplaces: {', '.join(MARKETPLACE_IDS.keys())}")
    print("=" * 70)

    # Safety check
    for sku in PROTECTED_SKUS:
        assert sku not in NEW_CHILDREN, f"SAFETY: {sku} is in NEW_CHILDREN!"
        assert sku not in OLD_CHILDREN, f"SAFETY: {sku} is in OLD_CHILDREN!"
    print("  Safety check passed.")

    api = AmazonAPI(AMZ_CREDS)
    results = []

    if args.step == "inspect" or args.step is None:
        step_inspect(api)
        if args.step == "inspect":
            return

    if args.step == "update-old" or args.step is None:
        step_update_old_children(api, results, args.dry_run)
        if args.step == "update-old":
            print_report(results)
            return

    if args.step == "relink-new" or args.step is None:
        step_relink_new_children(api, results, args.dry_run)
        if args.step == "relink-new":
            print_report(results)
            return

    if args.step == "deactivate-parent" or args.step is None:
        step_deactivate_new_parent(api, results, args.dry_run)
        if args.step == "deactivate-parent":
            print_report(results)
            return

    # Full run: print final report
    print_report(results)


def print_report(results):
    """Print summary report."""
    print("\n" + "=" * 70)
    print("  FINAL REPORT")
    print("=" * 70)

    if not results:
        print("  No API calls were made (dry run or inspect only).")
        return

    # Count by action
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
    relinked = sum(1 for r in results
                   if r["action"] == "relink_to_old_parent"
                   and r["http_status"] in (200, 204) and not r["errors"])
    old_updated = sum(1 for r in results
                      if r["action"] == "update_color_without_flag"
                      and r["http_status"] in (200, 204) and not r["errors"])
    deactivated = sum(1 for r in results
                      if r["action"] == "deactivate_parent"
                      and r["http_status"] in (200, 204) and not r["errors"])

    print(f"\n  Summary:")
    print(f"    Old children color updated (ohne Flagge): {old_updated}")
    print(f"    New children re-linked to {OLD_PARENT_SKU}: {relinked}")
    print(f"    Parent deactivated on marketplaces: {deactivated}")
    print(f"    Total API calls: {len(results)}")
    print(f"\n  Merged listing (DE): https://www.amazon.de/dp/B0GS3K29WB")

    # Save results
    results_path = Path(__file__).parent / "merge_germany_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"  Results saved to: {results_path}")


if __name__ == "__main__":
    main()
