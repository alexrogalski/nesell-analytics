#!/usr/bin/env python3.11
"""
Merge two Poland hat listings into ONE listing with 2D variation (STYLE_NAME/COLOR_NAME).

Current state (from inspect):
  - Existing parent PFT-MEGA-PL (ASIN B0GQTB83S8, DISCOVERABLE, STYLE_NAME/COLOR_NAME)
  - All 22 children already linked to PFT-MEGA-PL with variation_theme STYLE_NAME/COLOR_NAME
  - But style values are "Polen Flagge"/"Polen" instead of localized flag labels
  - Old parents PFT-93855132 and PFT-93856269 still DISCOVERABLE (need deactivation)

What this script does:
  1. Update style+color on flag children (PFT-93855132-*) to localized "with flag" labels
  2. Update style+color on no-flag children (PFT-93856269-*) to localized "without flag" labels
  3. Deactivate both old parents (PFT-93855132, PFT-93856269) on all 8 EU marketplaces

Parent PFT-MEGA-PL is already working - children are linked, no parent creation needed.

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/merge_poland.py --dry-run
  python3.11 scripts/merge_poland.py --step inspect
  python3.11 scripts/merge_poland.py --step update-flag
  python3.11 scripts/merge_poland.py --step update-noflag
  python3.11 scripts/merge_poland.py --step deactivate-parents
  python3.11 scripts/merge_poland.py          # all steps
"""

import argparse
import json
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests

# -- Credentials ---------------------------------------------------------------
KEYS_DIR = Path.home() / ".keys"
AMZ_CREDS = json.loads((KEYS_DIR / "amazon-sp-api.json").read_text())
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"

# -- Constants -----------------------------------------------------------------

ACTIVE_PARENT_SKU = "PFT-MEGA-PL"     # already exists + DISCOVERABLE
FLAG_PARENT_SKU = "PFT-93855132"      # old with-flag parent (to deactivate)
NOFLAG_PARENT_SKU = "PFT-93856269"    # old without-flag parent (to deactivate)

VARIANT_SUFFIXES = ["12735", "12736", "7853", "7854", "7855", "7856", "7857", "7858", "7859", "8745", "9794"]

FLAG_CHILDREN = [f"PFT-93855132-{s}" for s in VARIANT_SUFFIXES]
NOFLAG_CHILDREN = [f"PFT-93856269-{s}" for s in VARIANT_SUFFIXES]

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

# Localized style values (the "flag" dimension)
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

# Color names per variant suffix per marketplace
COLORS = {
    "DE": {
        "12735": "Rot",
        "12736": "Dunkelgrau",
        "7853": "Weiss",
        "7854": "Schwarz",
        "7855": "Beige",
        "7856": "Hellblau",
        "7857": "Marineblau",
        "7858": "Rosa",
        "7859": "Steingrau",
        "8745": "Tannengruen",
        "9794": "Gruenes Tarnmuster",
    },
    "FR": {
        "12735": "Rouge",
        "12736": "Gris Fonce",
        "7853": "Blanc",
        "7854": "Noir",
        "7855": "Beige",
        "7856": "Bleu Clair",
        "7857": "Bleu Marine",
        "7858": "Rose",
        "7859": "Gris Pierre",
        "8745": "Vert Sapin",
        "9794": "Camouflage Vert",
    },
    "IT": {
        "12735": "Rosso",
        "12736": "Grigio Scuro",
        "7853": "Bianco",
        "7854": "Nero",
        "7855": "Beige",
        "7856": "Azzurro",
        "7857": "Blu Navy",
        "7858": "Rosa",
        "7859": "Grigio Pietra",
        "8745": "Verde Abete",
        "9794": "Mimetico Verde",
    },
    "ES": {
        "12735": "Rojo",
        "12736": "Gris Oscuro",
        "7853": "Blanco",
        "7854": "Negro",
        "7855": "Beige",
        "7856": "Azul Claro",
        "7857": "Azul Marino",
        "7858": "Rosa",
        "7859": "Gris Piedra",
        "8745": "Verde Abeto",
        "9794": "Camuflaje Verde",
    },
    "NL": {
        "12735": "Rood",
        "12736": "Donkergrijs",
        "7853": "Wit",
        "7854": "Zwart",
        "7855": "Beige",
        "7856": "Lichtblauw",
        "7857": "Marineblauw",
        "7858": "Roze",
        "7859": "Steengrijs",
        "8745": "Dennengroen",
        "9794": "Groen Camouflage",
    },
    "PL": {
        "12735": "Czerwony",
        "12736": "Ciemnoszary",
        "7853": "Bialy",
        "7854": "Czarny",
        "7855": "Bezowy",
        "7856": "Jasnoniebieski",
        "7857": "Granatowy",
        "7858": "Rozowy",
        "7859": "Szary Kamien",
        "8745": "Zielony Jodlowy",
        "9794": "Zielony Kamuflaz",
    },
    "SE": {
        "12735": "Rod",
        "12736": "Morkgra",
        "7853": "Vit",
        "7854": "Svart",
        "7855": "Beige",
        "7856": "Ljusbla",
        "7857": "Marinbla",
        "7858": "Rosa",
        "7859": "Stengra",
        "8745": "Grangron",
        "9794": "Gron kamouflage",
    },
    "BE": {
        "12735": "Rouge",
        "12736": "Gris Fonce",
        "7853": "Blanc",
        "7854": "Noir",
        "7855": "Beige",
        "7856": "Bleu Clair",
        "7857": "Bleu Marine",
        "7858": "Rose",
        "7859": "Gris Pierre",
        "8745": "Vert Sapin",
        "9794": "Camouflage Vert",
    },
}

SLEEP_BETWEEN = 0.6


# -- Amazon SP-API Client -----------------------------------------------------

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
        """PUT (create/replace) a listing item."""
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


# -- Report Helpers ------------------------------------------------------------

def log_result(results, action, sku, mkt, status_code, resp):
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


# ==============================================================================
# STEP: INSPECT
# ==============================================================================

def step_inspect(api):
    print("\n" + "=" * 70)
    print("  STEP: INSPECT CURRENT STATE (DE)")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    for label, sku in [("Flag Parent", FLAG_PARENT_SKU), ("NoFlag Parent", NOFLAG_PARENT_SKU)]:
        print(f"\n  --- {label}: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
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

    # Check a few children from each group
    for label, children in [("Flag child", FLAG_CHILDREN[:2]), ("NoFlag child", NOFLAG_CHILDREN[:2])]:
        for sku in children:
            print(f"\n  --- {label}: {sku} ---")
            code, data = api.get_listing(sku, de_mp)
            if code == 200 and data:
                attrs = data.get("attributes", {})
                print(f"  color: {attrs.get('color', [])}")
                print(f"  style: {attrs.get('style', [])}")
                print(f"  parentage_level: {attrs.get('parentage_level', [])}")
                print(f"  child_parent_sku_relationship: {attrs.get('child_parent_sku_relationship', [])}")
                print(f"  variation_theme: {attrs.get('variation_theme', [])}")
                summaries = data.get("summaries", [])
                if summaries:
                    print(f"  ASIN: {summaries[0].get('asin','?')}")
            time.sleep(SLEEP_BETWEEN)

    # Check active parent PFT-MEGA-PL
    print(f"\n  --- Active Parent: {ACTIVE_PARENT_SKU} ---")
    code, data = api.get_listing(ACTIVE_PARENT_SKU, de_mp)
    print(f"  HTTP {code}")
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {attrs.get('variation_theme', [])}")
        print(f"  parentage_level: {attrs.get('parentage_level', [])}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}")
            print(f"  status: {summaries[0].get('status','?')}")

    print("\n  Inspect complete.\n")


# ==============================================================================
# STEP: UPDATE FLAG CHILDREN STYLE + COLOR
# ==============================================================================

def step_update_flag_children(api, results, dry_run=False):
    """Update PFT-93855132-* children: style=localized 'with flag', color=clean."""
    print("\n" + "=" * 70)
    print("  STEP: UPDATE FLAG CHILDREN (style -> localized 'with flag')")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        style_val = STYLE_LABELS[mkt_code]["with"]
        mkt_colors = COLORS.get(mkt_code, COLORS["DE"])
        print(f"\n  --- {mkt_code}: style='{style_val}' ---")

        for suffix_id in VARIANT_SUFFIXES:
            sku = f"PFT-93855132-{suffix_id}"
            color_val = mkt_colors.get(suffix_id, "")

            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/color",
                    "value": [{"value": color_val, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": style_val, "language_tag": lang, "marketplace_id": mp_id}]
                },
            ]
            if dry_run:
                print(f"  [DRY] {sku} on {mkt_code}: style='{style_val}', color='{color_val}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "update_flag_child", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Updated {total} flag children across all marketplaces.")
    return total


# ==============================================================================
# STEP: UPDATE NO-FLAG CHILDREN STYLE + COLOR
# ==============================================================================

def step_update_noflag_children(api, results, dry_run=False):
    """Update PFT-93856269-* children: style=localized 'without flag', color=clean."""
    print("\n" + "=" * 70)
    print("  STEP: UPDATE NO-FLAG CHILDREN (style -> localized 'without flag')")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        style_val = STYLE_LABELS[mkt_code]["without"]
        mkt_colors = COLORS.get(mkt_code, COLORS["DE"])
        print(f"\n  --- {mkt_code}: style='{style_val}' ---")

        for suffix_id in VARIANT_SUFFIXES:
            sku = f"PFT-93856269-{suffix_id}"
            color_val = mkt_colors.get(suffix_id, "")

            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/color",
                    "value": [{"value": color_val, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": style_val, "language_tag": lang, "marketplace_id": mp_id}]
                },
            ]
            if dry_run:
                print(f"  [DRY] {sku} on {mkt_code}: style='{style_val}', color='{color_val}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "update_noflag_child", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Updated {total} no-flag children across all marketplaces.")
    return total


# ==============================================================================
# STEP: DEACTIVATE OLD PARENTS
# ==============================================================================

def step_deactivate_parents(api, results, dry_run=False):
    """Deactivate both old parents on all 8 marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: DEACTIVATE OLD PARENTS ({FLAG_PARENT_SKU}, {NOFLAG_PARENT_SKU})")
    print("=" * 70)

    total = 0
    for parent_sku in [FLAG_PARENT_SKU, NOFLAG_PARENT_SKU]:
        for mkt_code, mp_id in MARKETPLACE_IDS.items():
            if dry_run:
                print(f"  [DRY] Would DELETE {parent_sku} on {mkt_code}")
                total += 1
                continue

            code, resp = api.delete_listing(parent_sku, mp_id)
            log_result(results, "deactivate_parent", parent_sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Deactivated parents on {total} marketplace slots.")
    return total


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Update Poland Flag/NoFlag style labels + deactivate old parents")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen without making API calls")
    parser.add_argument("--step", choices=["inspect", "update-flag", "update-noflag", "deactivate-parents"],
                        help="Run only a specific step")
    args = parser.parse_args()

    print("=" * 70)
    print("  MERGE POLAND FAMILIES (2D variation: STYLE_NAME/COLOR_NAME)")
    print(f"  Active parent (keep): {ACTIVE_PARENT_SKU}")
    print(f"  Flag parent (deactivate): {FLAG_PARENT_SKU}")
    print(f"  NoFlag parent (deactivate): {NOFLAG_PARENT_SKU}")
    print(f"  Flag children (update style): {len(FLAG_CHILDREN)} SKUs")
    print(f"  NoFlag children (update style): {len(NOFLAG_CHILDREN)} SKUs")
    print(f"  Variation theme: STYLE_NAME/COLOR_NAME")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Marketplaces: {', '.join(MARKETPLACE_IDS.keys())}")
    print("=" * 70)

    api = AmazonAPI(AMZ_CREDS)
    results = []

    if args.step == "inspect" or args.step is None:
        step_inspect(api)
        if args.step == "inspect":
            return

    if args.step == "update-flag" or args.step is None:
        step_update_flag_children(api, results, args.dry_run)
        if args.step == "update-flag":
            print_report(results)
            return

    if args.step == "update-noflag" or args.step is None:
        step_update_noflag_children(api, results, args.dry_run)
        if args.step == "update-noflag":
            print_report(results)
            return

    if args.step == "deactivate-parents" or args.step is None:
        step_deactivate_parents(api, results, args.dry_run)
        if args.step == "deactivate-parents":
            print_report(results)
            return

    # Full run
    print_report(results)


def print_report(results):
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
    flag_updated = sum(1 for r in results
                       if r["action"] == "update_flag_child"
                       and r["http_status"] in (200, 204) and not r["errors"])
    noflag_updated = sum(1 for r in results
                         if r["action"] == "update_noflag_child"
                         and r["http_status"] in (200, 204) and not r["errors"])
    deactivated = sum(1 for r in results
                      if r["action"] == "deactivate_parent"
                      and r["http_status"] in (200, 204) and not r["errors"])

    print(f"\n  Summary:")
    print(f"    Active parent: {ACTIVE_PARENT_SKU} (unchanged)")
    print(f"    Flag children style updated: {flag_updated}")
    print(f"    NoFlag children style updated: {noflag_updated}")
    print(f"    Old parents deactivated on: {deactivated} marketplace slots")
    print(f"    Total API calls: {len(results)}")

    results_path = Path(__file__).parent / "merge_poland_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"  Results saved to: {results_path}")


if __name__ == "__main__":
    main()
