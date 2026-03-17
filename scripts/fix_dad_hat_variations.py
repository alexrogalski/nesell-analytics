#!/usr/bin/env python3.11
"""
Fix broken dad hat Amazon variation families.

Based on investigation (2026-03-14), the actual state is:
  - Most families WORK (Belgium, Britain, France, Netherlands, Italy, Poland, America, Germany)
  - Sweden: parent PFT-90201053 missing parentage attrs, children already linked
  - Spain: parent PFT-93855391 missing parentage attrs, children fully orphaned
  - Europe: MEGA-EUROPE missing flag children, "Klasyczny" style garbage (separate fix)

This script fixes Sweden and Spain. Europe needs separate handling (creating new listings).

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/fix_dad_hat_variations.py --dry-run
  python3.11 scripts/fix_dad_hat_variations.py --fix sweden
  python3.11 scripts/fix_dad_hat_variations.py --fix spain
  python3.11 scripts/fix_dad_hat_variations.py --fix europe-cleanup
  python3.11 scripts/fix_dad_hat_variations.py --verify
  python3.11 scripts/fix_dad_hat_variations.py                      # fix all
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

PRODUCT_TYPE = "HAT"

MARKETPLACES = {
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS",
    "NL": "A1805IZSGTT6HS",
    "PL": "A1C3SOZRARQ6R3",
    "SE": "A2NODRKZP88ZB9",
    "BE": "AMEN7PMS3EDWL",
}

CHILD_SUFFIXES = ["7853", "7854", "7855", "7856", "7857", "7858", "7859", "8745", "9794", "12735", "12736"]
COLOR_NAMES = {
    "7853": "White", "7854": "Black", "7855": "Khaki", "7856": "Light Blue",
    "7857": "Navy", "7858": "Pink", "7859": "Stone", "8745": "Spruce",
    "9794": "Green Camo", "12735": "Cranberry", "12736": "Dark Grey",
}

# Legacy Europe children (numeric Printful SKU format)
EUROPE_LEGACY_CHILDREN = [f"6843674_{s}" for s in
    ["7853", "7854", "7855", "7856", "7857", "7858", "7859", "9794", "12735", "12736"]]
EUROPE_EXTRA_CHILDREN = ["PFT-82980216-8745", "PFT-88471944-8745"]

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
                print(f"    [GET {sku}] 429, waiting {wait}s")
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

    def patch_listing(self, sku, mp_id, patches, retries=8):
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
        body = {"productType": PRODUCT_TYPE, "patches": patches}
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
                print(f"    [PATCH {sku}] 429, waiting {wait}s")
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


# -- Logging ------------------------------------------------------------------

def log_result(results, action, sku, mkt, status_code, resp):
    issues = resp.get("issues", []) if isinstance(resp, dict) else []
    errors = [i for i in issues if i.get("severity") == "ERROR"]
    warnings = [i for i in issues if i.get("severity") == "WARNING"]
    status = resp.get("status", "?") if isinstance(resp, dict) else "?"

    raw_errors = resp.get("errors", []) if isinstance(resp, dict) else []
    if raw_errors and not errors:
        errors = raw_errors

    entry = {
        "action": action, "sku": sku, "marketplace": mkt,
        "http_status": status_code, "api_status": status,
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
# FIX SWEDEN
# Parent PFT-90201053 needs parentage_level=parent, variation_theme=COLOR.
# Children already have parentage_level=child, parent_sku=PFT-90201053.
# Delete garbage "Klassisk"/"Klassisch" style attribute from children.
# =============================================================================

def fix_sweden(api, results, dry_run=False):
    parent_sku = "PFT-90201053"
    print("\n" + "=" * 70)
    print(f"  FIX SWEDEN: {parent_sku}")
    print("  Parent: set parentage_level=parent, variation_theme=COLOR")
    print("  Children: delete style attribute (Klassisk garbage)")
    print("=" * 70)

    # Step 1: Patch parent
    print("\n  --- Patching parent ---")
    for mkt_code, mp_id in MARKETPLACES.items():
        patches = [
            {"op": "replace", "path": "/attributes/parentage_level",
             "value": [{"marketplace_id": mp_id, "value": "parent"}]},
            {"op": "replace", "path": "/attributes/variation_theme",
             "value": [{"marketplace_id": mp_id, "name": "COLOR"}]},
        ]
        if dry_run:
            print(f"  [DRY] PATCH {parent_sku} on {mkt_code}: parent, COLOR")
            continue
        code, resp = api.patch_listing(parent_sku, mp_id, patches)
        log_result(results, "fix_parent", parent_sku, mkt_code, code, resp)
        time.sleep(SLEEP_BETWEEN)

    # Note: delete style via PATCH op:"delete" causes Amazon internal error (4000000).
    # Style "Klassisk" is cosmetic and ignored when variation_theme=COLOR.
    # Children already linked to parent, so Sweden should work now.

    print(f"\n  Sweden fix complete.")


# =============================================================================
# FIX SPAIN
# Parent PFT-93855391 needs parentage_level=parent, variation_theme=COLOR.
# Children need parentage_level=child, child_parent_sku_relationship -> parent.
# Delete garbage "Klassisch" style attribute from children.
# =============================================================================

def fix_spain(api, results, dry_run=False):
    parent_sku = "PFT-93855391"
    print("\n" + "=" * 70)
    print(f"  FIX SPAIN: {parent_sku}")
    print("  Parent: set parentage_level=parent, variation_theme=COLOR")
    print("  Children: link to parent + delete style")
    print("=" * 70)

    # Step 1: Patch parent
    print("\n  --- Patching parent ---")
    for mkt_code, mp_id in MARKETPLACES.items():
        patches = [
            {"op": "replace", "path": "/attributes/parentage_level",
             "value": [{"marketplace_id": mp_id, "value": "parent"}]},
            {"op": "replace", "path": "/attributes/variation_theme",
             "value": [{"marketplace_id": mp_id, "name": "COLOR"}]},
        ]
        if dry_run:
            print(f"  [DRY] PATCH {parent_sku} on {mkt_code}: parent, COLOR")
            continue
        code, resp = api.patch_listing(parent_sku, mp_id, patches)
        log_result(results, "fix_parent", parent_sku, mkt_code, code, resp)
        time.sleep(SLEEP_BETWEEN)

    # Step 2: Link children to parent (without delete style - causes Amazon internal error)
    print("\n  --- Linking children ---")
    for suffix in CHILD_SUFFIXES:
        child_sku = f"PFT-93855391-{suffix}"
        color = COLOR_NAMES.get(suffix, suffix)
        for mkt_code, mp_id in MARKETPLACES.items():
            patches = [
                {"op": "replace", "path": "/attributes/parentage_level",
                 "value": [{"marketplace_id": mp_id, "value": "child"}]},
                {"op": "replace", "path": "/attributes/child_parent_sku_relationship",
                 "value": [{"child_relationship_type": "variation",
                            "parent_sku": parent_sku, "marketplace_id": mp_id}]},
            ]
            if dry_run:
                print(f"  [DRY] LINK {child_sku} ({color}) -> {parent_sku} on {mkt_code}")
                continue
            code, resp = api.patch_listing(child_sku, mp_id, patches)
            log_result(results, "link_child", child_sku, mkt_code, code, resp)
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Spain fix complete.")


# =============================================================================
# FIX EUROPE (cleanup only)
# MEGA-EUROPE parent already works. Children have wrong style values.
# Delete "Klassisch"/"Klassisk" style from legacy children (6843674_*).
# The "mit EU-Flagge" child (PFT-82980216-8745) keeps its style.
# =============================================================================

def fix_europe_cleanup(api, results, dry_run=False):
    print("\n" + "=" * 70)
    print("  FIX EUROPE: cleanup style attributes")
    print("  Legacy children (6843674_*): should be 'ohne Flagge' (already set)")
    print("  PFT-88471944-8745: should be 'ohne Flagge' (already set)")
    print("  PFT-82980216-8745: should be 'mit EU-Flagge' (already set)")
    print("  NOTE: Missing flag children need to be created separately")
    print("=" * 70)

    # Check current state of MEGA-EUROPE on DE
    de_mp = MARKETPLACES["DE"]
    print("\n  --- Current MEGA-EUROPE state (DE) ---")
    code, data = api.get_listing("MEGA-EUROPE", de_mp)
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
        print(f"  variation_theme: {json.dumps(attrs.get('variation_theme', []))}")
        print(f"  style: {json.dumps(attrs.get('style', []))}")
    time.sleep(SLEEP_BETWEEN)

    # Check a sample child
    sample_sku = "6843674_7859"  # The ASIN user complained about (B0DTHVX359)
    print(f"\n  --- Sample child: {sample_sku} ---")
    code, data = api.get_listing(sample_sku, de_mp)
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  style: {json.dumps(attrs.get('style', []))}")
        print(f"  color: {json.dumps(attrs.get('color', []))}")
        print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
        print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
    time.sleep(SLEEP_BETWEEN)

    print(f"\n  Europe state checked. Missing flag children need to be created via")
    print(f"  Printful template 99682362 -> new listings -> link to MEGA-EUROPE.")
    print(f"  This is a separate task (creating new Amazon listings).")


# =============================================================================
# VERIFY
# =============================================================================

def verify(api):
    print("\n" + "=" * 70)
    print("  VERIFY ALL VARIATION FAMILIES (DE)")
    print("=" * 70)

    de_mp = MARKETPLACES["DE"]

    parents = [
        ("MEGA-BELGIUM", "Belgium merged"),
        ("MEGA-BRITAIN", "GB merged"),
        ("PFT-MEGA-FR", "France merged"),
        ("PFT-MEGA-NL2", "Netherlands merged"),
        ("PFT-MEGA-IT", "Italy merged"),
        ("PFT-MEGA-PL", "Poland merged"),
        ("MEGA-AMERICA", "America"),
        ("MEGA-GERMANY-OLD", "Germany merged"),
        ("MEGA-EUROPE", "Europe merged"),
        ("PFT-90201053", "Sweden"),
        ("PFT-93855391", "Spain"),
    ]

    for parent_sku, label in parents:
        print(f"\n  --- {label}: {parent_sku} ---")
        code, data = api.get_listing(parent_sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            parentage = attrs.get("parentage_level", [])
            theme = attrs.get("variation_theme", [])
            p_val = parentage[0].get("value", "?") if parentage else "NOT SET"
            t_val = theme[0].get("name", "?") if theme else "NOT SET"
            summaries = data.get("summaries", [])
            asin = summaries[0].get("asin", "?") if summaries else "no ASIN"
            status_list = summaries[0].get("status", []) if summaries else []
            print(f"  ASIN: {asin} | parentage: {p_val} | theme: {t_val} | status: {status_list}")

            issues = data.get("issues", [])
            errs = sum(1 for i in issues if i.get("severity") == "ERROR")
            warns = sum(1 for i in issues if i.get("severity") == "WARNING")
            if errs or warns:
                print(f"  Issues: {errs} errors, {warns} warnings")
        elif code == 404:
            print(f"  NOT FOUND (404)")
        else:
            print(f"  HTTP {code}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Verification complete.\n")


# =============================================================================
# REPORT
# =============================================================================

def print_report(results):
    print("\n" + "=" * 70)
    print("  FINAL REPORT")
    print("=" * 70)

    if not results:
        print("  No API calls were made (dry run or verify only).")
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
        for r in errors_list[:20]:
            print(f"  {r['action']} {r['sku']} on {r['marketplace']}: {'; '.join(r['errors'][:3])}")

    total_ok = sum(c["ok"] for c in actions.values())
    total_err = sum(c["err"] for c in actions.values())
    print(f"\n  Summary: {total_ok} OK, {total_err} errors out of {len(results)} API calls")

    results_path = Path(__file__).parent / "fix_dad_hat_variations_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"\n  Results saved to: {results_path}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Fix broken dad hat variation families on Amazon")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without making API calls")
    parser.add_argument("--fix", choices=["sweden", "spain", "europe-cleanup", "all"],
                        default="all",
                        help="Which family to fix (default: all)")
    parser.add_argument("--verify", action="store_true",
                        help="Only run verification of all families")
    args = parser.parse_args()

    print("=" * 70)
    print("  FIX DAD HAT VARIATION FAMILIES")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Fix: {args.fix}")
    print(f"  Marketplaces: {', '.join(MARKETPLACES.keys())}")
    print("=" * 70)

    api = AmazonAPI(AMZ_CREDS)
    results = []

    if args.verify:
        verify(api)
        return

    if args.fix in ("sweden", "all"):
        fix_sweden(api, results, args.dry_run)

    if args.fix in ("spain", "all"):
        fix_spain(api, results, args.dry_run)

    if args.fix in ("europe-cleanup", "all"):
        fix_europe_cleanup(api, results, args.dry_run)

    if not args.dry_run:
        print("\n  Waiting 5s before verification...")
        time.sleep(5)
        verify(api)

    print_report(results)


if __name__ == "__main__":
    main()
