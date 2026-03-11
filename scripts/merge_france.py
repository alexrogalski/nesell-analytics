#!/usr/bin/env python3.11
"""
Update France hat variation style labels and deactivate old parents.

Current state (from API inspection):
  - Parent: PFT-MEGA-FR (ASIN B0GQTCRM46, STYLE_NAME/COLOR_NAME theme) -- KEEP
  - Flag children (PFT-93854576-*): style="Frankreich Flagge" -> localized "mit Flagge"
  - NoFlag children (PFT-93856317-*): style="Frankreich" -> localized "ohne Flagge"
  - All children already linked to PFT-MEGA-FR
  - Old parents PFT-93854576 and PFT-93856317 to be deactivated

Steps:
  1. Update flag children style: "Frankreich Flagge" -> "mit Flagge" (localized)
  2. Update noflag children style: "Frankreich" -> "ohne Flagge" (localized)
  3. Deactivate old parents (PFT-93854576 and PFT-93856317) on all 8 EU marketplaces

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/merge_france.py --dry-run
  python3.11 scripts/merge_france.py --step inspect
  python3.11 scripts/merge_france.py --step update-flag
  python3.11 scripts/merge_france.py --step update-noflag
  python3.11 scripts/merge_france.py --step deactivate-parents
  python3.11 scripts/merge_france.py            # full run (all steps)
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

PARENT_SKU = "PFT-MEGA-FR"  # Already exists with STYLE_NAME/COLOR_NAME, ASIN B0GQTCRM46

OLD_FLAG_PARENT = "PFT-93854576"       # with flag - to deactivate
OLD_NOFLAG_PARENT = "PFT-93856317"     # without flag - to deactivate

# Flag children (PFT-93854576-*)
FLAG_SUFFIXES = ["12735", "12736", "7853", "7854", "7855", "7856", "7858", "7859", "8745", "9794"]
FLAG_CHILDREN = [f"PFT-93854576-{s}" for s in FLAG_SUFFIXES]

# No-flag children (PFT-93856317-*)
NOFLAG_SUFFIXES = ["12735", "12736", "7853", "7854", "7855", "7856", "7857", "7858", "7859", "8745", "9794"]
NOFLAG_CHILDREN = [f"PFT-93856317-{s}" for s in NOFLAG_SUFFIXES]

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

# Localized style values for the STYLE dropdown
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
    """Inspect current state of parent and all children on DE."""
    print("\n" + "=" * 70)
    print("  STEP: INSPECT CURRENT STATE (DE)")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    # Check parent
    print(f"\n  --- Parent: {PARENT_SKU} ---")
    code, data = api.get_listing(PARENT_SKU, de_mp)
    print(f"  HTTP {code}")
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {attrs.get('variation_theme', [])}")
        print(f"  parentage_level: {attrs.get('parentage_level', [])}")
        print(f"  item_name: {attrs.get('item_name', [])}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}")
            print(f"  status: {summaries[0].get('status','?')}")
    time.sleep(SLEEP_BETWEEN)

    # Check old parents
    for parent_sku in [OLD_FLAG_PARENT, OLD_NOFLAG_PARENT]:
        print(f"\n  --- Old Parent: {parent_sku} ---")
        code, data = api.get_listing(parent_sku, de_mp)
        print(f"  HTTP {code}")
        if code == 200 and data:
            summaries = data.get("summaries", [])
            if summaries:
                print(f"  ASIN: {summaries[0].get('asin','?')}, status: {summaries[0].get('status','?')}")
        time.sleep(SLEEP_BETWEEN)

    # Check all flag children
    print(f"\n  --- FLAG CHILDREN ---")
    for sku in FLAG_CHILDREN:
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            color = attrs.get("color", [{}])[0].get("value", "?") if attrs.get("color") else "?"
            style = attrs.get("style", [{}])[0].get("value", "?") if attrs.get("style") else "?"
            parent = attrs.get("child_parent_sku_relationship", [{}])[0].get("parent_sku", "?") if attrs.get("child_parent_sku_relationship") else "?"
            summaries = data.get("summaries", [])
            status = summaries[0].get("status", "?") if summaries else "?"
            print(f"  {sku}: color={color}, style={style}, parent={parent}, status={status}")
        else:
            print(f"  {sku}: HTTP {code}")
        time.sleep(SLEEP_BETWEEN)

    # Check all noflag children
    print(f"\n  --- NOFLAG CHILDREN ---")
    for sku in NOFLAG_CHILDREN:
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            color = attrs.get("color", [{}])[0].get("value", "?") if attrs.get("color") else "?"
            style = attrs.get("style", [{}])[0].get("value", "?") if attrs.get("style") else "?"
            parent = attrs.get("child_parent_sku_relationship", [{}])[0].get("parent_sku", "?") if attrs.get("child_parent_sku_relationship") else "?"
            summaries = data.get("summaries", [])
            status = summaries[0].get("status", "?") if summaries else "?"
            print(f"  {sku}: color={color}, style={style}, parent={parent}, status={status}")
        else:
            print(f"  {sku}: HTTP {code}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Inspect complete.\n")


# ═══════════════════════════════════════════════════════════════════════
# STEP: UPDATE FLAG CHILDREN STYLE
# ═══════════════════════════════════════════════════════════════════════

def step_update_flag_children(api, results, dry_run=False):
    """Update flag children style from 'Frankreich Flagge' to localized 'mit Flagge'."""
    print("\n" + "=" * 70)
    print("  STEP: UPDATE FLAG CHILDREN STYLE -> localized 'with flag'")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        style_val = STYLE_LABELS[mkt_code]["with"]
        print(f"\n  --- {mkt_code}: style='{style_val}' ---")

        for sku in FLAG_CHILDREN:
            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": style_val, "language_tag": lang, "marketplace_id": mp_id}]
                },
            ]

            if dry_run:
                print(f"  [DRY] {sku} on {mkt_code}: style -> '{style_val}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "update_flag_style", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Updated {total} flag children across all marketplaces.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# STEP: UPDATE NOFLAG CHILDREN STYLE
# ═══════════════════════════════════════════════════════════════════════

def step_update_noflag_children(api, results, dry_run=False):
    """Update noflag children style from 'Frankreich' to localized 'ohne Flagge'."""
    print("\n" + "=" * 70)
    print("  STEP: UPDATE NOFLAG CHILDREN STYLE -> localized 'without flag'")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        style_val = STYLE_LABELS[mkt_code]["without"]
        print(f"\n  --- {mkt_code}: style='{style_val}' ---")

        for sku in NOFLAG_CHILDREN:
            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": style_val, "language_tag": lang, "marketplace_id": mp_id}]
                },
            ]

            if dry_run:
                print(f"  [DRY] {sku} on {mkt_code}: style -> '{style_val}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "update_noflag_style", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Updated {total} noflag children across all marketplaces.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# STEP: DEACTIVATE OLD PARENTS
# ═══════════════════════════════════════════════════════════════════════

def step_deactivate_parents(api, results, dry_run=False):
    """Deactivate both old parents on all 8 marketplaces."""
    print("\n" + "=" * 70)
    print("  STEP: DEACTIVATE OLD PARENTS")
    print("=" * 70)

    total = 0
    for parent_sku in [OLD_FLAG_PARENT, OLD_NOFLAG_PARENT]:
        for mkt_code, mp_id in MARKETPLACE_IDS.items():
            if dry_run:
                print(f"  [DRY] Would DELETE {parent_sku} on {mkt_code}")
                total += 1
                continue

            code, resp = api.delete_listing(parent_sku, mp_id)
            log_result(results, "deactivate_parent", parent_sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Deactivated {total} parent listings.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Update France hat style labels and deactivate old parents")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without making API calls")
    parser.add_argument("--step",
                        choices=["inspect", "update-flag", "update-noflag", "deactivate-parents"],
                        help="Run only a specific step")
    args = parser.parse_args()

    print("=" * 70)
    print("  MERGE FRANCE FAMILIES (style labels update)")
    print(f"  Parent (keep): {PARENT_SKU} (ASIN B0GQTCRM46)")
    print(f"  Old flag parent (deactivate): {OLD_FLAG_PARENT}")
    print(f"  Old noflag parent (deactivate): {OLD_NOFLAG_PARENT}")
    print(f"  Flag children (style -> 'with flag'): {len(FLAG_CHILDREN)} SKUs")
    print(f"  NoFlag children (style -> 'without flag'): {len(NOFLAG_CHILDREN)} SKUs")
    print(f"  Total children: {len(FLAG_CHILDREN) + len(NOFLAG_CHILDREN)}")
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
    flag_updated = sum(1 for r in results
                       if r["action"] == "update_flag_style"
                       and r["http_status"] in (200, 204) and not r["errors"])
    noflag_updated = sum(1 for r in results
                         if r["action"] == "update_noflag_style"
                         and r["http_status"] in (200, 204) and not r["errors"])
    deactivated = sum(1 for r in results
                      if r["action"] == "deactivate_parent"
                      and r["http_status"] in (200, 204) and not r["errors"])

    print(f"\n  Summary:")
    print(f"    Flag children style updated: {flag_updated}")
    print(f"    NoFlag children style updated: {noflag_updated}")
    print(f"    Old parents deactivated: {deactivated}")
    print(f"    Total API calls: {len(results)}")

    # Save results
    results_path = Path(__file__).parent / "merge_france_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"  Results saved to: {results_path}")


if __name__ == "__main__":
    main()
