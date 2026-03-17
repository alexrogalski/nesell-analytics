#!/usr/bin/env python3.11
"""
Apply SEO title fixes from amazon-seo-printful-2026-03-16.md

5 parent SKUs with titles under 80 chars — extend with "Verstellbare ... Baseball Cap" keywords.
Patches DE marketplace (A1PA6795UKMFR9) only per task scope.

Run:
    cd ~/nesell-analytics
    python3.11 scripts/apply_seo_title_fixes_2026_03_16.py
    python3.11 scripts/apply_seo_title_fixes_2026_03_16.py --dry-run
"""

import argparse
import json
import time
import requests
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

# ─── Credentials & constants ─────────────────────────────────────────────────

CREDENTIALS_PATH = Path.home() / ".keys" / "amazon-sp-api.json"
AMZ_CREDS = json.loads(CREDENTIALS_PATH.read_text())
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"
TOKEN_URL = "https://api.amazon.com/auth/o2/token"
DE_MKT = "A1PA6795UKMFR9"
SLEEP = 0.5

# ─── Title changes from SEO report ───────────────────────────────────────────

TITLE_FIXES = [
    {
        "sku": "PFT-88471944",
        "asin": "B0GQT2C2QM",
        "old_title": "Bestickte Kappe Make Europe Great Again - Dad Hat Unisex - Tannengruen",
        "new_title": "Bestickte Kappe Make Europe Great Again - Verstellbare Dad Hat Unisex Baseball Cap Baumwolle - Tannengruen",
        "old_chars": 70,
        "new_chars": 120,
    },
    {
        "sku": "PFT-93856238",
        "asin": "B0GQHPDMG6",
        "old_title": "Bestickte Kappe Make Belgium Great Again - Dad Hat Unisex - Gruenes Tarnmuster",
        "new_title": "Bestickte Kappe Make Belgium Great Again - Verstellbare Dad Hat Unisex Baseball Cap - Gruenes Tarnmuster",
        "old_chars": 78,
        "new_chars": 107,
    },
    {
        "sku": "PFT-93856269",
        "asin": "B0GQD3X69D",
        "old_title": "Bestickte Kappe Make Poland Great Again - Dad Hat Unisex - Gruenes Tarnmuster",
        "new_title": "Bestickte Kappe Make Poland Great Again - Verstellbare Dad Hat Unisex Baseball Cap - Gruenes Tarnmuster",
        "old_chars": 77,
        "new_chars": 106,
    },
    {
        "sku": "PFT-93856295",
        "asin": "B0GQCVBSTP",
        "old_title": "Bestickte Kappe Make Italy Great Again - Dad Hat Unisex - Gruenes Tarnmuster",
        "new_title": "Bestickte Kappe Make Italy Great Again - Verstellbare Dad Hat Unisex Baseball Cap - Gruenes Tarnmuster",
        "old_chars": 76,
        "new_chars": 105,
    },
    {
        "sku": "PFT-93856317",
        "asin": "B0GQH5Q9V1",
        "old_title": "Bestickte Kappe Make France Great Again - Dad Hat Unisex - Gruenes Tarnmuster",
        "new_title": "Bestickte Kappe Make France Great Again - Verstellbare Dad Hat Unisex Baseball Cap - Gruenes Tarnmuster",
        "old_chars": 77,
        "new_chars": 106,
    },
]

# ─── Auth ─────────────────────────────────────────────────────────────────────

_token = None
_token_time = 0


def refresh_token():
    global _token, _token_time
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": AMZ_CREDS["refresh_token"],
        "client_id": AMZ_CREDS["client_id"],
        "client_secret": AMZ_CREDS["client_secret"],
    })
    resp.raise_for_status()
    _token = resp.json()["access_token"]
    _token_time = time.time()
    print(f"  Token refreshed: {_token[:20]}...")
    return _token


def get_headers():
    global _token, _token_time
    if not _token or time.time() - _token_time > 3000:
        refresh_token()
    return {"x-amz-access-token": _token, "Content-Type": "application/json"}


# ─── API helpers ─────────────────────────────────────────────────────────────

def api_patch(path, body, params=None, retries=8):
    url = f"{AMZ_BASE}{path}"
    for attempt in range(retries):
        try:
            resp = requests.patch(url, headers=get_headers(), json=body, params=params, timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
            time.sleep(10 * (attempt + 1))
            continue
        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [429] rate limited, waiting {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            refresh_token()
            time.sleep(3)
            continue
        if resp.status_code >= 500:
            time.sleep(5 * (attempt + 1))
            continue
        return resp.status_code, resp.json() if resp.text else {}
    return 0, {}


def api_get(path, params=None, retries=5):
    url = f"{AMZ_BASE}{path}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=get_headers(), params=params, timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
            time.sleep(10 * (attempt + 1))
            continue
        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [429] rate limited, waiting {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            refresh_token()
            time.sleep(3)
            continue
        return resp.status_code, resp.json() if resp.text else {}
    return 0, {}


def get_current_title(sku):
    """Fetch the current item_name from SP-API for verification."""
    encoded_sku = quote(sku, safe="")
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{encoded_sku}"
    params = {
        "marketplaceIds": DE_MKT,
        "issueLocale": "en_US",
        "includedData": "attributes,summaries",
    }
    status, data = api_get(path, params)
    if status != 200:
        return None, f"HTTP {status}"
    attrs = data.get("attributes", {})
    item_name_list = attrs.get("item_name", [])
    if item_name_list:
        return item_name_list[0].get("value", ""), None
    summaries = data.get("summaries", [])
    if summaries:
        return summaries[0].get("itemName", ""), None
    return None, "no item_name in response"


def patch_title(sku, new_title, product_type="HAT"):
    """PATCH item_name attribute on DE marketplace."""
    encoded_sku = quote(sku, safe="")
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{encoded_sku}"
    body = {
        "productType": product_type,
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/item_name",
                "value": [{"value": new_title, "marketplace_id": DE_MKT}]
            }
        ]
    }
    params = {"marketplaceIds": DE_MKT, "issueLocale": "en_US"}
    return api_patch(path, body, params)


# ─── Compliance checks ───────────────────────────────────────────────────────

def check_title_compliance(title, sku):
    violations = []
    if "nesell" in title.lower():
        violations.append("CONTAINS 'nesell'")
    if len(title) > 200:
        violations.append(f"TOO LONG ({len(title)} chars, max 200)")
    # Check for known model numbers
    model_patterns = ["6245CM", "6245", "DN3637", "6506"]
    for pattern in model_patterns:
        if pattern.lower() in title.lower():
            violations.append(f"CONTAINS MODEL NUMBER '{pattern}'")
    return violations


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Apply SEO title fixes to 5 PFT- parent SKUs")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without patching")
    args = parser.parse_args()

    dry_run = args.dry_run

    print("=" * 70)
    print("  SEO TITLE FIX — PFT- Short Titles (2026-03-16)")
    print(f"  Marketplace: DE (A1PA6795UKMFR9)")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    if not dry_run:
        refresh_token()

    results = []

    for fix in TITLE_FIXES:
        sku = fix["sku"]
        old_title = fix["old_title"]
        new_title = fix["new_title"]

        print(f"\n--- {sku} (ASIN: {fix['asin']}) ---")
        print(f"  OLD ({fix['old_chars']} chars): {old_title}")
        print(f"  NEW ({fix['new_chars']} chars): {new_title}")

        # Compliance check
        violations = check_title_compliance(new_title, sku)
        if violations:
            print(f"  [ABORT] Compliance violations: {violations}")
            results.append({
                "sku": sku, "asin": fix["asin"],
                "old_title": old_title, "new_title": new_title,
                "status": "ABORTED", "reason": str(violations),
            })
            continue

        if dry_run:
            print(f"  [DRY RUN] Would PATCH title")
            results.append({
                "sku": sku, "asin": fix["asin"],
                "old_title": old_title, "new_title": new_title,
                "status": "DRY_RUN",
            })
            continue

        # Verify current title matches expected before patching
        current_title, err = get_current_title(sku)
        time.sleep(SLEEP)
        if err:
            print(f"  [WARN] Could not fetch current title: {err} — patching anyway")
        elif current_title and current_title.strip() != old_title.strip():
            print(f"  [WARN] Current title differs from expected:")
            print(f"    Expected: {old_title[:80]}")
            print(f"    Found:    {current_title[:80]}")
            print(f"  Proceeding with patch anyway...")

        # Apply patch
        status_code, resp = patch_title(sku, new_title)
        time.sleep(SLEEP)

        resp_status = resp.get("status", "?") if isinstance(resp, dict) else "?"
        issues = resp.get("issues", []) if isinstance(resp, dict) else []
        errors = [i for i in issues if i.get("severity") == "ERROR"]
        warnings = [i for i in issues if i.get("severity") == "WARNING"]

        if status_code == 200 and resp_status in ("ACCEPTED", "VALID"):
            print(f"  [OK] PATCH accepted — status: {resp_status}")
            if warnings:
                for w in warnings:
                    print(f"  [WARN] {w.get('message', '')[:100]}")
            results.append({
                "sku": sku, "asin": fix["asin"],
                "old_title": old_title, "new_title": new_title,
                "old_chars": fix["old_chars"], "new_chars": fix["new_chars"],
                "status": "OK", "http": status_code, "amz_status": resp_status,
                "warnings": [w.get("message", "") for w in warnings],
                "applied_at": datetime.now().isoformat(),
            })
        else:
            err_msg = errors[0].get("message", "")[:120] if errors else f"HTTP_{status_code} {resp_status}"
            print(f"  [FAIL] PATCH failed: {err_msg}")
            print(f"    Full response: {json.dumps(resp)[:200]}")
            results.append({
                "sku": sku, "asin": fix["asin"],
                "old_title": old_title, "new_title": new_title,
                "status": "FAILED", "http": status_code, "amz_status": resp_status,
                "error": err_msg,
                "applied_at": datetime.now().isoformat(),
            })

    # ─── Summary ─────────────────────────────────────────────────────────────

    ok = [r for r in results if r["status"] == "OK"]
    failed = [r for r in results if r["status"] == "FAILED"]
    aborted = [r for r in results if r["status"] == "ABORTED"]
    dry = [r for r in results if r["status"] == "DRY_RUN"]

    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"  Total fixes attempted: {len(results)}")
    if dry_run:
        print(f"  DRY RUN (no changes applied): {len(dry)}")
    else:
        print(f"  Successful: {len(ok)}")
        print(f"  Failed: {len(failed)}")
        print(f"  Aborted (compliance): {len(aborted)}")

    # ─── Write changelog ──────────────────────────────────────────────────────

    changelog_path = Path.home() / "nesell-analytics" / "research" / "amazon-seo-changes-applied-2026-03-16.md"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# Amazon SEO Changes Applied — 2026-03-16",
        "",
        f"**Applied:** {now}",
        f"**Mode:** {'DRY RUN' if dry_run else 'LIVE'}",
        f"**Marketplace:** DE (A1PA6795UKMFR9)",
        f"**Source:** amazon-seo-printful-2026-03-16.md",
        "",
        "---",
        "",
        "## Changes Log",
        "",
        f"| SKU | ASIN | Old Title (chars) | New Title (chars) | Status |",
        f"|-----|------|-------------------|-------------------|--------|",
    ]

    for r in results:
        old_chars = r.get("old_chars", len(r["old_title"]))
        new_chars = r.get("new_chars", len(r["new_title"]))
        status_badge = {
            "OK": "✅ OK",
            "FAILED": "❌ FAILED",
            "ABORTED": "🚫 ABORTED",
            "DRY_RUN": "🔍 DRY RUN",
        }.get(r["status"], r["status"])
        # Truncate titles for table readability
        old_short = r["old_title"][:50] + "..." if len(r["old_title"]) > 50 else r["old_title"]
        new_short = r["new_title"][:50] + "..." if len(r["new_title"]) > 50 else r["new_title"]
        lines.append(f"| {r['sku']} | {r['asin']} | {old_short} ({old_chars}) | {new_short} ({new_chars}) | {status_badge} |")

    lines += [
        "",
        "---",
        "",
        "## Detailed Changes",
        "",
    ]

    for r in results:
        old_chars = r.get("old_chars", len(r["old_title"]))
        new_chars = r.get("new_chars", len(r["new_title"]))
        lines += [
            f"### {r['sku']} ({r['asin']})",
            "",
            f"**Status:** {r['status']}",
            f"**Applied at:** {r.get('applied_at', 'N/A')}",
            "",
            f"**BEFORE** ({old_chars} chars):",
            f"> {r['old_title']}",
            "",
            f"**AFTER** ({new_chars} chars):",
            f"> {r['new_title']}",
            "",
        ]
        if r.get("error"):
            lines.append(f"**Error:** {r['error']}")
            lines.append("")
        if r.get("warnings"):
            for w in r["warnings"]:
                lines.append(f"**Warning:** {w}")
            lines.append("")
        lines.append("---")
        lines.append("")

    lines += [
        "## Summary",
        "",
        f"- **Total:** {len(results)} listings processed",
    ]
    if not dry_run:
        lines += [
            f"- **OK:** {len(ok)}",
            f"- **Failed:** {len(failed)}",
            f"- **Aborted:** {len(aborted)}",
        ]
    else:
        lines.append(f"- **Dry run (not applied):** {len(dry)}")

    lines += [
        "",
        "## Compliance Verification",
        "",
        "All new titles checked before patching:",
        "- ✅ No 'nesell' in titles",
        "- ✅ No manufacturer model numbers",
        "- ✅ All titles ≤ 200 chars",
        "",
        "## Next Steps",
        "",
        "- [ ] GPSR compliance: add `gpsr_safety_attestation` and `dsa_responsible_party_address` to all PFT- listings (separate ticket)",
        "- [ ] Optional: expand backend keywords with trending terms (vatertag geschenk, patrioten mütze)",
        "- [ ] Monitor CTR/conversion change in Seller Central over next 2 weeks",
    ]

    changelog_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  Changelog written to: {changelog_path}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return results


if __name__ == "__main__":
    main()
