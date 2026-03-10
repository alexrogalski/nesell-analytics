#!/usr/bin/env python3.11
"""
Fix old Europe listing titles that incorrectly say "mit Flagge".

The Europe NO-FLAG template (PFT-88471944, BL product 6843674) should NOT have
"Flagge" or "flag" in titles. The Germany template (PFT-90034427) is the WITH flag one.

Strategy:
1. Use GET_MERCHANT_LISTINGS_ALL_DATA report to find all active Europe SKUs
2. For each SKU, check title on all 8 EU marketplaces
3. If title contains "Flagge"/"flag"/"mit Flagge"/"with Flag", fix it via PATCH
"""

import json
import time
import re
import requests
import gzip
import csv
import io
from pathlib import Path
from datetime import datetime
from urllib.parse import quote


# ─── Configuration ───────────────────────────────────────────────────────────

CREDENTIALS_PATH = Path.home() / ".keys" / "amazon-sp-api.json"
AMZ_CREDS = json.loads(CREDENTIALS_PATH.read_text())
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"
TOKEN_URL = "https://api.amazon.com/auth/o2/token"

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

# Known Europe-related SKU patterns (the old ones may have different format)
# PFT-88471944-* are the no-flag Europe template children (some deactivated)
# PFT-82980216-* are the EU flag template children (these SHOULD have "Flagge")
# We need to find ANY SKU that references "Europe" and is NOT PFT-82980216
EUROPE_FLAG_PARENT = "PFT-82980216"  # This one SHOULD have "mit Flagge" — DO NOT TOUCH
EUROPE_NOFLAG_PARENT = "PFT-88471944"  # This one should NOT have "mit Flagge"

# Flag patterns to detect in titles (case-insensitive)
FLAG_PATTERNS = [
    r'\bmit\s+Flagge\b',
    r'\bwith\s+Flag\b',
    r'\bavec\s+Drapeau\b',
    r'\bcon\s+Bandera\b',
    r'\bcon\s+Bandiera\b',
    r'\bmed\s+Flagga\b',
    r'\bmet\s+Vlag\b',
    r'\bz\s+Flag[aą]\b',
]

SLEEP_BETWEEN_CALLS = 0.5


# ─── Auth ────────────────────────────────────────────────────────────────────

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
    print(f"Got access token: {_token[:20]}...")
    return _token


def get_headers():
    global _token, _token_time
    if not _token or time.time() - _token_time > 3000:
        refresh_token()
    return {"x-amz-access-token": _token, "Content-Type": "application/json"}


# ─── API Helpers ─────────────────────────────────────────────────────────────

def api_get(path, params=None, retries=8):
    url = f"{AMZ_BASE}{path}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=get_headers(), params=params, timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
            time.sleep(10 * (attempt + 1))
            continue
        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [429] rate limited, waiting {wait}s ({attempt+1}/{retries})")
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


def api_post(path, body=None, retries=5):
    url = f"{AMZ_BASE}{path}"
    for attempt in range(retries):
        try:
            resp = requests.post(url, headers=get_headers(), json=body or {}, timeout=30)
        except requests.exceptions.ConnectionError:
            time.sleep(10 * (attempt + 1))
            continue
        if resp.status_code == 429:
            time.sleep(min(5 * (2 ** attempt), 60))
            continue
        if resp.status_code == 403:
            refresh_token()
            time.sleep(3)
            continue
        return resp.status_code, resp.json() if resp.text else {}
    return 0, {}


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
            print(f"    [429] rate limited, waiting {wait}s ({attempt+1}/{retries})")
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


# ─── Report Download ─────────────────────────────────────────────────────────

def create_report(report_type, marketplace_ids):
    body = {
        "reportType": report_type,
        "marketplaceIds": marketplace_ids,
    }
    status, data = api_post("/reports/2021-06-30/reports", body)
    report_id = data.get("reportId")
    if not report_id:
        print(f"    [WARN] Failed to create report {report_type}: {data}")
    return report_id


def poll_report(report_id, timeout_minutes=15):
    start = time.time()
    while time.time() - start < timeout_minutes * 60:
        _, data = api_get(f"/reports/2021-06-30/reports/{report_id}")
        status = data.get("processingStatus", "")
        if status == "DONE":
            return data.get("reportDocumentId")
        elif status in ("CANCELLED", "FATAL"):
            print(f"    [WARN] Report {report_id} status: {status}")
            return None
        time.sleep(10)
    print(f"    [WARN] Report {report_id} timed out")
    return None


def download_report(document_id):
    _, data = api_get(f"/reports/2021-06-30/documents/{document_id}")
    url = data.get("url")
    if not url:
        return ""
    compression = data.get("compressionAlgorithm")
    resp = requests.get(url, timeout=120)
    content = resp.content
    if compression == "GZIP":
        content = gzip.decompress(content)
    # Try utf-8-sig first, fall back to latin-1 for special chars
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def fetch_all_listings_report():
    """Fetch GET_MERCHANT_LISTINGS_ALL_DATA report for DE marketplace."""
    print("\n[Step 1] Fetching active listings report from Amazon...")
    report_id = create_report("GET_MERCHANT_LISTINGS_ALL_DATA", [MARKETPLACES["DE"]])
    if not report_id:
        return []
    print(f"  Report created: {report_id}, polling...")
    doc_id = poll_report(report_id)
    if not doc_id:
        return []
    text = download_report(doc_id)
    if not text:
        return []
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    rows = list(reader)
    print(f"  Got {len(rows)} listings from report")
    return rows


# ─── Listing Operations ─────────────────────────────────────────────────────

def get_listing(sku, mp_id):
    """Get listing with attributes and summaries."""
    encoded_sku = quote(sku, safe="")
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{encoded_sku}"
    params = {
        "marketplaceIds": mp_id,
        "issueLocale": "en_US",
        "includedData": "attributes,summaries",
    }
    return api_get(path, params)


def get_title_from_listing(listing_data):
    """Extract item_name from listing attributes."""
    attrs = listing_data.get("attributes", {})
    item_name_list = attrs.get("item_name", [])
    if item_name_list:
        return item_name_list[0].get("value", "")
    # Fallback to summaries
    summaries = listing_data.get("summaries", [])
    if summaries:
        return summaries[0].get("itemName", "")
    return ""


def has_flag_reference(title):
    """Check if a title contains any flag-related words."""
    for pattern in FLAG_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return True
    # Also check general terms
    if re.search(r'\bFlagge\b', title, re.IGNORECASE):
        return True
    if re.search(r'\bFlag\b', title, re.IGNORECASE):
        return True
    if re.search(r'\bDrapeau\b', title, re.IGNORECASE):
        return True
    if re.search(r'\bBandera\b', title, re.IGNORECASE):
        return True
    if re.search(r'\bBandiera\b', title, re.IGNORECASE):
        return True
    if re.search(r'\bFlagga\b', title, re.IGNORECASE):
        return True
    if re.search(r'\bVlag\b', title, re.IGNORECASE):
        return True
    return False


def remove_flag_from_title(title):
    """Remove flag references from a title while keeping everything else clean."""
    original = title

    # Remove common flag patterns (with surrounding spaces/dashes)
    # "mit Flagge" (German)
    title = re.sub(r'\s*[-–]\s*mit\s+Flagge\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s+mit\s+Flagge\b', '', title, flags=re.IGNORECASE)

    # "with Flag" (English)
    title = re.sub(r'\s*[-–]\s*with\s+Flag\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s+with\s+Flag\b', '', title, flags=re.IGNORECASE)

    # "avec Drapeau" (French)
    title = re.sub(r'\s*[-–]\s*avec\s+Drapeau\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s+avec\s+Drapeau\b', '', title, flags=re.IGNORECASE)

    # "con Bandera" (Spanish)
    title = re.sub(r'\s*[-–]\s*con\s+Bandera\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s+con\s+Bandera\b', '', title, flags=re.IGNORECASE)

    # "con Bandiera" (Italian)
    title = re.sub(r'\s*[-–]\s*con\s+Bandiera\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s+con\s+Bandiera\b', '', title, flags=re.IGNORECASE)

    # "med Flagga" (Swedish)
    title = re.sub(r'\s*[-–]\s*med\s+Flagga\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s+med\s+Flagga\b', '', title, flags=re.IGNORECASE)

    # "met Vlag" (Dutch)
    title = re.sub(r'\s*[-–]\s*met\s+Vlag\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s+met\s+Vlag\b', '', title, flags=re.IGNORECASE)

    # "z Flaga" / "z Flagą" (Polish)
    title = re.sub(r'\s*[-–]\s*z\s+Flag[aą]\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s+z\s+Flag[aą]\b', '', title, flags=re.IGNORECASE)

    # Also remove standalone "Flagge" if in keywords context
    title = re.sub(r'\s+Flagge\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\bFlagge\s+', '', title, flags=re.IGNORECASE)

    # Clean up double spaces, trailing/leading dashes
    title = re.sub(r'\s{2,}', ' ', title)
    title = re.sub(r'\s*-\s*-\s*', ' - ', title)
    title = title.strip(' -')

    return title


def patch_title(sku, mp_id, new_title, product_type="HAT"):
    """PATCH the item_name attribute."""
    encoded_sku = quote(sku, safe="")
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{encoded_sku}"
    body = {
        "productType": product_type,
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/item_name",
                "value": [{"value": new_title, "marketplace_id": mp_id}]
            }
        ]
    }
    params = {"marketplaceIds": mp_id, "issueLocale": "en_US"}
    return api_patch(path, body, params)


# ─── Keyword/generic_keyword fix ────────────────────────────────────────────

def remove_flag_from_keywords(keywords_str):
    """Remove flag-related words from generic_keyword string."""
    # Remove "flagge", "flag", "drapeau", "bandera", "bandiera", "flagga", "vlag", "flaga"
    result = re.sub(r'\b(?:flagge|flag|drapeau|bandera|bandiera|flagga|vlag|flag[aą])\b', '', keywords_str, flags=re.IGNORECASE)
    result = re.sub(r'\s{2,}', ' ', result).strip()
    return result


def patch_keywords(sku, mp_id, new_keywords, product_type="HAT"):
    """PATCH the generic_keyword attribute."""
    encoded_sku = quote(sku, safe="")
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{encoded_sku}"
    body = {
        "productType": product_type,
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/generic_keyword",
                "value": [{"value": new_keywords, "marketplace_id": mp_id}]
            }
        ]
    }
    params = {"marketplaceIds": mp_id, "issueLocale": "en_US"}
    return api_patch(path, body, params)


def patch_bullets(sku, mp_id, new_bullets, product_type="HAT"):
    """PATCH the bullet_point attribute."""
    encoded_sku = quote(sku, safe="")
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{encoded_sku}"
    body = {
        "productType": product_type,
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/bullet_point",
                "value": [{"value": b, "marketplace_id": mp_id} for b in new_bullets]
            }
        ]
    }
    params = {"marketplaceIds": mp_id, "issueLocale": "en_US"}
    return api_patch(path, body, params)


def patch_description(sku, mp_id, new_description, product_type="HAT"):
    """PATCH the product_description attribute."""
    encoded_sku = quote(sku, safe="")
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{encoded_sku}"
    body = {
        "productType": product_type,
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/product_description",
                "value": [{"value": new_description, "marketplace_id": mp_id}]
            }
        ]
    }
    params = {"marketplaceIds": mp_id, "issueLocale": "en_US"}
    return api_patch(path, body, params)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  FIX EUROPE NO-FLAG LISTING TITLES")
    print(f"  Remove 'mit Flagge' / 'with Flag' references")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    refresh_token()

    # ── Step 1: Fetch report to find all Europe-related SKUs ──
    rows = fetch_all_listings_report()
    if not rows:
        print("  ERROR: Could not fetch listings report. Aborting.")
        return

    # Print columns for debugging
    if rows:
        print(f"  Report columns: {list(rows[0].keys())}")

    # Find Europe-related listings
    # Look for titles containing "Europe" or "Europa" but NOT from PFT-82980216 (flag version)
    europe_skus = []
    all_europe_rows = []

    for row in rows:
        sku = row.get("seller-sku", "").strip()
        title = row.get("item-name", "").strip()
        asin = row.get("asin1", "").strip()
        status = row.get("status", "").strip()

        # Skip inactive
        if status and status.lower() not in ("active", ""):
            continue

        # Check if this is a Europe product
        is_europe = False
        if "europe" in title.lower() or "europa" in title.lower():
            is_europe = True
        if sku.startswith(EUROPE_NOFLAG_PARENT):
            is_europe = True

        if is_europe:
            all_europe_rows.append(row)
            # Skip the EU flag template (PFT-82980216) — it SHOULD have "mit Flagge"
            if sku.startswith(EUROPE_FLAG_PARENT):
                print(f"  [SKIP] {sku}: EU flag template (should have Flagge)")
                continue
            europe_skus.append((sku, title, asin))

    print(f"\n  Found {len(all_europe_rows)} total Europe listings in report")
    print(f"  Found {len(europe_skus)} Europe NO-FLAG SKUs to check")

    if not europe_skus:
        print("\n  No Europe no-flag SKUs found in report.")
        print("  Trying alternative: check known PFT-88471944 children...")

        # Fallback: check known children of PFT-88471944
        known_suffixes = ["7853", "7854", "7855", "7856", "7857", "7858", "7859",
                          "8745", "9794", "12735", "12736"]
        for suffix in known_suffixes:
            sku = f"{EUROPE_NOFLAG_PARENT}-{suffix}"
            status_code, data = get_listing(sku, MARKETPLACES["DE"])
            time.sleep(SLEEP_BETWEEN_CALLS)
            if status_code == 200:
                title = get_title_from_listing(data)
                summaries = data.get("summaries", [])
                asin = summaries[0].get("asin", "") if summaries else ""
                europe_skus.append((sku, title, asin))
                print(f"  Found active: {sku} -> {title[:60]}")
            else:
                print(f"  Not found: {sku} (HTTP {status_code})")

    # Also check the parent itself
    for parent_sku in [EUROPE_NOFLAG_PARENT]:
        status_code, data = get_listing(parent_sku, MARKETPLACES["DE"])
        time.sleep(SLEEP_BETWEEN_CALLS)
        if status_code == 200:
            title = get_title_from_listing(data)
            summaries = data.get("summaries", [])
            asin = summaries[0].get("asin", "") if summaries else ""
            if not any(s[0] == parent_sku for s in europe_skus):
                europe_skus.append((parent_sku, title, asin))
                print(f"  Found parent: {parent_sku} -> {title[:60]}")

    print(f"\n{'='*70}")
    print(f"  EUROPE NO-FLAG SKUs TO CHECK ({len(europe_skus)} total):")
    print(f"{'='*70}")
    for sku, title, asin in europe_skus:
        flag_marker = " [HAS FLAG!]" if has_flag_reference(title) else ""
        print(f"  {sku:<30} ASIN={asin} {title[:50]}{flag_marker}")

    # ── Step 2: Check each SKU on all 8 EU marketplaces ──
    print(f"\n{'='*70}")
    print(f"  CHECKING TITLES ON ALL 8 MARKETPLACES")
    print(f"{'='*70}")

    fixes_needed = []  # (sku, country, mp_id, old_title, new_title, product_type, keywords_fix, bullets_fix, desc_fix)

    for sku, report_title, asin in europe_skus:
        print(f"\n--- {sku} ---")

        for country, mp_id in MARKETPLACES.items():
            status_code, data = get_listing(sku, mp_id)
            time.sleep(SLEEP_BETWEEN_CALLS)

            if status_code != 200:
                continue

            title = get_title_from_listing(data)
            summaries = data.get("summaries", [])
            product_type = summaries[0].get("productType", "HAT") if summaries else "HAT"

            # Check title for flag references
            title_needs_fix = has_flag_reference(title)

            # Also check keywords, bullets, description
            attrs = data.get("attributes", {})

            keywords_str = ""
            keywords_list = attrs.get("generic_keyword", [])
            if keywords_list:
                keywords_str = keywords_list[0].get("value", "")
            keywords_needs_fix = has_flag_reference(keywords_str)

            bullets_list = attrs.get("bullet_point", [])
            bullets_values = [b.get("value", "") for b in bullets_list]
            bullets_need_fix = any(has_flag_reference(b) for b in bullets_values)

            desc_list = attrs.get("product_description", [])
            desc_str = desc_list[0].get("value", "") if desc_list else ""
            desc_needs_fix = has_flag_reference(desc_str)

            if title_needs_fix or keywords_needs_fix or bullets_need_fix or desc_needs_fix:
                new_title = remove_flag_from_title(title) if title_needs_fix else title
                new_keywords = remove_flag_from_keywords(keywords_str) if keywords_needs_fix else None
                new_bullets = [remove_flag_from_title(b) for b in bullets_values] if bullets_need_fix else None
                new_desc = remove_flag_from_title(desc_str) if desc_needs_fix else None

                issues = []
                if title_needs_fix:
                    issues.append("title")
                if keywords_needs_fix:
                    issues.append("keywords")
                if bullets_need_fix:
                    issues.append("bullets")
                if desc_needs_fix:
                    issues.append("description")

                print(f"  {country}: FIX NEEDED ({', '.join(issues)})")
                if title_needs_fix:
                    print(f"    OLD title: {title[:80]}")
                    print(f"    NEW title: {new_title[:80]}")

                fixes_needed.append({
                    "sku": sku,
                    "country": country,
                    "mp_id": mp_id,
                    "old_title": title,
                    "new_title": new_title,
                    "product_type": product_type,
                    "title_fix": title_needs_fix,
                    "keywords_fix": new_keywords,
                    "bullets_fix": new_bullets,
                    "desc_fix": new_desc,
                })
            else:
                print(f"  {country}: OK (no flag references)")

    # ── Step 3: Apply fixes ──
    print(f"\n{'='*70}")
    print(f"  APPLYING FIXES ({len(fixes_needed)} patches needed)")
    print(f"{'='*70}")

    success_count = 0
    error_count = 0
    results = []

    for fix in fixes_needed:
        sku = fix["sku"]
        country = fix["country"]
        mp_id = fix["mp_id"]
        product_type = fix["product_type"]

        label = f"{sku} @ {country}"

        # Fix title
        if fix["title_fix"]:
            status_code, resp = patch_title(sku, mp_id, fix["new_title"], product_type)
            time.sleep(SLEEP_BETWEEN_CALLS)
            resp_status = resp.get("status", "?") if isinstance(resp, dict) else "?"
            issues = resp.get("issues", []) if isinstance(resp, dict) else []
            errors = [i for i in issues if i.get("severity") == "ERROR"]

            if status_code == 200 and resp_status in ("ACCEPTED", "VALID"):
                print(f"  [OK] {label} title -> {resp_status}")
                success_count += 1
            else:
                err = errors[0].get("message", "")[:80] if errors else f"HTTP_{status_code} {resp_status}"
                print(f"  [FAIL] {label} title -> {err}")
                error_count += 1

            results.append({
                "sku": sku, "country": country, "field": "title",
                "old": fix["old_title"], "new": fix["new_title"],
                "status": resp_status, "http": status_code,
            })

        # Fix keywords
        if fix["keywords_fix"]:
            status_code, resp = patch_keywords(sku, mp_id, fix["keywords_fix"], product_type)
            time.sleep(SLEEP_BETWEEN_CALLS)
            resp_status = resp.get("status", "?") if isinstance(resp, dict) else "?"
            if status_code == 200 and resp_status in ("ACCEPTED", "VALID"):
                print(f"  [OK] {label} keywords -> {resp_status}")
                success_count += 1
            else:
                print(f"  [FAIL] {label} keywords -> HTTP {status_code} {resp_status}")
                error_count += 1

            results.append({
                "sku": sku, "country": country, "field": "keywords",
                "status": resp_status, "http": status_code,
            })

        # Fix bullets
        if fix["bullets_fix"]:
            status_code, resp = patch_bullets(sku, mp_id, fix["bullets_fix"], product_type)
            time.sleep(SLEEP_BETWEEN_CALLS)
            resp_status = resp.get("status", "?") if isinstance(resp, dict) else "?"
            if status_code == 200 and resp_status in ("ACCEPTED", "VALID"):
                print(f"  [OK] {label} bullets -> {resp_status}")
                success_count += 1
            else:
                print(f"  [FAIL] {label} bullets -> HTTP {status_code} {resp_status}")
                error_count += 1

            results.append({
                "sku": sku, "country": country, "field": "bullets",
                "status": resp_status, "http": status_code,
            })

        # Fix description
        if fix["desc_fix"]:
            status_code, resp = patch_description(sku, mp_id, fix["desc_fix"], product_type)
            time.sleep(SLEEP_BETWEEN_CALLS)
            resp_status = resp.get("status", "?") if isinstance(resp, dict) else "?"
            if status_code == 200 and resp_status in ("ACCEPTED", "VALID"):
                print(f"  [OK] {label} description -> {resp_status}")
                success_count += 1
            else:
                print(f"  [FAIL] {label} description -> HTTP {status_code} {resp_status}")
                error_count += 1

            results.append({
                "sku": sku, "country": country, "field": "description",
                "status": resp_status, "http": status_code,
            })

    # ── Summary ──
    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"  Europe no-flag SKUs checked: {len(europe_skus)}")
    print(f"  Fixes needed: {len(fixes_needed)}")
    print(f"  Successful patches: {success_count}")
    print(f"  Failed patches: {error_count}")

    if fixes_needed:
        print(f"\n  Fixes applied:")
        for fix in fixes_needed:
            issues = []
            if fix["title_fix"]:
                issues.append("title")
            if fix["keywords_fix"]:
                issues.append("keywords")
            if fix["bullets_fix"]:
                issues.append("bullets")
            if fix["desc_fix"]:
                issues.append("description")
            print(f"    {fix['sku']} @ {fix['country']}: {', '.join(issues)}")
            if fix["title_fix"]:
                print(f"      OLD: {fix['old_title'][:80]}")
                print(f"      NEW: {fix['new_title'][:80]}")

    # Save results
    results_path = Path(__file__).parent / "fix_europe_titles_results.json"
    output = {
        "timestamp": datetime.now().isoformat(),
        "skus_checked": [(s, t[:60], a) for s, t, a in europe_skus],
        "fixes_applied": fixes_needed,
        "patch_results": results,
        "summary": {
            "skus_checked": len(europe_skus),
            "fixes_needed": len(fixes_needed),
            "success": success_count,
            "errors": error_count,
        }
    }
    with open(results_path, "w") as f:
        json.dump(output, f, indent=2, default=str, ensure_ascii=False)
    print(f"\n  Results saved to: {results_path}")

    print(f"\n  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
