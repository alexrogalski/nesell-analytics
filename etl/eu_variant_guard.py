#!/usr/bin/env python3
"""
EU Variant Guard - prevents non-EU-fulfillable Printful variants from being sold on Amazon EU.

Runs daily. Checks Printful availability API for variants not fulfillable in EU,
then scans Amazon listings and zeros stock for any matches.

Usage:
    python3 -m etl.eu_variant_guard [--dry-run]
    python3 -m etl.eu_variant_guard --dry-run --verbose
"""

import argparse
import json
import re
import requests
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from . import config
from . import amazon_api

# ── Config ──────────────────────────────────────────────────────────────────

# Printful catalog products to monitor (hat products)
MONITORED_PRODUCTS = {
    206: {"name": "Classic Dad Hat (Yupoong 6245CM)", "technique": "EMBROIDERY"},
    252: {"name": "Trucker Cap (Yupoong 6606)", "technique": "EMBROIDERY"},
    253: {"name": "Bucket Hat (Yupoong 5003)", "technique": "EMBROIDERY"},
}

# Hardcoded known non-EU variants as fallback (variant_id -> reason)
HARDCODED_BLOCKED = {
    8745: "Spruce (US/CA only)",
}

# Variant ID -> color name (for human-readable logs)
VARIANT_COLORS = {
    # Dad Hat 206
    7853: "White", 7854: "Black", 7855: "Khaki", 7856: "Light Blue",
    7857: "Navy", 7858: "Pink", 7859: "Stone", 8745: "Spruce",
    9794: "Green Camo", 12735: "Cranberry", 12736: "Dark Grey",
    # Trucker Cap 252 (actual variant IDs from Printful API)
    8746: "White", 8747: "Black", 8748: "Charcoal/Black", 8749: "Dark Navy",
    8750: "Heather/Black", 8751: "Khaki", 8752: "Grey/White",
    8753: "Red/White", 8754: "Navy/White", 8755: "Royal/White", 8756: "Red",
    16709: "Heather Grey/White", 16710: "Brown/Khaki",
    20390: "Coyote Brown", 20391: "Dark Grey", 20392: "Loden/Black",
    20393: "Olive/Black", 20394: "Rust/Black", 20395: "Steel Blue/Black",
    22454: "Spruce",
    # Bucket Hat 253 (actual variant IDs from Printful API)
    8759: "Black", 8760: "Navy", 8761: "White",
    8762: "Charcoal Grey", 8763: "Khaki",
}

SELLER_ID = config.AMZ_SELLER_ID

EU_MARKETPLACES = {
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS",
    "NL": "A1805IZSGTT6HS",
    "SE": "A2NODRKZP88ZB9",
    "PL": "A1C3SOZRARQ6R3",
    "BE": "AMEN7PMS3EDWL",
}

STATE_FILE = Path(__file__).parent.parent / "data" / "eu_variant_guard_state.json"

# Printful credentials
_pf = config._load_env_file(config.KEYS_DIR / "printful.env")
PF_TOKEN = _pf.get("PRINTFUL_API_TOKEN_V2", _pf.get("PRINTFUL_API_TOKEN", ""))
PF_STORE_ID = _pf.get("PRINTFUL_STORE_ID", "15269225")

# Module-level flags set by CLI
_verbose = False


def log(msg, verbose_only=False):
    """Print log message. If verbose_only, only print when --verbose is set."""
    if verbose_only and not _verbose:
        return
    print(msg)


# ── Step 1: Printful Availability ──────────────────────────────────────────

def pf_get(path, params=None):
    """GET Printful v2 API with retry."""
    headers = {
        "Authorization": f"Bearer {PF_TOKEN}",
        "X-PF-Store-Id": PF_STORE_ID,
    }
    for attempt in range(3):
        try:
            r = requests.get(
                f"https://api.printful.com{path}",
                headers=headers,
                params=params or {},
                timeout=30,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            log(f"  [{type(e).__name__}] retry in 10s ({attempt+1}/3)")
            time.sleep(10)
            continue
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            log(f"  [Rate limit] Waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code == 200:
            return r.json()
        log(f"  [Printful] GET {path} -> {r.status_code}: {r.text[:200]}")
        return None
    return None


def get_non_eu_variants():
    """Check Printful availability for all monitored products.

    Returns tuple: (blocked_dict, known_variant_ids_set)
      blocked_dict: {variant_id: {"color": str, "product": str, "reason": str}}
      known_variant_ids_set: set of ALL variant IDs that exist in Printful catalog
    """
    blocked = {}
    known_variant_ids = set()

    for product_id, info in MONITORED_PRODUCTS.items():
        technique = info["technique"]
        log(f"\n  Checking {info['name']} (product {product_id}, {technique})...")

        data = pf_get(
            f"/v2/catalog-products/{product_id}/availability",
            {"techniques": technique},
        )
        if not data or "data" not in data:
            log(f"    WARNING: Could not fetch availability for product {product_id}")
            continue

        product_total = 0
        product_blocked = 0

        for item in data["data"]:
            vid = item["catalog_variant_id"]
            known_variant_ids.add(vid)
            eu_status = "unknown"

            for tech in item.get("techniques", []):
                for region in tech.get("selling_regions", []):
                    if region["name"] == "europe":
                        eu_status = region["availability"]
                        break

            product_total += 1
            color = VARIANT_COLORS.get(vid, f"variant_{vid}")

            if eu_status != "in stock":
                product_blocked += 1
                blocked[vid] = {
                    "color": color,
                    "product": info["name"],
                    "product_id": product_id,
                    "reason": f"europe={eu_status}",
                }
                log(f"    BLOCKED: {color} ({vid}) - {eu_status}")
            else:
                log(f"    OK: {color} ({vid})", verbose_only=True)

        log(f"    Product {product_id}: {product_total} variants, {product_blocked} blocked")

    # Add hardcoded fallbacks that might not appear in API
    for vid, reason in HARDCODED_BLOCKED.items():
        if vid not in blocked:
            color = VARIANT_COLORS.get(vid, f"variant_{vid}")
            blocked[vid] = {
                "color": color,
                "product": "Hardcoded fallback",
                "product_id": 0,
                "reason": reason,
            }
            log(f"    HARDCODED: {color} ({vid}) - {reason}")

    log(f"\n  Known valid variant IDs in Printful catalog: {len(known_variant_ids)}")
    return blocked, known_variant_ids


def check_variant_exists_in_printful(variant_id):
    """Check if a specific variant ID exists in Printful catalog.
    Returns True if it exists, False if 404/discontinued."""
    headers = {
        "Authorization": f"Bearer {PF_TOKEN}",
        "X-PF-Store-Id": PF_STORE_ID,
    }
    try:
        r = requests.get(
            f"https://api.printful.com/v2/catalog-variants/{variant_id}",
            headers=headers,
            timeout=15,
        )
        return r.status_code == 200
    except Exception:
        return True  # assume exists on network error (safe default)


def find_discontinued_variants(listings, known_variant_ids):
    """Scan Amazon listings for variant IDs not in Printful catalog.

    Returns dict of newly discovered blocked variants:
      {variant_id: {"color": str, "product": str, "reason": str}}
    """
    unknown_vids = {}  # vid -> list of SKUs

    for row in listings:
        sku = row.get("seller-sku", row.get("sku", "")).strip()
        if not sku:
            continue
        vid = extract_variant_id_from_sku(sku)
        if vid is None:
            continue
        if vid in known_variant_ids:
            continue
        # This variant ID is on Amazon but NOT in any monitored Printful product
        if vid not in unknown_vids:
            unknown_vids[vid] = []
        unknown_vids[vid].append(sku)

    if not unknown_vids:
        log("  No unknown variant IDs found on Amazon.")
        return {}

    log(f"  Found {len(unknown_vids)} variant IDs on Amazon not in Printful catalog.")
    log("  Verifying each against Printful API...")

    discontinued = {}
    for vid, skus in sorted(unknown_vids.items()):
        time.sleep(0.3)  # rate limit
        exists = check_variant_exists_in_printful(vid)
        color = VARIANT_COLORS.get(vid, f"variant_{vid}")
        if not exists:
            discontinued[vid] = {
                "color": color,
                "product": "DISCONTINUED (removed from Printful catalog)",
                "product_id": 0,
                "reason": "variant removed from Printful catalog (404)",
            }
            log(f"    DISCONTINUED: variant {vid} ({color}) - 404 from Printful, {len(skus)} Amazon SKU(s): {', '.join(skus[:3])}")
        else:
            log(f"    EXISTS (not monitored): variant {vid} ({color}) - exists in Printful but not in monitored products", verbose_only=True)

    return discontinued


# ── Step 2: Amazon Listings ────────────────────────────────────────────────

def extract_variant_id_from_sku(sku):
    """Extract Printful variant_id from various SKU patterns.

    Patterns:
      PFT-{template_id}-{variant_id}               -> variant_id
      PFT-MEGA-{country}-NF-{variant_id}            -> variant_id
      PFT-MEGA-{country}-FL-{variant_id}            -> variant_id
      {old_sku}_{variant_id}  (e.g. 6843674_8745)   -> variant_id
      67856BE7EEE4B_{color}                          -> needs color map (skip)

    Returns int variant_id or None.
    """
    if not sku:
        return None

    # Pattern 1: PFT-{template}-{variant_id}  (most common)
    m = re.match(r'^PFT-(\d+)-(\d+)$', sku)
    if m:
        return int(m.group(2))

    # Pattern 2: PFT-MEGA-{country}-{NF|FL}-{variant_id}
    m = re.match(r'^PFT-MEGA-[A-Z]+-(?:NF|FL)-(\d+)$', sku)
    if m:
        return int(m.group(1))

    # Pattern 3: {digits}_{variant_id}  (legacy SKUs like 6843674_8745)
    m = re.match(r'^[\dA-Fa-f]+_(\d{4,5})$', sku)
    if m:
        vid = int(m.group(1))
        # Sanity check: variant IDs for hats are in 7000-25000 range
        if 7000 <= vid <= 25000:
            return vid

    # Pattern 4: {letters}-{letters}-{letters} with trailing _{variant_id}
    # e.g. SU-HHEA-WWB5_8745 or similar legacy formats
    m = re.search(r'_(\d{4,5})$', sku)
    if m:
        vid = int(m.group(1))
        if 7000 <= vid <= 25000:
            return vid

    return None


def _download_report_safe(document_id):
    """Download report with fallback encoding (handles Latin-1 product names)."""
    import gzip
    import csv
    import io

    data = amazon_api.api_get(f"/reports/2021-06-30/documents/{document_id}")
    url = data.get("url")
    if not url:
        return []

    compression = data.get("compressionAlgorithm")
    resp = requests.get(url, timeout=120)
    content = resp.content
    if compression == "GZIP":
        content = gzip.decompress(content)

    # Try utf-8-sig first, fall back to latin-1 (Amazon uses it for EU product names)
    for encoding in ("utf-8-sig", "latin-1", "cp1252"):
        try:
            text = content.decode(encoding)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        text = content.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader)


def get_amazon_listings():
    """Fetch all active Amazon listings via GET_MERCHANT_LISTINGS_ALL_DATA report.

    Returns list of dicts with keys: seller-sku, asin1, quantity, status, etc.
    """
    log("\n  Requesting GET_MERCHANT_LISTINGS_ALL_DATA report...")

    report_id = amazon_api.create_report(
        "GET_MERCHANT_LISTINGS_ALL_DATA",
        marketplace_ids=list(EU_MARKETPLACES.values()),
    )
    if not report_id:
        log("  WARNING: Failed to create listings report")
        return []

    log(f"  Report created: {report_id}, polling...")
    doc_id = amazon_api.poll_report(report_id)
    if not doc_id:
        log("  WARNING: Report polling failed or timed out")
        return []

    rows = _download_report_safe(doc_id)
    log(f"  Got {len(rows)} listing rows from Amazon")
    return rows


def find_active_blocked_listings(listings, blocked_variants):
    """Cross-reference Amazon listings with blocked variants.

    Returns list of dicts:
      {"sku": str, "variant_id": int, "color": str, "quantity": int,
       "asin": str, "status": str, "item_name": str}
    """
    results = []
    checked = 0
    matched = 0

    for row in listings:
        sku = row.get("seller-sku", row.get("sku", "")).strip()
        if not sku:
            continue

        vid = extract_variant_id_from_sku(sku)
        if vid is None:
            continue

        checked += 1

        if vid in blocked_variants:
            matched += 1
            # Try to extract quantity from listing report
            qty_str = row.get("quantity", row.get("afn-listing-exists", "0"))
            try:
                qty = int(qty_str)
            except (ValueError, TypeError):
                qty = 0

            status = row.get("status", row.get("listing-id", "unknown"))
            asin = row.get("asin1", row.get("asin", ""))
            name = row.get("item-name", row.get("product-name", ""))

            results.append({
                "sku": sku,
                "variant_id": vid,
                "color": blocked_variants[vid]["color"],
                "product": blocked_variants[vid]["product"],
                "reason": blocked_variants[vid]["reason"],
                "quantity": qty,
                "asin": asin,
                "status": status,
                "item_name": name[:60] if name else "",
            })

    log(f"  Checked {checked} listings with variant IDs, found {matched} matching blocked variants")
    return results


# ── Step 3: Zero Stock ─────────────────────────────────────────────────────

def zero_stock_on_amazon(sku, dry_run=True):
    """Set stock to 0 for a SKU across all 8 EU marketplaces.

    Returns dict: {"market": "OK"|"FAIL(status_code)"}
    """
    results = {}

    if dry_run:
        for market_code in EU_MARKETPLACES:
            results[market_code] = "DRY_RUN"
        return results

    access_token = amazon_api.headers()["x-amz-access-token"]

    for market_code, market_id in EU_MARKETPLACES.items():
        url = (f"{config.AMZ_API_BASE}/listings/2021-08-01/items"
               f"/{SELLER_ID}/{requests.utils.quote(sku, safe='')}")
        try:
            r = requests.patch(
                url,
                params={"marketplaceIds": market_id},
                headers={
                    "x-amz-access-token": access_token,
                    "Content-Type": "application/json",
                },
                json={
                    "productType": "HAT",
                    "patches": [{
                        "op": "replace",
                        "path": "/attributes/fulfillment_availability",
                        "value": [{
                            "fulfillment_channel_code": "DEFAULT",
                            "quantity": 0,
                            "marketplace_id": market_id,
                        }],
                    }],
                },
                timeout=30,
            )
            if r.status_code in (200, 202):
                results[market_code] = "OK"
            else:
                results[market_code] = f"FAIL({r.status_code})"
                log(f"    [WARN] {market_code} patch failed: {r.status_code} {r.text[:150]}", verbose_only=True)
        except Exception as e:
            results[market_code] = f"ERR({type(e).__name__})"

        time.sleep(0.5)  # rate limit between marketplace calls

    return results


# ── Step 4: Summary & State ────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_run": None, "blocked_variants": {}, "actions": []}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── Main Guard Logic ───────────────────────────────────────────────────────

def run_guard(dry_run=True, verbose=False):
    """Main entry point. Can be called from etl/run.py or standalone."""
    global _verbose
    _verbose = verbose

    log("=" * 60)
    log(f"EU Variant Guard {'(DRY RUN)' if dry_run else '(ENFORCE)'}")
    log(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)

    # Step 1: Get non-EU variants from Printful
    log("\n[1/5] Checking Printful EU availability...")
    blocked_variants, known_variant_ids = get_non_eu_variants()
    log(f"\n  Blocked variants (not EU-fulfillable): {len(blocked_variants)}")

    # Step 2: Get Amazon listings
    log("\n[2/5] Fetching Amazon EU listings...")
    listings = get_amazon_listings()

    if not listings:
        log("  WARNING: No listings to check. Skipping.")
        log("=" * 60)
        return {"checked": 0, "blocked": len(blocked_variants), "already_zeroed": 0, "newly_zeroed": 0}

    # Step 2b: Detect discontinued variants (on Amazon but removed from Printful)
    log("\n[3/5] Scanning for discontinued/removed Printful variants on Amazon...")
    discontinued = find_discontinued_variants(listings, known_variant_ids)
    if discontinued:
        log(f"  Found {len(discontinued)} DISCONTINUED variants still on Amazon!")
        blocked_variants.update(discontinued)
        log(f"  Total blocked (availability + discontinued): {len(blocked_variants)}")

    if not blocked_variants:
        log("\n  All variants are EU-fulfillable and current. Nothing to do.")
        log("=" * 60)
        return {"checked": len(listings), "blocked": 0, "already_zeroed": 0, "newly_zeroed": 0}

    # Step 4: Cross-reference and fix
    log("\n[4/5] Cross-referencing blocked variants with active listings...")
    affected = find_active_blocked_listings(listings, blocked_variants)

    already_zeroed = 0
    newly_zeroed = 0
    zero_failed = 0

    for item in affected:
        sku = item["sku"]
        vid = item["variant_id"]
        color = item["color"]
        qty = item["quantity"]

        if qty == 0:
            already_zeroed += 1
            log(f"  OK (already zeroed): {sku} (variant {vid} = {color})", verbose_only=True)
            continue

        # Need to zero this listing
        log(f"\n  BLOCKING: {sku} (variant {vid} = {color}, qty={qty})")
        log(f"    Reason: {item['reason']}")
        log(f"    ASIN: {item['asin']} | {item['item_name']}")

        market_results = zero_stock_on_amazon(sku, dry_run=dry_run)

        ok_markets = [m for m, r in market_results.items() if r in ("OK", "DRY_RUN")]
        fail_markets = [m for m, r in market_results.items() if r not in ("OK", "DRY_RUN")]

        if dry_run:
            log(f"    [DRY RUN] Would zero stock on: {', '.join(ok_markets)}")
            newly_zeroed += 1
        else:
            if ok_markets:
                log(f"    Zeroed on: {', '.join(ok_markets)}")
                newly_zeroed += 1
            if fail_markets:
                log(f"    FAILED on: {', '.join(f'{m}={market_results[m]}' for m in fail_markets)}")
                zero_failed += 1

    # Step 5: Summary
    summary = {
        "checked": len(listings),
        "blocked": len(blocked_variants),
        "discontinued": len(discontinued),
        "affected_listings": len(affected),
        "already_zeroed": already_zeroed,
        "newly_zeroed": newly_zeroed,
        "zero_failed": zero_failed,
    }

    log(f"\n{'=' * 60}")
    log("[5/5] Summary:")
    log(f"  Listings checked:       {summary['checked']}")
    log(f"  Blocked variant IDs:    {summary['blocked']} (availability: {summary['blocked'] - summary['discontinued']}, discontinued: {summary['discontinued']})")
    log(f"  Affected Amazon SKUs:   {summary['affected_listings']}")
    log(f"  Already at zero stock:  {summary['already_zeroed']}")
    log(f"  Newly zeroed:           {summary['newly_zeroed']}{' (dry run)' if dry_run else ''}")
    if zero_failed:
        log(f"  Failed to zero:         {summary['zero_failed']}")
    log(f"{'=' * 60}")

    # Save state (even on dry run, for tracking)
    state = load_state()
    state["blocked_variants"] = {
        str(vid): info for vid, info in blocked_variants.items()
    }
    state["last_summary"] = summary
    state["last_dry_run"] = dry_run
    state["actions"] = state.get("actions", [])[-50:]  # keep last 50
    if newly_zeroed > 0:
        state["actions"].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dry_run": dry_run,
            "newly_zeroed": newly_zeroed,
            "skus": [item["sku"] for item in affected if item["quantity"] > 0],
        })
    save_state(state)

    return summary


# ── CLI Entry Point ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="EU Variant Guard - block non-EU Printful variants on Amazon"
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=True,
        help="Report only, don't change stock (default)",
    )
    parser.add_argument(
        "--enforce", action="store_true",
        help="Actually zero stock on Amazon for blocked variants",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Detailed output including OK variants",
    )
    args = parser.parse_args()

    dry_run = not args.enforce
    run_guard(dry_run=dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
