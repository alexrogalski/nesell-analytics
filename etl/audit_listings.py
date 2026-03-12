"""Printful-Baselinker-Amazon listing audit.

Cross-references all three platforms to find missing variants, SKU mismatches,
pricing inconsistencies, and Amazon listing issues (GPSR, suppression, images).

Usage:
    cd ~/nesell-analytics
    python3.11 -m etl.audit_listings              # full audit
    python3.11 -m etl.audit_listings --fast        # skip child SKU check on Amazon
    python3.11 -m etl.audit_listings --json        # output JSON
    python3.11 -m etl.audit_listings --family nf_fr  # only 1 family
    python3.11 -m etl.audit_listings --skip-amazon # skip Amazon (only PF + BL)
"""
import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Any

from . import config
from .amazon_api import api_get, headers
from .amazon_listings import (
    MARKETPLACE_IDS,
    DAD_HAT_COLORS,
    TRUCKER_COLORS_DE,
    NF_CHILDREN,
    FL_CHILDREN,
    TRUCKER_CHILDREN,
    COUNTRY_DATA,
    SELLER_ID,
    get_listing_full,
)
from .sku_mapping import (
    get_sku_mapping,
    _pf_get,
)


# ── Product Families ─────────────────────────────────────────────────

def build_product_families() -> list[dict[str, Any]]:
    """Build the list of product families from COUNTRY_DATA.

    Each family represents a parent SKU + its expected child variants
    on a specific country. Only includes families where parent_sku is
    not None.
    """
    families = []
    for country, data in COUNTRY_DATA.items():
        # NF (no-flag dad hat)
        if data.get("nf_parent"):
            families.append({
                "family_id": f"nf_{country.lower().replace(' ', '_')}",
                "parent_sku": data["nf_parent"],
                "hat_type": "dad_hat",
                "variant_type": "nf",
                "expected_variants": list(NF_CHILDREN),
                "country": country,
            })
        # FL (flag dad hat)
        if data.get("fl_parent"):
            families.append({
                "family_id": f"fl_{country.lower().replace(' ', '_')}",
                "parent_sku": data["fl_parent"],
                "hat_type": "dad_hat",
                "variant_type": "fl",
                "expected_variants": list(FL_CHILDREN),
                "country": country,
            })
        # MEGA (trucker hat)
        if data.get("mega_parent"):
            families.append({
                "family_id": f"mega_{country.lower().replace(' ', '_')}",
                "parent_sku": data["mega_parent"],
                "hat_type": "trucker",
                "variant_type": "mega",
                "expected_variants": list(TRUCKER_CHILDREN),
                "country": country,
            })
    return families


PRODUCT_FAMILIES = build_product_families()


# ── Severity ──────────────────────────────────────────────────────────

CRITICAL = "CRITICAL"
WARNING = "WARNING"
INFO = "INFO"


# ── Data Fetchers ─────────────────────────────────────────────────────

def fetch_printful_sync_products() -> dict[int, dict]:
    """Fetch all Printful sync products and their variants.

    Returns dict keyed by sync product id:
        {
            sync_product_id: {
                "external_id": "...",
                "name": "...",
                "synced": int,
                "variants": {variant_id_str: variant_data, ...},
            }
        }
    """
    token = config.PRINTFUL_V1_TOKEN
    products = {}

    # Printful paginate: default 20 per page, up to 100
    offset = 0
    limit = 100
    while True:
        data = _pf_get(token, "/store/products", {"offset": offset, "limit": limit})
        if not data or "result" not in data:
            break
        result_list = data["result"]
        if not result_list:
            break

        for item in result_list:
            pid = item.get("id")
            products[pid] = {
                "external_id": str(item.get("external_id", "")),
                "name": item.get("name", ""),
                "synced": item.get("synced", 0),
                "variants": {},
            }

        offset += limit
        total = data.get("paging", {}).get("total", 0)
        if offset >= total:
            break
        time.sleep(0.3)

    print(f"  [Printful] Fetched {len(products)} sync products")

    # Fetch variant details for each product
    fetched = 0
    for pid in list(products.keys()):
        detail = _pf_get(token, f"/store/products/{pid}")
        if not detail or "result" not in detail:
            continue
        sync_variants = detail["result"].get("sync_variants", [])
        for sv in sync_variants:
            vid = str(sv.get("variant_id", ""))
            products[pid]["variants"][vid] = {
                "variant_id": vid,
                "name": sv.get("name", ""),
                "synced": sv.get("synced", False),
                "is_ignored": sv.get("is_ignored", False),
                "product": sv.get("product", {}),
                "sku": sv.get("sku", ""),
                "external_id": str(sv.get("external_id", "")),
            }
        fetched += 1
        if fetched % 20 == 0:
            print(f"  [Printful] Fetched details for {fetched}/{len(products)} products...")
        time.sleep(0.3)

    print(f"  [Printful] Fetched variant details for {fetched} products")
    return products


def build_printful_variant_set(
    pf_products: dict[int, dict],
) -> dict[str, set[str]]:
    """Collect all synced Printful catalog variant_ids, categorized by hat type.

    Printful sync products don't use our PFT-{template}-{variant} SKU format.
    Instead, we iterate all sync products, collect their catalog variant_ids
    (the dict keys in pdata["variants"]), and categorize each as "dad_hat"
    or "trucker" based on whether the variant_id appears in DAD_HAT_COLORS
    or TRUCKER_COLORS_DE.

    Returns:
        {"dad_hat": {set of variant_id strings}, "trucker": {set of variant_id strings}}
    """
    dad_hat_ids = set(DAD_HAT_COLORS.keys())
    trucker_ids = set(TRUCKER_COLORS_DE.keys())

    result: dict[str, set[str]] = {"dad_hat": set(), "trucker": set()}

    for pid, pdata in pf_products.items():
        variants = pdata.get("variants", {})
        for vid in variants:
            if vid in dad_hat_ids:
                result["dad_hat"].add(vid)
            elif vid in trucker_ids:
                result["trucker"].add(vid)

    return result


def fetch_baselinker_mapping() -> dict[str, dict[str, Any]]:
    """Fetch Baselinker inventory mapping (PFT-* SKUs)."""
    print("  [Baselinker] Fetching SKU mapping...")
    return get_sku_mapping(config.BASELINKER_TOKEN)


def check_amazon_listing(
    sku: str, marketplace_id: str,
) -> dict[str, Any]:
    """Check a single Amazon listing. Returns parsed result dict."""
    data = get_listing_full(sku, marketplace_id)
    if not data:
        return {"exists": False, "sku": sku, "marketplace_id": marketplace_id}

    # Check if listing actually exists (vs error response)
    summaries = data.get("summaries", [])
    exists = len(summaries) > 0

    # Parse issues
    issues = data.get("issues", [])
    error_issues = [i for i in issues if i.get("severity") == "ERROR"]
    warning_issues = [i for i in issues if i.get("severity") == "WARNING"]

    # Count images from attributes
    attrs = data.get("attributes", {})
    image_count = 0
    if "main_product_image_locator" in attrs:
        image_count += 1
    for key in attrs:
        if key.startswith("other_product_image_locator"):
            vals = attrs[key]
            if isinstance(vals, list):
                image_count += len(vals)

    # Check offers
    offers = data.get("offers", [])

    return {
        "exists": exists,
        "sku": sku,
        "marketplace_id": marketplace_id,
        "issues": issues,
        "error_issues": error_issues,
        "warning_issues": warning_issues,
        "image_count": image_count,
        "offers": offers,
        "asin": summaries[0].get("asin", "") if summaries else "",
        "status": summaries[0].get("status", []) if summaries else [],
    }


# ── Checks ────────────────────────────────────────────────────────────

def check_printful_sync(
    family: dict, pf_variant_set: dict[str, set[str]],
) -> list[dict]:
    """Check 1: PRINTFUL_SYNC - all expected variants synced in Printful.

    Uses the global variant set (keyed by hat_type) instead of per-template
    lookup. All dad_hat families share the same global set of synced variant_ids,
    and all trucker families share theirs.
    """
    issues = []
    hat_type = family["hat_type"]
    synced = pf_variant_set.get(hat_type, set())
    expected = set(family["expected_variants"])
    missing = expected - synced

    if missing:
        issues.append({
            "check": "PRINTFUL_SYNC",
            "severity": CRITICAL,
            "family": family["family_id"],
            "message": f"Missing {len(missing)}/{len(expected)} variants in Printful sync: {sorted(missing)}",
            "synced": len(synced & expected),
            "expected": len(expected),
            "missing_variants": sorted(missing),
        })
    return issues


def check_printful_availability(
    family: dict, pf_products: dict[int, dict],
) -> list[dict]:
    """Check 2: PRINTFUL_AVAIL - synced variants availability status.

    Checks all Printful sync products for variants matching this family's
    expected_variants. If any synced variant has is_ignored=True, flag it.
    Uses variant_id (catalog ID) matching instead of template_id lookup.
    """
    issues = []
    hat_type = family["hat_type"]
    expected_set = set(family["expected_variants"])

    # Determine which catalog variant IDs belong to this hat type
    if hat_type == "dad_hat":
        relevant_ids = set(DAD_HAT_COLORS.keys())
    else:
        relevant_ids = set(TRUCKER_COLORS_DE.keys())

    ignored_variants = []

    for pid, pdata in pf_products.items():
        for vid, vdata in pdata.get("variants", {}).items():
            # Only check variants that belong to this hat type AND are expected
            if vid in relevant_ids and vid in expected_set and vdata.get("is_ignored"):
                ignored_variants.append(vid)

    if ignored_variants:
        issues.append({
            "check": "PRINTFUL_AVAIL",
            "severity": CRITICAL,
            "family": family["family_id"],
            "message": f"{len(ignored_variants)} variants marked as ignored in Printful: {sorted(ignored_variants)}",
            "ignored_variants": sorted(ignored_variants),
        })
    return issues


def check_bl_variants(
    family: dict, bl_mapping: dict[str, dict[str, Any]],
) -> list[dict]:
    """Check 3: BL_VARIANTS - all expected children exist in Baselinker."""
    issues = []
    parent_sku = family["parent_sku"]
    expected = family["expected_variants"]
    missing = []

    for vid in expected:
        child_sku = f"{parent_sku}-{vid}"
        if child_sku not in bl_mapping:
            missing.append(vid)

    if missing:
        issues.append({
            "check": "BL_VARIANTS",
            "severity": CRITICAL,
            "family": family["family_id"],
            "message": f"Missing {len(missing)}/{len(expected)} children in Baselinker: {sorted(missing)}",
            "expected": len(expected),
            "found": len(expected) - len(missing),
            "missing_variants": sorted(missing),
        })
    return issues


def check_bl_duplicates(
    bl_mapping: dict[str, dict[str, Any]],
) -> list[dict]:
    """Check 4: BL_DUPLICATES - duplicate SKUs in Baselinker inventory."""
    issues = []
    # bl_mapping is keyed by SKU so there can't be exact duplicates in the dict
    # But we check for duplicate product_ids pointing to the same variant
    seen_variants: dict[str, list[str]] = defaultdict(list)
    for sku, info in bl_mapping.items():
        if info["variant_id"] is not None:
            key = f"{info['template_id']}-{info['variant_id']}"
            seen_variants[key].append(sku)

    for key, skus in seen_variants.items():
        if len(skus) > 1:
            issues.append({
                "check": "BL_DUPLICATES",
                "severity": CRITICAL,
                "family": "global",
                "message": f"Duplicate variant {key} found under SKUs: {skus}",
                "duplicate_skus": skus,
            })
    return issues


def check_bl_pricing(
    family: dict, bl_mapping: dict[str, dict[str, Any]],
) -> list[dict]:
    """Check 5: BL_PRICING - all children in family have the same price."""
    issues = []
    parent_sku = family["parent_sku"]
    prices: dict[float, list[str]] = defaultdict(list)

    for vid in family["expected_variants"]:
        child_sku = f"{parent_sku}-{vid}"
        info = bl_mapping.get(child_sku)
        if info:
            prices[info["price"]].append(vid)

    if len(prices) > 1:
        price_str = ", ".join(
            f"{p:.2f} ({len(vids)} variants)" for p, vids in sorted(prices.items())
        )
        issues.append({
            "check": "BL_PRICING",
            "severity": WARNING,
            "family": family["family_id"],
            "message": f"Inconsistent pricing: {price_str}",
            "price_groups": {str(p): vids for p, vids in prices.items()},
        })
    return issues


def check_sku_alignment(
    family: dict,
    bl_mapping: dict[str, dict[str, Any]],
    amz_results: dict[str, dict[str, dict]],
) -> list[dict]:
    """Check 6: SKU_ALIGNMENT - BL children have Amazon listings on >= 1 marketplace."""
    issues = []
    parent_sku = family["parent_sku"]
    orphaned = []

    for vid in family["expected_variants"]:
        child_sku = f"{parent_sku}-{vid}"
        if child_sku not in bl_mapping:
            continue

        # Check if this child has any Amazon listing
        found_any = False
        child_results = amz_results.get(child_sku, {})
        for mkt_code, result in child_results.items():
            if result.get("exists"):
                found_any = True
                break

        if not found_any and child_results:
            orphaned.append(vid)

    if orphaned:
        issues.append({
            "check": "SKU_ALIGNMENT",
            "severity": WARNING,
            "family": family["family_id"],
            "message": f"{len(orphaned)} BL children have no Amazon listing: {sorted(orphaned)}",
            "orphaned_variants": sorted(orphaned),
        })
    return issues


def check_amz_coverage(
    family: dict, amz_results: dict[str, dict[str, dict]],
) -> list[dict]:
    """Check 7: AMZ_COVERAGE - parent SKU exists on all 8 EU marketplaces."""
    issues = []
    parent_sku = family["parent_sku"]
    parent_results = amz_results.get(parent_sku, {})

    missing_markets = []
    present_markets = []
    for mkt_code in MARKETPLACE_IDS:
        result = parent_results.get(mkt_code, {})
        if result.get("exists"):
            present_markets.append(mkt_code)
        else:
            missing_markets.append(mkt_code)

    if missing_markets:
        issues.append({
            "check": "AMZ_COVERAGE",
            "severity": CRITICAL,
            "family": family["family_id"],
            "message": f"Parent {parent_sku} missing on {len(missing_markets)} marketplaces: {missing_markets}",
            "present": present_markets,
            "missing": missing_markets,
        })
    return issues


def check_amz_issues(
    family: dict, amz_results: dict[str, dict[str, dict]],
) -> list[dict]:
    """Check 8: AMZ_ISSUES - Amazon listing issues (GPSR, suppression, etc.)."""
    issues = []
    parent_sku = family["parent_sku"]

    # Check parent
    for mkt_code, result in amz_results.get(parent_sku, {}).items():
        error_issues = result.get("error_issues", [])
        if error_issues:
            for ei in error_issues:
                issues.append({
                    "check": "AMZ_ISSUES",
                    "severity": CRITICAL,
                    "family": family["family_id"],
                    "message": f"[{mkt_code}] Parent {parent_sku}: {ei.get('message', ei.get('code', 'unknown'))}",
                    "sku": parent_sku,
                    "marketplace": mkt_code,
                    "issue_code": ei.get("code", ""),
                    "issue_message": ei.get("message", ""),
                })

    # Check children
    for vid in family["expected_variants"]:
        child_sku = f"{parent_sku}-{vid}"
        for mkt_code, result in amz_results.get(child_sku, {}).items():
            error_issues = result.get("error_issues", [])
            if error_issues:
                for ei in error_issues:
                    issues.append({
                        "check": "AMZ_ISSUES",
                        "severity": CRITICAL,
                        "family": family["family_id"],
                        "message": f"[{mkt_code}] Child {child_sku}: {ei.get('message', ei.get('code', 'unknown'))}",
                        "sku": child_sku,
                        "marketplace": mkt_code,
                        "issue_code": ei.get("code", ""),
                        "issue_message": ei.get("message", ""),
                    })
    return issues


def check_amz_images(
    family: dict, amz_results: dict[str, dict[str, dict]],
) -> list[dict]:
    """Check 9: AMZ_IMAGES - listings should have >= 8 images."""
    issues = []
    parent_sku = family["parent_sku"]
    min_images = 8

    for vid in family["expected_variants"]:
        child_sku = f"{parent_sku}-{vid}"
        for mkt_code, result in amz_results.get(child_sku, {}).items():
            if not result.get("exists"):
                continue
            img_count = result.get("image_count", 0)
            if img_count < min_images:
                issues.append({
                    "check": "AMZ_IMAGES",
                    "severity": WARNING,
                    "family": family["family_id"],
                    "message": f"[{mkt_code}] {child_sku}: only {img_count}/{min_images} images",
                    "sku": child_sku,
                    "marketplace": mkt_code,
                    "image_count": img_count,
                })
    return issues


# ── Amazon Data Fetcher ───────────────────────────────────────────────

def fetch_amazon_data(
    families: list[dict],
    fast: bool = False,
) -> dict[str, dict[str, dict]]:
    """Fetch Amazon listing data for all families.

    Returns nested dict: {sku: {marketplace_code: result_dict}}
    """
    results: dict[str, dict[str, dict]] = defaultdict(dict)
    total_calls = 0

    for family in families:
        parent_sku = family["parent_sku"]
        print(f"  [Amazon] Checking {family['family_id']} ({parent_sku})...")

        # Check parent on all marketplaces
        for mkt_code, mkt_id in MARKETPLACE_IDS.items():
            result = check_amazon_listing(parent_sku, mkt_id)
            results[parent_sku][mkt_code] = result
            total_calls += 1
            time.sleep(0.5)

        # Check children (unless --fast)
        if not fast:
            for vid in family["expected_variants"]:
                child_sku = f"{parent_sku}-{vid}"
                for mkt_code, mkt_id in MARKETPLACE_IDS.items():
                    result = check_amazon_listing(child_sku, mkt_id)
                    results[child_sku][mkt_code] = result
                    total_calls += 1
                    time.sleep(0.5)

                if total_calls % 50 == 0:
                    print(f"  [Amazon] {total_calls} API calls made...")

    print(f"  [Amazon] Total API calls: {total_calls}")
    return results


# ── Report Formatting ─────────────────────────────────────────────────

def format_variant_label(vid: str, hat_type: str) -> str:
    """Get a human-readable label for a variant ID."""
    if hat_type == "trucker":
        return TRUCKER_COLORS_DE.get(vid, vid)
    return DAD_HAT_COLORS.get(vid, vid)


def format_coverage_matrix(
    parent_sku: str,
    amz_results: dict[str, dict[str, dict]],
) -> str:
    """Format marketplace coverage as a compact matrix line."""
    parent_data = amz_results.get(parent_sku, {})
    parts = []
    for mkt_code in MARKETPLACE_IDS:
        result = parent_data.get(mkt_code, {})
        if result.get("exists"):
            error_count = len(result.get("error_issues", []))
            if error_count > 0:
                parts.append(f"{mkt_code}:ERR({error_count})")
            else:
                parts.append(f"{mkt_code}:ok")
        else:
            parts.append(f"{mkt_code}:---")
    return "  ".join(parts)


def print_report(
    families: list[dict],
    all_issues: list[dict],
    pf_variant_set: dict[str, set[str]],
    bl_mapping: dict[str, dict[str, Any]],
    amz_results: dict[str, dict[str, dict]],
    skip_amazon: bool = False,
) -> None:
    """Print the full tabular report to stdout."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep = "=" * 80

    print(f"\n{sep}")
    print(f"  LISTING AUDIT REPORT")
    print(f"  Generated: {now}")
    print(f"  Families:  {len(families)}")
    print(sep)

    for family in families:
        fid = family["family_id"]
        parent_sku = family["parent_sku"]
        hat_type = family["hat_type"]
        country = family["country"]
        expected_count = len(family["expected_variants"])

        print(f"\n{'─' * 80}")
        print(f"  Family: {fid}  |  Country: {country}  |  Type: {hat_type}")
        print(f"  Parent SKU: {parent_sku}")
        print(f"{'─' * 80}")

        # Printful sync status (global variant set per hat type)
        synced = pf_variant_set.get(hat_type, set())
        synced_count = len(synced & set(family["expected_variants"]))
        pf_status = f"{synced_count}/{expected_count}"
        pf_indicator = "OK" if synced_count == expected_count else "MISSING"
        print(f"  Printful:    {pf_status} variants synced [{pf_indicator}]")

        # Baselinker status
        bl_found = sum(
            1 for vid in family["expected_variants"]
            if f"{parent_sku}-{vid}" in bl_mapping
        )
        bl_status = f"{bl_found}/{expected_count}"
        bl_indicator = "OK" if bl_found == expected_count else "MISSING"
        print(f"  Baselinker:  {bl_status} children [{bl_indicator}]")

        # Baselinker price range
        prices = set()
        for vid in family["expected_variants"]:
            child_sku = f"{parent_sku}-{vid}"
            info = bl_mapping.get(child_sku)
            if info:
                prices.add(info["price"])
        if prices:
            if len(prices) == 1:
                print(f"  BL Price:    {next(iter(prices)):.2f}")
            else:
                print(f"  BL Price:    MIXED ({', '.join(f'{p:.2f}' for p in sorted(prices))})")

        # Amazon coverage matrix
        if not skip_amazon:
            matrix = format_coverage_matrix(parent_sku, amz_results)
            print(f"  Amazon:      {matrix}")

        # Family-specific issues
        family_issues = [i for i in all_issues if i.get("family") == fid]
        if family_issues:
            crit = [i for i in family_issues if i["severity"] == CRITICAL]
            warn = [i for i in family_issues if i["severity"] == WARNING]
            info = [i for i in family_issues if i["severity"] == INFO]
            counts = []
            if crit:
                counts.append(f"{len(crit)} CRITICAL")
            if warn:
                counts.append(f"{len(warn)} WARNING")
            if info:
                counts.append(f"{len(info)} INFO")
            print(f"  Issues:      {', '.join(counts)}")

    # Global issues section
    global_issues = [i for i in all_issues if i.get("family") == "global"]

    # Issues summary grouped by severity
    print(f"\n{sep}")
    print("  ISSUES DETAIL")
    print(sep)

    critical_issues = [i for i in all_issues if i["severity"] == CRITICAL]
    warning_issues = [i for i in all_issues if i["severity"] == WARNING]
    info_issues = [i for i in all_issues if i["severity"] == INFO]

    if critical_issues:
        print(f"\n  CRITICAL ({len(critical_issues)}):")
        for i, issue in enumerate(critical_issues, 1):
            check = issue["check"]
            family = issue.get("family", "")
            msg = issue["message"]
            print(f"    {i:3d}. [{check}] {family}: {msg}")

    if warning_issues:
        print(f"\n  WARNING ({len(warning_issues)}):")
        for i, issue in enumerate(warning_issues, 1):
            check = issue["check"]
            family = issue.get("family", "")
            msg = issue["message"]
            print(f"    {i:3d}. [{check}] {family}: {msg}")

    if info_issues:
        print(f"\n  INFO ({len(info_issues)}):")
        for i, issue in enumerate(info_issues, 1):
            check = issue["check"]
            family = issue.get("family", "")
            msg = issue["message"]
            print(f"    {i:3d}. [{check}] {family}: {msg}")

    if not all_issues:
        print("\n  No issues found. All checks passed.")

    # Summary
    print(f"\n{sep}")
    print(f"  SUMMARY: {len(critical_issues)} CRITICAL / {len(warning_issues)} WARNING / {len(info_issues)} INFO")
    print(sep)
    print()


def build_json_report(
    families: list[dict],
    all_issues: list[dict],
    pf_variant_set: dict[str, set[str]],
    bl_mapping: dict[str, dict[str, Any]],
    amz_results: dict[str, dict[str, dict]],
    skip_amazon: bool = False,
) -> dict:
    """Build JSON report structure."""
    now = datetime.now().isoformat()

    family_reports = []
    for family in families:
        fid = family["family_id"]
        parent_sku = family["parent_sku"]
        expected_count = len(family["expected_variants"])

        # Printful (global variant set per hat type)
        hat_type = family["hat_type"]
        synced = pf_variant_set.get(hat_type, set())
        pf_synced = len(synced & set(family["expected_variants"]))

        # Baselinker
        bl_found = sum(
            1 for vid in family["expected_variants"]
            if f"{parent_sku}-{vid}" in bl_mapping
        )

        # Prices
        prices = {}
        for vid in family["expected_variants"]:
            child_sku = f"{parent_sku}-{vid}"
            info = bl_mapping.get(child_sku)
            if info:
                prices[vid] = info["price"]

        # Amazon
        amz_coverage = {}
        if not skip_amazon:
            parent_data = amz_results.get(parent_sku, {})
            for mkt_code in MARKETPLACE_IDS:
                result = parent_data.get(mkt_code, {})
                amz_coverage[mkt_code] = {
                    "exists": result.get("exists", False),
                    "asin": result.get("asin", ""),
                    "error_count": len(result.get("error_issues", [])),
                }

        family_issues = [i for i in all_issues if i.get("family") == fid]

        family_reports.append({
            "family_id": fid,
            "parent_sku": parent_sku,
            "hat_type": family["hat_type"],
            "country": family["country"],
            "expected_variants": expected_count,
            "printful_synced": pf_synced,
            "baselinker_found": bl_found,
            "baselinker_prices": prices,
            "amazon_coverage": amz_coverage,
            "issues": family_issues,
        })

    critical = [i for i in all_issues if i["severity"] == CRITICAL]
    warnings = [i for i in all_issues if i["severity"] == WARNING]
    infos = [i for i in all_issues if i["severity"] == INFO]

    return {
        "generated_at": now,
        "total_families": len(families),
        "summary": {
            "critical": len(critical),
            "warning": len(warnings),
            "info": len(infos),
        },
        "families": family_reports,
        "issues": all_issues,
    }


# ── Main ──────────────────────────────────────────────────────────────

def run_audit(
    fast: bool = False,
    output_json: bool = False,
    family_filter: str | None = None,
    skip_amazon: bool = False,
) -> dict | None:
    """Run the full listing audit pipeline."""
    print("\n" + "=" * 80)
    print("  LISTING AUDIT: Printful + Baselinker + Amazon")
    print("=" * 80)

    # Filter families if requested
    families = PRODUCT_FAMILIES
    if family_filter:
        families = [f for f in families if f["family_id"] == family_filter]
        if not families:
            print(f"\n  ERROR: Family '{family_filter}' not found.")
            print(f"  Available families: {', '.join(f['family_id'] for f in PRODUCT_FAMILIES)}")
            sys.exit(1)

    print(f"\n  Auditing {len(families)} product families")
    if fast:
        print("  Mode: FAST (parent-only Amazon checks)")
    if skip_amazon:
        print("  Mode: SKIP-AMAZON (Printful + Baselinker only)")
    print()

    # ── Phase 1: Fetch data ──────────────────────────────────────────
    print("Phase 1: Fetching data from all platforms...")
    print("-" * 40)

    # Printful
    print("\n[1/3] Printful sync products...")
    pf_products = fetch_printful_sync_products()
    pf_variant_set = build_printful_variant_set(pf_products)
    print(f"  Synced variants: dad_hat={len(pf_variant_set.get('dad_hat', set()))}, trucker={len(pf_variant_set.get('trucker', set()))}")

    # Baselinker
    print("\n[2/3] Baselinker inventory...")
    bl_mapping = fetch_baselinker_mapping()

    # Amazon
    amz_results: dict[str, dict[str, dict]] = {}
    if not skip_amazon:
        print("\n[3/3] Amazon listings...")
        amz_results = fetch_amazon_data(families, fast=fast)
    else:
        print("\n[3/3] Amazon skipped")

    # ── Phase 2: Run checks ──────────────────────────────────────────
    print(f"\n{'─' * 40}")
    print("Phase 2: Running checks...")
    print("-" * 40)

    all_issues: list[dict] = []

    # Check 4 is global (not per-family)
    print("  Running BL_DUPLICATES check...")
    all_issues.extend(check_bl_duplicates(bl_mapping))

    for family in families:
        fid = family["family_id"]
        print(f"  Checking {fid}...")

        # Check 1: PRINTFUL_SYNC
        all_issues.extend(check_printful_sync(family, pf_variant_set))

        # Check 2: PRINTFUL_AVAIL
        all_issues.extend(check_printful_availability(family, pf_products))

        # Check 3: BL_VARIANTS
        all_issues.extend(check_bl_variants(family, bl_mapping))

        # Check 5: BL_PRICING
        all_issues.extend(check_bl_pricing(family, bl_mapping))

        if not skip_amazon:
            # Check 6: SKU_ALIGNMENT
            all_issues.extend(check_sku_alignment(family, bl_mapping, amz_results))

            # Check 7: AMZ_COVERAGE
            all_issues.extend(check_amz_coverage(family, amz_results))

            # Check 8: AMZ_ISSUES
            all_issues.extend(check_amz_issues(family, amz_results))

            # Check 9: AMZ_IMAGES (only when not fast, since we need child data)
            if not fast:
                all_issues.extend(check_amz_images(family, amz_results))

    # ── Phase 3: Report ──────────────────────────────────────────────
    if output_json:
        report = build_json_report(
            families, all_issues, pf_variant_set, bl_mapping, amz_results, skip_amazon,
        )
        print(json.dumps(report, indent=2, default=str))
        return report
    else:
        print_report(
            families, all_issues, pf_variant_set, bl_mapping, amz_results, skip_amazon,
        )
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Audit Printful-Baselinker-Amazon listing coverage",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Skip child SKU checks on Amazon (parent-only)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output report as JSON",
    )
    parser.add_argument(
        "--family",
        type=str,
        default=None,
        help="Audit only one product family (e.g. nf_fr, fl_italy, mega_belgium)",
    )
    parser.add_argument(
        "--skip-amazon",
        action="store_true",
        help="Skip Amazon checks (only Printful + Baselinker)",
    )
    args = parser.parse_args()

    run_audit(
        fast=args.fast,
        output_json=args.output_json,
        family_filter=args.family,
        skip_amazon=args.skip_amazon,
    )


if __name__ == "__main__":
    main()
