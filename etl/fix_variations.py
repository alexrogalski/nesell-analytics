"""Fix variation listings: set style_name + normalize color_name on MEGA parents.

Root cause: task_create_parents() and child builders never set style_name when
linking children to MEGA parents. Amazon "guesses" style from title, creating
3-4 groups instead of 2. Also, old manual SKUs have English colors while PFT-
children have German colors, causing duplicate color entries.

Usage:
    python3.11 -m etl.fix_variations --dry-run   # preview changes
    python3.11 -m etl.fix_variations              # apply fixes
    python3.11 -m etl.fix_variations --parent MEGA-GERMANY-OLD  # fix one parent only
"""

import argparse
import json
import time
import sys
from datetime import datetime
from . import config
from .amazon_api import headers, _refresh_token, api_get

import requests

SELLER_ID = "A1IZH6PW7A624A"

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

# All MEGA parents and their primary marketplace
MEGA_PARENTS = {
    "MEGA-GERMANY-OLD":  "DE",
    "PFT-MEGA-FR":       "DE",
    "PFT-MEGA-IT":       "DE",
    "PFT-MEGA-PL":       "DE",
    "PFT-MEGA-BE2":      "DE",
    "PFT-MEGA-NL2":      "DE",
    "PFT-MEGA-GB":       "DE",
}

# FL parent SKU prefixes per MEGA parent (children of these = "mit Flagge")
FL_PARENT_PREFIXES = {
    "MEGA-GERMANY-OLD":  ["PFT-100032925"],
    "PFT-MEGA-FR":       ["PFT-93854576"],
    "PFT-MEGA-IT":       ["PFT-93854948"],
    "PFT-MEGA-PL":       ["PFT-93855132"],
    "PFT-MEGA-BE2":      ["PFT-93855556"],
    "PFT-MEGA-NL2":      ["PFT-93855915"],
    "PFT-MEGA-GB":       ["PFT-93856110"],
}

# NF parent SKU prefixes per MEGA parent (children of these = "ohne Flagge")
NF_PARENT_PREFIXES = {
    "PFT-MEGA-FR":       ["PFT-93856317"],
    "PFT-MEGA-IT":       ["PFT-93856295"],
    "PFT-MEGA-PL":       ["PFT-93856269"],
    "PFT-MEGA-BE2":      ["PFT-93856238"],
    "PFT-MEGA-NL2":      ["PFT-93856209"],
    "PFT-MEGA-GB":       ["PFT-93856169"],
}

# For Europe mega-parent (if it exists)
# PFT-88471944-* = FL, FBA-6843674_* = NF, PFT-82980216-* = NF

# English -> German color normalization (for old manual SKUs on DE marketplace)
COLOR_NORMALIZE_DE = {
    "White":      "Weiss",
    "Black":      "Schwarz",
    "Khaki":      "Beige",
    "Light Blue": "Hellblau",
    "Navy":       "Marineblau",
    "Pink":       "Rosa",
    "Stone":      "Steingrau",
    "Spruce":     "Tannengruen",
    "Green Camo": "Gruenes Tarnmuster",
    "Cranberry":  "Cranberry",
    "Dark Grey":  "Dunkelgrau",
}

# German style names
STYLE_MIT_FLAGGE = "mit Flagge"
STYLE_OHNE_FLAGGE = "ohne Flagge"


def api_patch(path, body, params=None, retries=8):
    """PATCH with retry for Listings API."""
    url = f"https://sellingpartnerapi-eu.amazon.com{path}"
    for attempt in range(retries):
        try:
            resp = requests.patch(url, headers=headers(), json=body, params=params, timeout=30)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"    [ConnErr] retry in {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [429] rate limited, waiting {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            _refresh_token()
            time.sleep(3)
            continue
        if resp.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [{resp.status_code}] server error, retry in {wait}s")
            time.sleep(wait)
            continue
        return resp.status_code, resp.json()
    print(f"    [WARN] All {retries} attempts failed for PATCH {path}")
    return 0, {}


def get_listing(sku, marketplace_id):
    """Get full listing with relationships."""
    return api_get(
        f'/listings/2021-08-01/items/{SELLER_ID}/{sku}',
        params={
            'marketplaceIds': marketplace_id,
            'includedData': 'summaries,attributes,relationships'
        }
    )


def get_children_skus(parent_sku, marketplace_id):
    """Get all child SKUs linked to a parent."""
    data = get_listing(parent_sku, marketplace_id)
    children = []
    relationships = data.get("relationships", [])
    for rel in relationships:
        for child in rel.get("childSkus", []):
            children.append(child)
    # Also check attributes-based relationships
    attrs = data.get("attributes", {})
    if not children and "child_parent_sku_relationship" in attrs:
        # Parent doesn't have childSkus directly; we need to scan differently
        pass
    return children, data


def determine_style(child_sku, mega_parent):
    """Determine correct style_name based on SKU pattern."""
    # FL parent prefixes for this mega parent
    fl_prefixes = FL_PARENT_PREFIXES.get(mega_parent, [])
    for prefix in fl_prefixes:
        if child_sku.startswith(prefix):
            return STYLE_MIT_FLAGGE

    # NF parent prefixes for this mega parent
    nf_prefixes = NF_PARENT_PREFIXES.get(mega_parent, [])
    for prefix in nf_prefixes:
        if child_sku.startswith(prefix):
            return STYLE_OHNE_FLAGGE

    # Special case: MEGA-GERMANY-OLD
    # PFT-100032925-* = FL, everything else (old manual SKUs) = NF
    if mega_parent == "MEGA-GERMANY-OLD":
        if child_sku.startswith("PFT-100032925"):
            return STYLE_MIT_FLAGGE
        return STYLE_OHNE_FLAGGE

    # Special case: Europe mega-parent (if it exists)
    if "EUROPE" in mega_parent.upper():
        if child_sku.startswith("PFT-88471944"):
            return STYLE_MIT_FLAGGE
        # FBA-6843674_* and PFT-82980216-* are NF
        return STYLE_OHNE_FLAGGE

    # Default: if SKU contains known FL parent prefix from any country
    return STYLE_OHNE_FLAGGE


def normalize_color(current_color, marketplace_code):
    """Normalize color name to marketplace language."""
    if marketplace_code == "DE":
        return COLOR_NORMALIZE_DE.get(current_color, current_color)
    # Other marketplaces: no normalization needed (already correct)
    return current_color


def audit_parent(parent_sku, mkt_code, dry_run=True):
    """Audit and optionally fix all children of a MEGA parent."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    print(f"\n{'='*70}")
    print(f"  MEGA Parent: {parent_sku} on {mkt_code} ({mkt_id})")
    print(f"{'='*70}")

    # Get parent listing
    parent_data = get_listing(parent_sku, mkt_id)
    if "errors" in parent_data or not parent_data:
        print(f"  [ERROR] Parent {parent_sku} not found on {mkt_code}")
        return 0, 0, 0

    # Get variation theme
    attrs = parent_data.get("attributes", {})
    vt = attrs.get("variation_theme", [{}])
    vt_name = vt[0].get("name", "N/A") if vt else "N/A"
    print(f"  Variation theme: {vt_name}")

    # Find children via relationships
    relationships = parent_data.get("relationships", [])
    child_skus = []
    for rel in relationships:
        for child in rel.get("childSkus", []):
            child_skus.append(child)

    if not child_skus:
        print(f"  [WARN] No children found via relationships API")
        print(f"  Trying to discover children by known SKU patterns...")
        child_skus = discover_children_by_pattern(parent_sku, mkt_id)

    print(f"  Found {len(child_skus)} children")

    fixed = 0
    skipped = 0
    errors = 0

    for child_sku in child_skus:
        time.sleep(0.3)
        child_data = get_listing(child_sku, mkt_id)
        if "errors" in child_data or not child_data:
            print(f"    [SKIP] {child_sku} - not found")
            skipped += 1
            continue

        child_attrs = child_data.get("attributes", {})

        # Current values
        current_style_list = child_attrs.get("style_name", [])
        current_style = current_style_list[0].get("value", "N/A") if current_style_list else "N/A"

        current_color_list = child_attrs.get("color", [])
        current_color = current_color_list[0].get("value", "N/A") if current_color_list else "N/A"

        # Determine correct values
        correct_style = determine_style(child_sku, parent_sku)
        correct_color = normalize_color(current_color, mkt_code)

        style_needs_fix = current_style != correct_style
        color_needs_fix = current_color != correct_color

        if not style_needs_fix and not color_needs_fix:
            print(f"    [OK] {child_sku}: style={current_style}, color={current_color}")
            skipped += 1
            continue

        changes = []
        if style_needs_fix:
            changes.append(f"style: {current_style} -> {correct_style}")
        if color_needs_fix:
            changes.append(f"color: {current_color} -> {correct_color}")

        print(f"    [FIX] {child_sku}: {', '.join(changes)}")

        if dry_run:
            fixed += 1
            continue

        # Build PATCH
        patches = []
        if style_needs_fix:
            patches.append({
                "op": "replace",
                "path": "/attributes/style_name",
                "value": [{"value": correct_style, "language_tag": "de_DE", "marketplace_id": mkt_id}]
            })
        if color_needs_fix:
            patches.append({
                "op": "replace",
                "path": "/attributes/color",
                "value": [{"value": correct_color, "language_tag": "de_DE", "marketplace_id": mkt_id}]
            })

        path = f"/listings/2021-08-01/items/{SELLER_ID}/{child_sku}"
        body = {
            "productType": "HAT",
            "patches": patches,
        }

        status, resp = api_patch(path, body, params={"marketplaceIds": mkt_id})
        status_str = resp.get("status", "UNKNOWN") if isinstance(resp, dict) else "UNKNOWN"
        if status in (200, 202):
            print(f"      [{status}] PATCH -> {status_str}")
            fixed += 1
        else:
            print(f"      [{status}] PATCH FAILED: {json.dumps(resp, indent=2)[:200]}")
            errors += 1

        time.sleep(0.5)

    return fixed, skipped, errors


def discover_children_by_pattern(parent_sku, mkt_id):
    """Try to discover children by known SKU patterns when relationships API is empty."""
    from .amazon_listings import NF_CHILDREN, FL_CHILDREN

    suffixes = NF_CHILDREN  # same as FL_CHILDREN

    candidates = []

    if parent_sku == "MEGA-GERMANY-OLD":
        # NF children: old manual SKUs - we don't know the pattern, try common ones
        # FL children: PFT-100032925-{suffix}
        for s in suffixes:
            candidates.append(f"PFT-100032925-{s}")
        # For NF, we need to try fetching known manual SKUs
        # These were the original Germany NF children before mega-parent
        nf_parent = "PFT-90034427"
        for s in suffixes:
            candidates.append(f"{nf_parent}-{s}")

    elif parent_sku in FL_PARENT_PREFIXES:
        fl_prefixes = FL_PARENT_PREFIXES[parent_sku]
        nf_prefixes = NF_PARENT_PREFIXES.get(parent_sku, [])
        for prefix in fl_prefixes + nf_prefixes:
            for s in suffixes:
                candidates.append(f"{prefix}-{s}")

    # Verify which candidates actually exist
    existing = []
    for sku in candidates:
        data = api_get(
            f'/listings/2021-08-01/items/{SELLER_ID}/{sku}',
            params={'marketplaceIds': mkt_id, 'includedData': 'summaries'}
        )
        if data and 'errors' not in data:
            existing.append(sku)
        time.sleep(0.3)

    return existing


def main():
    parser = argparse.ArgumentParser(
        description="Fix style_name and color_name on MEGA parent variation children"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without applying them")
    parser.add_argument("--parent", type=str, default=None,
                        help="Fix only this specific parent SKU (e.g. MEGA-GERMANY-OLD)")
    args = parser.parse_args()

    print(f"\n{'#'*70}")
    print(f"# Fix Variations - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# {'DRY RUN' if args.dry_run else 'LIVE MODE'}")
    print(f"{'#'*70}")

    if args.parent:
        parents = {args.parent: MEGA_PARENTS.get(args.parent, "DE")}
        if args.parent not in MEGA_PARENTS:
            print(f"\n[WARN] {args.parent} not in known MEGA_PARENTS dict, using DE marketplace")
    else:
        parents = MEGA_PARENTS

    total_fixed = 0
    total_skipped = 0
    total_errors = 0

    for parent_sku, mkt_code in parents.items():
        fixed, skipped, errors = audit_parent(parent_sku, mkt_code, dry_run=args.dry_run)
        total_fixed += fixed
        total_skipped += skipped
        total_errors += errors

    print(f"\n{'#'*70}")
    print(f"# SUMMARY")
    print(f"#   {'Would fix' if args.dry_run else 'Fixed'}: {total_fixed}")
    print(f"#   Skipped (already OK): {total_skipped}")
    print(f"#   Errors: {total_errors}")
    print(f"{'#'*70}")

    if args.dry_run and total_fixed > 0:
        print(f"\nRun without --dry-run to apply {total_fixed} fixes.")


if __name__ == "__main__":
    main()
