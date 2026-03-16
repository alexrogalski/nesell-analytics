"""Fast Amazon Listing Audit — EU 8 marketplaces.

Fetches all seller listings via SP-API Listings Items API (paginated),
checks titles, bullet points, images, suppression, GPSR, then writes
a summary JSON to /tmp/amazon-audit-<date>.json.

Read-only: no writes to Amazon. Safe to run overnight.

Usage:
    cd ~/nesell-analytics
    python3.11 scripts/amazon_listing_audit.py
    python3.11 scripts/amazon_listing_audit.py --marketplace DE
    python3.11 scripts/amazon_listing_audit.py --output /tmp/my-audit.json
"""
import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import date

# Add parent dir to path so we can import etl modules
sys.path.insert(0, "/Users/alexanderrogalski/nesell-analytics")

from etl.amazon_api import api_get, headers
from etl import config

# ── Constants ─────────────────────────────────────────────────────────

SELLER_ID = config.AMZ_SELLER_ID or "A1IZH6PW7A624A"

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

MIN_TITLE_LEN = 80
MIN_BULLETS = 5
MIN_IMAGES = 3
REQUIRED_GPSR_ATTRS = ["gpsr_safety_attestation", "dsa_responsible_party_address"]

# ── Listing Fetcher ───────────────────────────────────────────────────

def fetch_all_listings(marketplace_id: str) -> list[dict]:
    """Fetch all listings for a marketplace using pagination."""
    listings = []
    page_token = None
    page = 0

    while True:
        params = {
            "marketplaceIds": marketplace_id,
            "includedData": "summaries,attributes,issues",
            "pageSize": 20,
        }
        if page_token:
            params["pageToken"] = page_token

        data = api_get(
            f"/listings/2021-08-01/items/{SELLER_ID}",
            params=params,
        )

        items = data.get("items", [])
        listings.extend(items)
        page += 1

        next_token = data.get("pagination", {}).get("nextToken")
        if not next_token:
            break

        page_token = next_token
        time.sleep(0.5)  # respect rate limits

    return listings


# ── Issue Checkers ────────────────────────────────────────────────────

def check_listing(item: dict, marketplace: str) -> list[dict]:
    """Return list of issues for a single listing item."""
    issues = []
    sku = item.get("sku", "unknown")
    attrs = item.get("attributes", {})
    summaries = item.get("summaries", [{}])
    summary = summaries[0] if summaries else {}
    api_issues = item.get("issues", [])

    status = summary.get("status", [])
    asin = summary.get("asin", "")
    condition = summary.get("conditionType", "")

    def add(severity, issue_type, detail):
        issues.append({
            "sku": sku,
            "asin": asin,
            "marketplace": marketplace,
            "severity": severity,
            "issue_type": issue_type,
            "detail": detail,
        })

    # 1. Suppressed listing
    if "SUPPRESSED" in status:
        add("CRITICAL", "suppressed_listing", f"Listing is SUPPRESSED (status={status})")

    # 2. Inactive listing
    if "INCOMPLETE" in status:
        add("WARNING", "incomplete_listing", f"Listing is INCOMPLETE (status={status})")

    # 3. Title length
    title_list = attrs.get("item_name", [])
    title = title_list[0].get("value", "") if title_list else ""
    title_len = len(title)
    if title_len == 0:
        add("CRITICAL", "missing_title", "No title found")
    elif title_len < MIN_TITLE_LEN:
        add("WARNING", "short_title", f"Title too short ({title_len} chars < {MIN_TITLE_LEN}): '{title[:80]}'")
    elif title_len > 200:
        add("WARNING", "long_title", f"Title too long ({title_len} chars > 200)")

    # 4. Bullet points
    bullets = attrs.get("bullet_point", [])
    if len(bullets) == 0:
        add("CRITICAL", "missing_bullets", "No bullet points at all")
    elif len(bullets) < MIN_BULLETS:
        add("WARNING", "few_bullets", f"Only {len(bullets)} bullet points (min {MIN_BULLETS})")

    # 5. Product description
    desc = attrs.get("product_description", [])
    if not desc:
        add("INFO", "missing_description", "No product description")

    # 6. Images — check main_product_image_locator or other_product_image_locator_1..8
    image_keys = ["main_product_image_locator"] + [f"other_product_image_locator_{i}" for i in range(1, 9)]
    image_count = sum(1 for k in image_keys if attrs.get(k))
    if image_count == 0:
        add("CRITICAL", "missing_images", "No images found in attributes")
    elif image_count < MIN_IMAGES:
        add("WARNING", "few_images", f"Only {image_count} images (min {MIN_IMAGES})")

    # 7. GPSR compliance
    missing_gpsr = [attr for attr in REQUIRED_GPSR_ATTRS if not attrs.get(attr)]
    if missing_gpsr:
        add("WARNING", "missing_gpsr", f"Missing GPSR attrs: {missing_gpsr}")

    # 8. Generic keywords / backend keywords
    if not attrs.get("generic_keyword"):
        add("INFO", "missing_keywords", "No backend keywords (generic_keyword)")

    # 9. Brand
    if not attrs.get("brand"):
        add("INFO", "missing_brand", "No brand attribute")

    # 10. Amazon-reported issues (from API)
    for api_issue in api_issues:
        sev = api_issue.get("severity", "ERROR")
        code = api_issue.get("code", "")
        msg = api_issue.get("message", "")
        # Map Amazon severity to our labels
        mapped_sev = "CRITICAL" if sev in ("ERROR",) else "WARNING"
        add(mapped_sev, f"api_issue_{code.lower()}", f"[Amazon] {sev}: {msg}")

    return issues


# ── Main Audit ────────────────────────────────────────────────────────

def run_audit(target_marketplaces: dict) -> dict:
    """Run full audit across all target marketplaces."""
    all_issues = []
    marketplace_stats = {}
    seen_skus = set()
    total_listings_raw = 0

    for mkt_code, mkt_id in target_marketplaces.items():
        print(f"\n[{mkt_code}] Fetching listings from marketplace {mkt_id}...")
        try:
            listings = fetch_all_listings(mkt_id)
        except Exception as e:
            print(f"  [ERROR] Failed to fetch {mkt_code}: {e}")
            marketplace_stats[mkt_code] = {"listings": 0, "issues": 0, "error": str(e)}
            continue

        print(f"  [{mkt_code}] Got {len(listings)} listings, checking issues...")
        mkt_issues = []

        for item in listings:
            total_listings_raw += 1
            sku = item.get("sku", "")
            seen_skus.add(sku)
            item_issues = check_listing(item, mkt_code)
            mkt_issues.extend(item_issues)
            all_issues.extend(item_issues)

        marketplace_stats[mkt_code] = {
            "listings": len(listings),
            "issues": len(mkt_issues),
            "critical": sum(1 for i in mkt_issues if i["severity"] == "CRITICAL"),
            "warning": sum(1 for i in mkt_issues if i["severity"] == "WARNING"),
            "info": sum(1 for i in mkt_issues if i["severity"] == "INFO"),
        }
        print(f"  [{mkt_code}] Done: {len(mkt_issues)} issues found")

    # ── Aggregate issue frequency ─────────────────────────────────────
    issue_freq = defaultdict(int)
    for issue in all_issues:
        issue_freq[issue["issue_type"]] += 1

    top_5 = sorted(issue_freq.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "audit_date": str(date.today()),
        "seller_id": SELLER_ID,
        "marketplaces_audited": list(target_marketplaces.keys()),
        "total_listings_checked": total_listings_raw,
        "unique_skus": len(seen_skus),
        "total_issues": len(all_issues),
        "issue_by_severity": {
            "CRITICAL": sum(1 for i in all_issues if i["severity"] == "CRITICAL"),
            "WARNING": sum(1 for i in all_issues if i["severity"] == "WARNING"),
            "INFO": sum(1 for i in all_issues if i["severity"] == "INFO"),
        },
        "top_5_issues_by_frequency": [
            {"issue_type": k, "count": v} for k, v in top_5
        ],
        "marketplace_stats": marketplace_stats,
        "issues": all_issues,
    }


# ── CLI ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Amazon EU listing audit")
    parser.add_argument("--marketplace", help="Single marketplace code (DE, FR, etc.)")
    parser.add_argument(
        "--output",
        default=f"/tmp/amazon-audit-{date.today()}.json",
        help="Output JSON file path",
    )
    args = parser.parse_args()

    if args.marketplace:
        if args.marketplace not in MARKETPLACES:
            print(f"Unknown marketplace: {args.marketplace}. Valid: {list(MARKETPLACES)}")
            sys.exit(1)
        target = {args.marketplace: MARKETPLACES[args.marketplace]}
    else:
        target = MARKETPLACES

    print(f"=== Amazon Listing Audit — {date.today()} ===")
    print(f"Marketplaces: {list(target.keys())}")
    print(f"Seller ID: {SELLER_ID}")

    result = run_audit(target)

    with open(args.output, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\n=== AUDIT COMPLETE ===")
    print(f"Total listings checked: {result['total_listings_checked']}")
    print(f"Unique SKUs: {result['unique_skus']}")
    print(f"Total issues: {result['total_issues']}")
    print(f"  CRITICAL: {result['issue_by_severity']['CRITICAL']}")
    print(f"  WARNING:  {result['issue_by_severity']['WARNING']}")
    print(f"  INFO:     {result['issue_by_severity']['INFO']}")
    print(f"\nTop 5 issues by frequency:")
    for entry in result["top_5_issues_by_frequency"]:
        print(f"  {entry['count']:3d}x  {entry['issue_type']}")
    print(f"\nResults saved to: {args.output}")


if __name__ == "__main__":
    main()
