#!/usr/bin/env python3
"""
Update Amazon dad hat listing images using image_config.json order.

For each dad hat template: generates Printful mockups per color variant,
maps URLs to Amazon slots using image_config order, PATCHes all 8 EU markets.

Usage:
    python3.11 scripts/update_dad_hat_images.py --dry-run
    python3.11 scripts/update_dad_hat_images.py
    python3.11 scripts/update_dad_hat_images.py --template 90202876   # single template
"""

import json
import re
import sys
import time
import urllib.parse
from datetime import datetime

import requests

# ── Config ───────────────────────────────────────────────────────────────────

with open("/Users/alexanderrogalski/.keys/amazon-sp-api.json") as f:
    AMZ_CREDS = json.load(f)

with open("/Users/alexanderrogalski/.keys/printful.env") as f:
    PF_ENV = {}
    for line in f:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            PF_ENV[k] = v

PRINTFUL_TOKEN = PF_ENV.get("PRINTFUL_API_TOKEN", "")
STORE_ID = PF_ENV.get("PRINTFUL_STORE_ID", "")
SELLER_ID = "A1IZH6PW7A624A"
AMZ_ENDPOINT = "https://sellingpartnerapi-eu.amazon.com"
PRODUCT_TYPE = "HAT"
PRINTFUL_PRODUCT_ID = 206  # Dad Hat Yupoong 6245CM

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

OPTION_GROUPS = ["Flat", "Flat, Premium", "Production", "Product details"]

CHILDREN = ["7853", "7854", "7855", "7856", "7857", "7858", "7859", "8745", "9794", "12735", "12736"]
COLOR_NAMES = {
    "7853": "White", "7854": "Black", "7855": "Khaki", "7856": "Light Blue",
    "7857": "Navy", "7858": "Pink", "7859": "Stone", "8745": "Spruce",
    "9794": "Green Camo", "12735": "Cranberry", "12736": "Dark Grey",
}

# All dad hat templates (template_id extracted from parent SKU PFT-{template_id})
TEMPLATES = {
    90202876: "America (flag)",
    90201053: "Sweden (flag)",
    90034427: "Germany (no flag)",
    93854576: "France (flag)",
    93854948: "Italy (flag)",
    93855132: "Poland (flag)",
    93855391: "Spain (no flag)",
    93855556: "Belgium (flag)",
    93855915: "Netherlands (flag)",
    93856110: "GB (flag)",
    93856169: "GB (no flag)",
    93856209: "Netherlands (no flag)",
    93856238: "Belgium (no flag)",
    93856317: "France (no flag)",
    # Europe variants
    89932086: "Europe (no flag)",
    99682362: "Europe (flag copy)",
    100032925: "Germany (flag)",
}

# Load image_config for ordering
with open("/Users/alexanderrogalski/nesell-analytics/etl/image_config.json") as f:
    IMAGE_CONFIG = json.load(f)

DAD_HAT_ORDER = [e["placement_id"] for e in IMAGE_CONFIG["dad_hat"]["image_order"]]
DAD_HAT_LABELS = {e["placement_id"]: e["label"] for e in IMAGE_CONFIG["dad_hat"]["image_order"]}


# ── Helpers ──────────────────────────────────────────────────────────────────

def pf_headers():
    return {
        "Authorization": f"Bearer {PRINTFUL_TOKEN}",
        "X-PF-Store-Id": STORE_ID,
        "Content-Type": "application/json",
    }


def get_amz_token():
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": AMZ_CREDS["refresh_token"],
        "client_id": AMZ_CREDS["client_id"],
        "client_secret": AMZ_CREDS["client_secret"],
    })
    r.raise_for_status()
    return r.json()["access_token"]


def generate_mockups(template_id, variant_id, max_retries=3):
    """Generate mockups via Printful. Returns {generator_mockup_id: url}."""
    payload = {
        "variant_ids": [variant_id],
        "product_template_id": template_id,
        "option_groups": OPTION_GROUPS,
    }

    task_key = None
    for attempt in range(max_retries):
        try:
            r = requests.post(
                f"https://api.printful.com/mockup-generator/create-task/{PRINTFUL_PRODUCT_ID}",
                headers=pf_headers(), json=payload, timeout=30,
            )
        except requests.exceptions.ConnectionError:
            print(f"      Connection error (attempt {attempt+1}), waiting 60s...")
            time.sleep(60)
            continue

        if r.status_code == 429:
            wait = 60
            try:
                m = re.search(r"(\d+) seconds", r.json().get("error", {}).get("message", ""))
                if m:
                    wait = int(m.group(1)) + 5
            except Exception:
                pass
            print(f"      Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue

        if r.status_code != 200:
            print(f"      ERROR {r.status_code}: {r.text[:200]}")
            return None

        task_key = r.json().get("result", {}).get("task_key")
        break

    if not task_key:
        return None

    # Poll
    for _ in range(30):
        time.sleep(10)
        try:
            pr = requests.get(
                f"https://api.printful.com/mockup-generator/task?task_key={task_key}",
                headers=pf_headers(), timeout=30,
            )
        except requests.exceptions.ConnectionError:
            time.sleep(10)
            continue

        if pr.status_code == 429:
            time.sleep(30)
            continue

        result = pr.json().get("result", {})
        status = result.get("status")

        if status == "completed":
            out = {}
            for m in result.get("mockups", []):
                gid = m.get("generator_mockup_id")
                url = m.get("mockup_url")
                if gid and url:
                    out[gid] = url
                for ex in m.get("extra", []):
                    egid = ex.get("generator_mockup_id")
                    eurl = ex.get("url")
                    if egid and eurl:
                        out[egid] = eurl
            return out

        elif status == "failed":
            print(f"      Mockup failed: {result.get('error')}")
            return None

    print("      Mockup timed out")
    return None


def build_ordered_urls(mockup_dict):
    """Map mockup URLs to ordered list using image_config placement order."""
    urls = []
    for pid in DAD_HAT_ORDER:
        url = mockup_dict.get(pid)
        if url:
            urls.append(url)
    return urls


def patch_images(token, sku, marketplace_id, image_urls):
    """PATCH image attributes on Amazon listing."""
    encoded = urllib.parse.quote(sku, safe="")
    url = f"{AMZ_ENDPOINT}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"

    patches = []
    if image_urls:
        patches.append({
            "op": "replace",
            "path": "/attributes/main_product_image_locator",
            "value": [{"media_location": image_urls[0], "marketplace_id": marketplace_id}],
        })
    for i, img in enumerate(image_urls[1:8]):
        patches.append({
            "op": "replace",
            "path": f"/attributes/other_product_image_locator_{i+1}",
            "value": [{"media_location": img, "marketplace_id": marketplace_id}],
        })

    body = {"productType": PRODUCT_TYPE, "patches": patches}
    headers = {"x-amz-access-token": token, "Content-Type": "application/json"}

    for retry in range(3):
        try:
            r = requests.patch(url, headers=headers, params={"marketplaceIds": marketplace_id}, json=body, timeout=30)
        except Exception:
            time.sleep(5)
            continue
        if r.status_code == 429:
            time.sleep(5)
            continue
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, {"raw": r.text[:300]}

    return 0, {"error": "max retries"}


def check_listing(token, sku, marketplace_id):
    encoded = urllib.parse.quote(sku, safe="")
    url = f"{AMZ_ENDPOINT}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
    headers = {"x-amz-access-token": token, "Content-Type": "application/json"}
    for _ in range(3):
        try:
            r = requests.get(url, headers=headers, params={"marketplaceIds": marketplace_id, "includedData": "summaries"}, timeout=30)
        except Exception:
            time.sleep(5)
            continue
        if r.status_code == 429:
            time.sleep(5)
            continue
        return r.status_code == 200
    return False


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv
    single_template = None
    for arg in sys.argv:
        if arg.startswith("--template"):
            idx = sys.argv.index(arg)
            if idx + 1 < len(sys.argv):
                single_template = int(sys.argv[idx + 1])

    templates = {single_template: TEMPLATES.get(single_template, "?")} if single_template else TEMPLATES

    print("=" * 80)
    print(f"DAD HAT IMAGE UPDATE - {datetime.now():%Y-%m-%d %H:%M}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Templates: {len(templates)}, Colors: {len(CHILDREN)}, Markets: {len(MARKETPLACES)}")
    print(f"Image order: {[DAD_HAT_LABELS.get(pid, pid) for pid in DAD_HAT_ORDER]}")
    print("=" * 80)

    amz_token = get_amz_token()
    token_time = time.time()

    stats = {"generated": 0, "patched": 0, "skipped": 0, "errors": 0}

    for tid, tname in templates.items():
        print(f"\n{'─'*60}")
        print(f"Template {tid}: {tname}")
        print(f"{'─'*60}")

        parent_sku = f"PFT-{tid}"

        for suffix in CHILDREN:
            variant_id = int(suffix)
            child_sku = f"PFT-{tid}-{suffix}"
            color = COLOR_NAMES.get(suffix, suffix)

            print(f"\n  [{color}] {child_sku}")

            # Generate mockups
            print(f"    Generating mockups...", end=" ", flush=True)
            mockups = generate_mockups(tid, variant_id)
            if not mockups:
                print("FAILED")
                stats["errors"] += 1
                continue

            ordered = build_ordered_urls(mockups)
            print(f"OK ({len(ordered)} images)")
            stats["generated"] += 1

            if not ordered:
                print(f"    No matching placement IDs! Got: {list(mockups.keys())}")
                stats["errors"] += 1
                continue

            # Refresh token if needed
            if time.time() - token_time > 2700:
                amz_token = get_amz_token()
                token_time = time.time()

            # Patch all marketplaces
            skus_to_patch = [child_sku]
            # Parent gets Black images
            if suffix == "7854":
                skus_to_patch.append(parent_sku)

            for sku in skus_to_patch:
                for mp, mp_id in MARKETPLACES.items():
                    if dry_run:
                        stats["patched"] += 1
                        continue

                    exists = check_listing(amz_token, sku, mp_id)
                    time.sleep(0.3)
                    if not exists:
                        stats["skipped"] += 1
                        continue

                    code, resp = patch_images(amz_token, sku, mp_id, ordered)
                    time.sleep(0.5)

                    if code == 200:
                        amz_status = resp.get("status", "?")
                        issues = resp.get("issues", [])
                        if issues:
                            print(f"    {sku} {mp}: {amz_status} ({len(issues)} issues)")
                        stats["patched"] += 1
                    else:
                        print(f"    {sku} {mp}: ERROR {code}")
                        stats["errors"] += 1

            # Small delay between variants to avoid Printful rate limits
            time.sleep(5)

        if dry_run:
            print(f"\n  DRY RUN: would patch {len(CHILDREN)} colors x {len(MARKETPLACES)} markets")

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"  Mockups generated: {stats['generated']}")
    print(f"  Listings patched:  {stats['patched']}")
    print(f"  Skipped (404):     {stats['skipped']}")
    print(f"  Errors:            {stats['errors']}")
    print(f"{'='*80}")

    # Save results
    out = f"/Users/alexanderrogalski/nesell-analytics/scripts/dad_hat_image_results.json"
    with open(out, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "mode": "dry_run" if dry_run else "live", "stats": stats}, f, indent=2)
    print(f"Results saved to {out}")


if __name__ == "__main__":
    main()
