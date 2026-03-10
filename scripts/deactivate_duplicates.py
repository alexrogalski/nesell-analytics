#!/usr/bin/env python3
"""
Deactivate 18 duplicate PFT Amazon listings across all 8 EU marketplaces.

Uses Amazon SP-API Listings API DELETE method to deactivate offers.
This sets quantity to 0 and removes from active listings (does NOT delete ASIN).

SKUs to DEACTIVATE (18):
  Germany template (PFT-90034427-*): 8 SKUs
  Europe no-flag template (PFT-88471944-*): 10 SKUs

SKUs to KEEP (4) — NOT touched by this script:
  PFT-90034427-12735, PFT-90034427-8745, PFT-88471944-8745, PFT-82980216-8745
"""

import json
import time
import sys
import urllib.parse
from datetime import datetime

import requests


# ─── Configuration ───────────────────────────────────────────────────────────

CREDENTIALS_PATH = "/Users/alexanderrogalski/.keys/amazon-sp-api.json"

SKUS_TO_DEACTIVATE = [
    # Germany template (PFT-90034427-*)
    "PFT-90034427-7854",
    "PFT-90034427-7857",
    "PFT-90034427-7853",
    "PFT-90034427-7855",
    "PFT-90034427-7856",
    "PFT-90034427-7858",
    "PFT-90034427-7859",
    "PFT-90034427-12736",
    # Europe no-flag template (PFT-88471944-*)
    "PFT-88471944-7853",
    "PFT-88471944-7854",
    "PFT-88471944-7855",
    "PFT-88471944-7856",
    "PFT-88471944-7857",
    "PFT-88471944-7858",
    "PFT-88471944-7859",
    "PFT-88471944-12736",
    "PFT-88471944-9794",
    "PFT-88471944-12735",
]

SKUS_TO_KEEP = [
    "PFT-90034427-12735",
    "PFT-90034427-8745",
    "PFT-88471944-8745",
    "PFT-82980216-8745",
]

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

SP_API_BASE = "https://sellingpartnerapi-eu.amazon.com"
TOKEN_URL = "https://api.amazon.com/auth/o2/token"

# Rate limiting: sleep between API calls (seconds)
SLEEP_BETWEEN_CALLS = 0.5


# ─── Safety check ────────────────────────────────────────────────────────────

def safety_check():
    """Verify no KEEP SKUs are in the deactivation list."""
    overlap = set(SKUS_TO_DEACTIVATE) & set(SKUS_TO_KEEP)
    if overlap:
        print(f"SAFETY ABORT: These SKUs are in BOTH deactivate and keep lists: {overlap}")
        sys.exit(1)
    print(f"Safety check passed: {len(SKUS_TO_DEACTIVATE)} SKUs to deactivate, {len(SKUS_TO_KEEP)} SKUs protected")
    print(f"  Deactivate: {SKUS_TO_DEACTIVATE}")
    print(f"  Keep (not touched): {SKUS_TO_KEEP}")
    print()


# ─── Auth ────────────────────────────────────────────────────────────────────

def load_credentials():
    with open(CREDENTIALS_PATH) as f:
        return json.load(f)


def get_access_token(creds):
    """Get LWA access token using refresh token."""
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": creds["refresh_token"],
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
    })
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print(f"Got access token: {token[:20]}...")
    return token


# ─── Listings API ────────────────────────────────────────────────────────────

def deactivate_listing(seller_id, sku, marketplace_id, access_token):
    """
    DELETE /listings/2021-08-01/items/{sellerId}/{sku}?marketplaceIds={marketplaceId}

    Returns (success: bool, status_code: int, response_body: dict/str)
    """
    # URL-encode the SKU (it may contain special chars, though these don't)
    encoded_sku = urllib.parse.quote(sku, safe="")
    url = f"{SP_API_BASE}/listings/2021-08-01/items/{seller_id}/{encoded_sku}"

    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
    }
    params = {
        "marketplaceIds": marketplace_id,
    }

    resp = requests.delete(url, headers=headers, params=params)

    try:
        body = resp.json()
    except Exception:
        body = resp.text

    success = resp.status_code in (200, 204)
    return success, resp.status_code, body


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Amazon SP-API: Deactivate Duplicate PFT Listings")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 70)
    print()

    # Safety check
    safety_check()

    # Load credentials and get token
    creds = load_credentials()
    seller_id = creds["seller_id"]
    access_token = get_access_token(creds)
    print()

    # Track results
    total = len(SKUS_TO_DEACTIVATE) * len(MARKETPLACES)
    success_count = 0
    error_count = 0
    results = []

    print(f"Deactivating {len(SKUS_TO_DEACTIVATE)} SKUs across {len(MARKETPLACES)} marketplaces ({total} total calls)")
    print("-" * 70)

    for sku in SKUS_TO_DEACTIVATE:
        for country, mp_id in MARKETPLACES.items():
            label = f"{sku} @ {country} ({mp_id})"

            try:
                ok, status, body = deactivate_listing(seller_id, sku, mp_id, access_token)

                if ok:
                    success_count += 1
                    status_str = body.get("status", "OK") if isinstance(body, dict) else "OK"
                    print(f"  [OK]    {label} -> {status} {status_str}")
                else:
                    error_count += 1
                    error_msg = ""
                    if isinstance(body, dict):
                        errors = body.get("errors", [])
                        if errors:
                            error_msg = errors[0].get("message", str(body))
                        else:
                            error_msg = str(body)
                    else:
                        error_msg = str(body)[:200]
                    print(f"  [FAIL]  {label} -> {status} {error_msg}")

                results.append({
                    "sku": sku,
                    "marketplace": country,
                    "marketplace_id": mp_id,
                    "success": ok,
                    "status_code": status,
                    "response": body if isinstance(body, dict) else {"raw": str(body)[:500]},
                })

            except Exception as e:
                error_count += 1
                print(f"  [ERROR] {label} -> {type(e).__name__}: {e}")
                results.append({
                    "sku": sku,
                    "marketplace": country,
                    "marketplace_id": mp_id,
                    "success": False,
                    "status_code": 0,
                    "response": {"error": str(e)},
                })

            time.sleep(SLEEP_BETWEEN_CALLS)

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Total API calls:  {total}")
    print(f"  Successful:       {success_count}")
    print(f"  Failed:           {error_count}")
    print(f"  Success rate:     {success_count/total*100:.1f}%")
    print()

    # Per-SKU summary
    print("Per-SKU results:")
    for sku in SKUS_TO_DEACTIVATE:
        sku_results = [r for r in results if r["sku"] == sku]
        sku_ok = sum(1 for r in sku_results if r["success"])
        sku_fail = sum(1 for r in sku_results if not r["success"])
        status_icon = "OK" if sku_fail == 0 else "PARTIAL" if sku_ok > 0 else "FAIL"
        print(f"  [{status_icon:7s}] {sku}: {sku_ok}/{len(sku_results)} marketplaces")

    print()
    print(f"Finished: {datetime.now().isoformat()}")

    # Save detailed results
    results_path = "/Users/alexanderrogalski/nesell-analytics/scripts/deactivate_results.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Detailed results saved to: {results_path}")

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
