#!/usr/bin/env python3.11
"""
Cleanup script: remove duplicate order_items rows.

For each (order_id, sku) combination, keeps the row with the lowest id
and deletes all others via Supabase REST API.

This is a one-time script that should be run BEFORE the migration that
adds the UNIQUE constraint on (order_id, sku).
"""
import os, sys, requests, json
from collections import defaultdict
from dotenv import load_dotenv

# Load env
load_dotenv(os.path.expanduser("~/nesell-analytics/.env"))
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}


def fetch_all_order_items():
    """Fetch all order_items (id, order_id, sku) with pagination."""
    all_rows = []
    offset = 0
    batch_size = 1000
    while True:
        params = {
            "select": "id,order_id,sku",
            "order": "id.asc",
            "offset": str(offset),
            "limit": str(batch_size),
        }
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/order_items",
            headers=HEADERS,
            params=params,
        )
        if resp.status_code != 200:
            print(f"ERROR fetching order_items: {resp.status_code} {resp.text[:200]}")
            sys.exit(1)
        rows = resp.json()
        all_rows.extend(rows)
        print(f"  Fetched {len(all_rows)} rows so far...")
        if len(rows) < batch_size:
            break
        offset += batch_size
    return all_rows


def find_duplicates(rows):
    """Group by (order_id, sku), return list of ids to delete."""
    groups = defaultdict(list)
    for r in rows:
        key = (r["order_id"], r.get("sku") or "")
        groups[key].append(r["id"])

    ids_to_delete = []
    dupe_groups = 0
    for key, ids in groups.items():
        if len(ids) > 1:
            dupe_groups += 1
            ids.sort()  # keep lowest id
            ids_to_delete.extend(ids[1:])  # delete the rest

    return ids_to_delete, dupe_groups


def delete_by_ids(ids_to_delete):
    """Delete rows by id via REST API, in batches."""
    deleted = 0
    batch_size = 100  # delete in batches to avoid URL length limits
    for i in range(0, len(ids_to_delete), batch_size):
        batch = ids_to_delete[i:i+batch_size]
        # PostgREST: DELETE with id=in.(1,2,3,...)
        id_list = ",".join(str(x) for x in batch)
        resp = requests.delete(
            f"{SUPABASE_URL}/rest/v1/order_items",
            headers=HEADERS,
            params={"id": f"in.({id_list})"},
        )
        if resp.status_code not in (200, 204):
            print(f"  ERROR deleting batch: {resp.status_code} {resp.text[:200]}")
        else:
            deleted += len(batch)
            print(f"  Deleted {deleted}/{len(ids_to_delete)} duplicate rows...")
    return deleted


def verify_sku(sku_to_check="194276338162"):
    """Verify a specific SKU has correct row count after cleanup."""
    params = {
        "select": "id,order_id,sku,created_at",
        "sku": f"eq.{sku_to_check}",
        "order": "id.asc",
    }
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/order_items",
        headers=HEADERS,
        params=params,
    )
    if resp.status_code != 200:
        print(f"ERROR verifying SKU: {resp.status_code}")
        return
    rows = resp.json()
    print(f"\n  SKU {sku_to_check}: {len(rows)} rows")
    for r in rows:
        print(f"    id={r['id']}, order_id={r['order_id']}, created_at={r['created_at']}")
    return len(rows)


def main():
    print("=" * 60)
    print("ORDER_ITEMS DUPLICATE CLEANUP")
    print("=" * 60)

    # Step 1: Fetch all order_items
    print("\n[1/4] Fetching all order_items...")
    rows = fetch_all_order_items()
    print(f"  Total rows: {len(rows)}")

    # Step 2: Find duplicates
    print("\n[2/4] Finding duplicates...")
    ids_to_delete, dupe_groups = find_duplicates(rows)
    print(f"  Duplicate groups: {dupe_groups}")
    print(f"  Rows to delete: {len(ids_to_delete)}")
    print(f"  Rows to keep: {len(rows) - len(ids_to_delete)}")

    if not ids_to_delete:
        print("\n  No duplicates found! Nothing to clean up.")
    else:
        # Step 3: Delete duplicates
        print(f"\n[3/4] Deleting {len(ids_to_delete)} duplicate rows...")
        deleted = delete_by_ids(ids_to_delete)
        print(f"  DONE: Deleted {deleted} duplicate rows")

    # Step 4: Verify specific SKU
    print("\n[4/4] Verifying SKU 194276338162...")
    count = verify_sku("194276338162")
    if count == 3:
        print("  PASS: SKU has exactly 3 rows (as expected)")
    elif count is not None:
        print(f"  NOTE: SKU has {count} rows (expected 3)")

    # Final count
    print("\n" + "=" * 60)
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/order_items",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                 "Prefer": "count=exact"},
        params={"select": "id", "limit": "0"},
    )
    total_after = resp.headers.get("content-range", "unknown")
    print(f"  Final order_items count: {total_after}")
    print("=" * 60)


if __name__ == "__main__":
    main()
