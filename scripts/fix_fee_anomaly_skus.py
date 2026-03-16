"""Fix fee-anomaly SKUs identified in pnl-analysis-2026-03-16.md.

Actions:
1. Map 'brak1' → 'SX7667-100' in products table (update name, cost, active)
2. Deactivate duplicate SKU 'L_socks_SX7666-100_888408282804' (real product: SX7666-010-L)
3. Deactivate low-velocity SKU '196575379679' (1 unit/90 days, confirmed data error)

Root causes (all were data errors, NOT A-to-Z claims):
- L_socks + 196575379679: aggregator null unit_price bug → fees counted, revenue=0
- brak1: orphan SKU with no product mapping + revenue under-counted (shipping excluded)

The aggregator bug is fixed in etl/aggregator.py (skip items where unit_price is null/0).
After running this script, re-run: python3.11 -m etl.run --aggregate
"""
import os
import sys
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://igyceeknivjdbvjqxcdi.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_secret_YRAEF-3-XOnly0iIaCP3yA_T7EQ8qd1")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}


def rest(method: str, path: str, **kwargs) -> requests.Response:
    url = f"{SUPABASE_URL}/rest/v1{path}"
    resp = requests.request(method, url, headers=HEADERS, **kwargs)
    return resp


def get_product(sku: str) -> dict | None:
    resp = rest("GET", f"/products?sku=eq.{sku}&select=sku,name,cost_pln,active")
    resp.raise_for_status()
    rows = resp.json()
    return rows[0] if rows else None


def patch_product(sku: str, payload: dict) -> dict:
    resp = rest("PATCH", f"/products?sku=eq.{sku}", json=payload)
    resp.raise_for_status()
    return resp.json()


def main():
    errors = []

    # ------------------------------------------------------------------ #
    # 1. Map brak1 → SX7667-100                                           #
    # ------------------------------------------------------------------ #
    print("\n[1] brak1 → SX7667-100 mapping")

    # Look up the real product cost
    real_product = get_product("SX7667-100")
    brak1 = get_product("brak1")

    if not brak1:
        print("  SKIP: brak1 not found in products table (may already be fixed)")
    else:
        print(f"  Current brak1: name='{brak1.get('name')}', cost={brak1.get('cost_pln')}, active={brak1.get('active')}")

        cost = real_product["cost_pln"] if real_product else brak1.get("cost_pln")
        name = real_product["name"] if real_product else "Nike Socks SX7667-100"
        if real_product:
            print(f"  Real product SX7667-100: name='{real_product.get('name')}', cost={real_product.get('cost_pln')}")

        # Update brak1 record with correct name, cost, and mark inactive
        # Note: we can't rename the SKU (it's the PK) — we deactivate brak1
        # and let the correct SX7667-100 entry handle future orders.
        result = patch_product("brak1", {
            "name": f"[ORPHAN] Nike Socks SX7667-100 — mapped to SX7667-100",
            "cost_pln": cost,
            "active": False,
        })
        print(f"  Updated brak1: marked inactive, name updated, cost={cost}")

        # Ensure the real SX7667-100 product exists and is active
        if not real_product:
            print("  WARNING: SX7667-100 not found — inserting placeholder")
            resp = rest("POST", "/products", json={
                "sku": "SX7667-100",
                "name": "Nike Socks SX7667-100 (3-pack)",
                "cost_pln": 32.50,
                "active": True,
                "source": "wholesale",
            })
            if resp.status_code in (200, 201):
                print("  Created SX7667-100 product record")
            else:
                errors.append(f"Failed to create SX7667-100: {resp.text}")
        else:
            patch_product("SX7667-100", {"active": True})
            print("  SX7667-100 confirmed active")

    # ------------------------------------------------------------------ #
    # 2. Deactivate L_socks duplicate                                     #
    # ------------------------------------------------------------------ #
    print("\n[2] Deactivate L_socks_SX7666-100_888408282804")

    dup_sku = "L_socks_SX7666-100_888408282804"
    dup = get_product(dup_sku)

    if not dup:
        print("  SKIP: SKU not found (may already be deactivated or doesn't exist as product)")
    else:
        print(f"  Current: name='{dup.get('name')}', active={dup.get('active')}")
        patch_product(dup_sku, {
            "active": False,
            "name": f"[DEACTIVATED DUPLICATE] {dup.get('name', dup_sku)} — use SX7666-010-L",
        })
        print("  Marked inactive (real product: SX7666-010-L)")

    # Confirm the real product is active
    real = get_product("SX7666-010-L")
    if real:
        if not real.get("active"):
            patch_product("SX7666-010-L", {"active": True})
            print("  Activated real product SX7666-010-L")
        else:
            print("  Real product SX7666-010-L is active ✓")
    else:
        print("  WARNING: SX7666-010-L not found in products table")

    # ------------------------------------------------------------------ #
    # 3. Deactivate 196575379679 (low velocity, confirmed data error)     #
    # ------------------------------------------------------------------ #
    print("\n[3] Deactivate 196575379679 (THE NORTH FACE hat, low velocity)")

    sku_196 = "196575379679"
    prod_196 = get_product(sku_196)

    if not prod_196:
        print("  INFO: SKU not in products table (likely order-only, no product record)")
        # Insert a deactivated record to signal intent
        resp = rest("POST", "/products", json={
            "sku": sku_196,
            "name": "[DEACTIVATED] THE NORTH FACE Norm Hoed — low velocity (1 unit/90d)",
            "cost_pln": 42.27,
            "active": False,
            "source": "wholesale",
        })
        if resp.status_code in (200, 201):
            print(f"  Created deactivated product record for {sku_196}")
        else:
            # If already exists (409 conflict), try patch
            patch_product(sku_196, {"active": False})
            print(f"  Patched {sku_196} as inactive")
    else:
        print(f"  Current: name='{prod_196.get('name')}', active={prod_196.get('active')}")
        patch_product(sku_196, {
            "active": False,
            "name": f"[DEACTIVATED] {prod_196.get('name', sku_196)} — low velocity",
        })
        print(f"  Marked inactive")

    # ------------------------------------------------------------------ #
    # Summary                                                              #
    # ------------------------------------------------------------------ #
    print("\n" + "=" * 60)
    if errors:
        print(f"Completed with {len(errors)} error(s):")
        for e in errors:
            print(f"  ERROR: {e}")
        sys.exit(1)
    else:
        print("All SKU fixes applied successfully.")
        print("\nNext step: re-run aggregator to fix daily_metrics:")
        print("  cd ~/nesell-analytics && python3.11 -m etl.run --aggregate")


if __name__ == "__main__":
    main()
