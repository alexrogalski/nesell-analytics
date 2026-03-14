"""COGS Filler: find products with missing cost data and fill from available sources.

Sources (in priority order):
1. Baselinker warehouse "test-exportivo" (inv_id=30229) -- average_cost field
2. Printful API costs (for PFT- prefix SKUs) -- via printful_costs.py logic
3. Nike socks hardcoded: 50 PLN per 6-pack = ~8.33 PLN per unit
4. Baselinker other inventories (63813 "All", etc.) -- fallback

Only fills where cost_pln IS NULL or cost_pln = 0. Never overwrites existing > 0 values.
"""

import requests
import json
import time
from datetime import date
from collections import defaultdict
from . import config, db


# ── Supabase helpers (reuse db module's internals) ──────────────────

def _get_missing_cogs_products():
    """Fetch all products from Supabase where cost_pln is NULL or 0."""
    all_products = []
    offset = 0
    while True:
        rows = db._get("products", {
            "select": "sku,name,cost_pln,cost_eur,source",
            "or": "(cost_pln.is.null,cost_pln.eq.0)",
            "order": "sku",
            "limit": "1000",
            "offset": str(offset),
        })
        all_products.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return all_products


def _update_product_cost(sku, cost_pln, cost_eur=None, source=None):
    """Update a single product's cost in Supabase."""
    data = {"cost_pln": round(cost_pln, 2)}
    if cost_eur is not None:
        data["cost_eur"] = round(cost_eur, 2)
    if source is not None:
        data["source"] = source
    db._patch("products", {"sku": f"eq.{sku}"}, data)


def _log_cost_history(entries):
    """Log cost changes to cost_history table."""
    if not entries:
        return
    for i in range(0, len(entries), 500):
        chunk = entries[i:i + 500]
        try:
            db._post("cost_history", chunk)
        except Exception as e:
            print(f"  WARNING: cost_history insert error: {e}")


# ── Baselinker warehouse lookup ─────────────────────────────────────

def _bl_api(method, params=None):
    """Call Baselinker API with rate limit retry (simplified version)."""
    for attempt in range(3):
        resp = requests.post(config.BASELINKER_URL, data={
            "token": config.BASELINKER_TOKEN,
            "method": method,
            "parameters": json.dumps(params or {})
        }, timeout=60)
        data = resp.json()
        if data.get("status") == "ERROR":
            msg = data.get("error_message", "")
            if "limit exceeded" in msg.lower() or "blocked until" in msg.lower():
                wait = 60 * (attempt + 1)
                print(f"  [Rate limit] Waiting {wait}s...")
                time.sleep(wait)
                continue
            raise Exception(f"Baselinker {method}: {msg}")
        return data
    raise Exception(f"Baselinker {method}: rate limit exceeded after 3 retries")


def _fetch_bl_warehouse_costs(inventory_id, skus_to_find):
    """Search Baselinker inventory for products and return {sku: average_cost}.

    Goes through all pages of the inventory, collects products whose SKU
    (or variant SKU) matches one of the requested SKUs, and returns their
    average_cost values.
    """
    found_costs = {}
    if not skus_to_find:
        return found_costs

    sku_set = set(skus_to_find)
    page = 1
    all_product_ids = []

    # Step 1: List all product IDs in inventory
    while True:
        data = _bl_api("getInventoryProductsList", {
            "inventory_id": inventory_id,
            "page": page,
        })
        products = data.get("products", {})
        if not products:
            break
        all_product_ids.extend(products.keys())
        page += 1
        time.sleep(0.3)

    print(f"  [BL inv {inventory_id}] Total product IDs: {len(all_product_ids)}")

    # Step 2: Fetch detailed data in batches of 100 and match SKUs
    for i in range(0, len(all_product_ids), 100):
        batch_ids = all_product_ids[i:i + 100]
        data = _bl_api("getInventoryProductsData", {
            "inventory_id": inventory_id,
            "products": batch_ids,
        })

        for pid, p in data.get("products", {}).items():
            sku = p.get("sku", "")
            avg_cost = float(p.get("average_cost", 0) or 0)

            if sku in sku_set and avg_cost > 0:
                found_costs[sku] = avg_cost

            # Also check variants
            for vid, v in p.get("variants", {}).items():
                vsku = v.get("sku", "")
                v_cost = float(v.get("average_cost", 0) or 0)
                if vsku in sku_set and v_cost > 0:
                    found_costs[vsku] = v_cost

        if (i // 100 + 1) % 10 == 0:
            print(f"  [BL inv {inventory_id}] Processed {i + 100}/{len(all_product_ids)} products...")
        time.sleep(0.3)

    return found_costs


# ── Printful costs (for PFT- products) ──────────────────────────────

def _get_eur_pln_rate():
    """Get latest EUR/PLN rate from Supabase."""
    rows = db._get("fx_rates", {
        "select": "rate_pln",
        "currency": "eq.EUR",
        "order": "date.desc",
        "limit": "1",
    })
    if rows:
        return float(rows[0]["rate_pln"])
    # Fallback
    try:
        r = requests.get("https://api.nbp.pl/api/exchangerates/rates/a/eur/?format=json")
        if r.status_code == 200:
            return float(r.json()["rates"][0]["mid"])
    except Exception:
        pass
    return 4.30


def _fill_printful_costs(pft_skus):
    """Use the existing printful_costs.py logic to compute costs for PFT- SKUs.

    Returns {sku: (cost_eur, cost_pln)}.
    """
    from .printful_costs import (
        get_pft_products, parse_sku, get_catalog_variant_info,
        get_v2_technique_prices, get_sync_product_costs, compute_costs,
        get_eur_pln_rate,
    )

    print("  [Printful] Fetching PFT- products from Supabase...")
    pft_products = get_pft_products()
    print(f"  [Printful] Found {len(pft_products)} total PFT- products in DB")

    # Parse SKUs to get variant IDs
    variant_ids = set()
    for p in pft_products:
        _, vid = parse_sku(p["sku"])
        if vid is not None:
            variant_ids.add(vid)

    # Filter to only the missing ones for logging, but compute all (needed for parent avg)
    missing_set = set(pft_skus)
    print(f"  [Printful] {len(missing_set)} PFT- SKUs missing costs")
    print(f"  [Printful] {len(variant_ids)} unique catalog variant IDs to look up")

    # Step 1: Get catalog variant info
    print("  [Printful] Fetching catalog variant info...")
    variant_info = get_catalog_variant_info(variant_ids)
    print(f"  [Printful] Got info for {len(variant_info)} variants")

    product_ids = set(v["product_id"] for v in variant_info.values())

    # Step 2: Get v2 technique pricing
    print("  [Printful] Fetching v2 technique pricing...")
    v2_prices, placement_info = get_v2_technique_prices(product_ids)
    print(f"  [Printful] Got pricing for {len(v2_prices)} variants")

    # Step 3: Get sync product costs (exact estimates)
    print("  [Printful] Fetching sync product estimates...")
    sync_costs = get_sync_product_costs()
    print(f"  [Printful] Got exact costs for {len(sync_costs)} sync variants")

    # Step 4: Compute all costs
    sku_costs = compute_costs(pft_products, variant_info, v2_prices, sync_costs)

    # Step 5: Get EUR/PLN rate
    eur_pln = get_eur_pln_rate()
    print(f"  [Printful] EUR/PLN rate: {eur_pln}")

    # Return only the missing ones
    result = {}
    for sku in missing_set:
        if sku in sku_costs:
            cost_eur = sku_costs[sku][0]
            cost_pln = round(cost_eur * eur_pln, 2)
            result[sku] = (cost_eur, cost_pln)

    return result


# ── Nike socks hardcoded cost ───────────────────────────────────────

NIKE_COST_PER_UNIT = 8.33  # 50 PLN / 6-pack


def _is_nike_sock(sku, name):
    """Check if a product is a Nike sock based on SKU pattern or name."""
    sku_upper = sku.upper()
    name_lower = (name or "").lower()
    # Common Nike sock SKU patterns
    if sku_upper.startswith("SX") and any(c.isdigit() for c in sku_upper):
        return True
    if "nike" in name_lower and ("sock" in name_lower or "skarpet" in name_lower):
        return True
    if "everyday cushioned" in name_lower:
        return True
    return False


# ── Main entry point ────────────────────────────────────────────────

def fill_cogs(conn):
    """Main COGS filler: find products without costs and fill from all sources."""
    print("=" * 60)
    print("COGS Filler - Finding and filling missing product costs")
    print("=" * 60)

    # Step 1: Get products with missing COGS
    print("\n[1/5] Fetching products with missing COGS from Supabase...")
    missing = _get_missing_cogs_products()
    print(f"  Found {len(missing)} products without COGS")
    if not missing:
        print("  All products have COGS! Nothing to do.")
        return

    # Categorize
    pft_skus = []
    nike_skus = []
    other_skus = []
    for p in missing:
        sku = p["sku"]
        if sku.startswith("PFT-"):
            pft_skus.append(sku)
        elif _is_nike_sock(sku, p.get("name")):
            nike_skus.append(sku)
        else:
            other_skus.append(sku)

    print(f"  Breakdown: {len(pft_skus)} PFT-, {len(nike_skus)} Nike, {len(other_skus)} other")

    filled = {}       # sku -> (cost_pln, cost_eur, source_label)
    still_missing = []
    cost_history_entries = []
    today = str(date.today())

    # Step 2: Fill from Baselinker warehouse (for non-PFT products)
    non_pft_skus = nike_skus + other_skus
    if non_pft_skus:
        print(f"\n[2/5] Checking Baselinker warehouse (inv_id=30229) for {len(non_pft_skus)} non-PFT SKUs...")
        bl_costs = _fetch_bl_warehouse_costs(30229, non_pft_skus)
        for sku in non_pft_skus:
            if sku in bl_costs:
                cost_pln = bl_costs[sku]
                filled[sku] = (cost_pln, None, "baselinker_warehouse")
                print(f"    FOUND: {sku} = {cost_pln} PLN (from BL warehouse)")
        found_bl = len(bl_costs)
        print(f"  Found {found_bl}/{len(non_pft_skus)} in Baselinker warehouse")
    else:
        print("\n[2/5] No non-PFT SKUs to check in Baselinker warehouse. Skipping.")

    # Step 3: Fill Printful costs (for PFT- products)
    if pft_skus:
        print(f"\n[3/5] Fetching Printful production costs for {len(pft_skus)} PFT- SKUs...")
        try:
            pft_costs = _fill_printful_costs(pft_skus)
            for sku in pft_skus:
                if sku in pft_costs:
                    cost_eur, cost_pln = pft_costs[sku]
                    filled[sku] = (cost_pln, cost_eur, "printful_api")
            print(f"  Filled {len(pft_costs)}/{len(pft_skus)} PFT- products from Printful API")
        except Exception as e:
            print(f"  ERROR: Failed to fetch Printful costs: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("\n[3/5] No PFT- SKUs missing. Skipping Printful lookup.")

    # Step 4: Fill Nike socks with hardcoded cost
    unfilled_nike = [s for s in nike_skus if s not in filled]
    if unfilled_nike:
        print(f"\n[4/5] Applying Nike socks hardcoded cost ({NIKE_COST_PER_UNIT} PLN/unit) to {len(unfilled_nike)} SKUs...")
        for sku in unfilled_nike:
            filled[sku] = (NIKE_COST_PER_UNIT, None, "nike_hardcoded")
            print(f"    SET: {sku} = {NIKE_COST_PER_UNIT} PLN (Nike socks hardcoded)")
    else:
        print("\n[4/5] No unfilled Nike socks. Skipping.")

    # Step 5: Update Supabase with filled costs
    print(f"\n[5/5] Updating Supabase with {len(filled)} filled costs...")
    updated = 0
    errors = 0
    for sku, (cost_pln, cost_eur, source_label) in sorted(filled.items()):
        try:
            _update_product_cost(sku, cost_pln, cost_eur=cost_eur, source=None)
            updated += 1
            cost_history_entries.append({
                "sku": sku,
                "cost_eur": cost_eur,
                "cost_pln": round(cost_pln, 2),
                "source": f"cogs_filler_{source_label}",
                "notes": f"Auto-filled by cogs_filler ({source_label})",
                "effective_from": today,
            })
        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"    ERROR updating {sku}: {e}")

    # Log to cost_history
    _log_cost_history(cost_history_entries)

    # Report still missing
    all_missing_skus = set(p["sku"] for p in missing)
    still_missing = sorted(all_missing_skus - set(filled.keys()))

    print(f"\n{'=' * 60}")
    print(f"COGS Filler Results:")
    print(f"  Started with:    {len(missing)} products missing COGS")
    print(f"  Filled:          {updated} products")
    print(f"  Errors:          {errors}")
    print(f"  Still missing:   {len(still_missing)} products")
    if still_missing:
        print(f"\n  Products still without COGS:")
        for sku in still_missing[:30]:
            p = next((x for x in missing if x["sku"] == sku), {})
            print(f"    {sku} | {p.get('name', '')[:50]} | source={p.get('source', '')}")
        if len(still_missing) > 30:
            print(f"    ... and {len(still_missing) - 30} more")
    print(f"{'=' * 60}")

    return {
        "total_missing": len(missing),
        "filled": updated,
        "errors": errors,
        "still_missing": len(still_missing),
    }
