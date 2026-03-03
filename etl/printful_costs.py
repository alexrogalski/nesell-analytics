"""Fetch production costs from Printful API and update Supabase products table.

Strategy:
1. Get all PFT- products from Supabase to extract catalog variant IDs
2. Look up each unique variant's catalog product_id via Printful v1 API
3. Get v2 pricing per catalog product (includes per-variant technique prices)
4. For the one sync product in the store, use /orders/estimate-costs for exact costs
5. Map costs back to DB SKUs and update cost_eur + cost_pln (via EUR/PLN FX rate)
6. Log cost changes to cost_history table
"""

import requests
import json
import time
import sys
from pathlib import Path
from datetime import date
from collections import defaultdict

# --- Config ---
KEYS_DIR = Path.home() / ".keys"


def load_env(path):
    vals = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip().replace("\r", "")
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip()
    return vals


# Printful
_pf = load_env(KEYS_DIR / "printful.env")
PRINTFUL_TOKEN = _pf.get("PRINTFUL_API_TOKEN", "")
PRINTFUL_STORE_ID = _pf.get("PRINTFUL_STORE_ID", "15269225")

# Supabase
_supa = load_env(Path(__file__).parent.parent / ".env")
SUPABASE_URL = _supa.get("SUPABASE_URL", "")
SUPABASE_KEY = _supa.get("SUPABASE_KEY", "")

# --- API Helpers ---

PF_HEADERS = {
    "Authorization": f"Bearer {PRINTFUL_TOKEN}",
    "X-PF-Store-Id": PRINTFUL_STORE_ID,
    "Content-Type": "application/json",
}

SUPA_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}


def pf_get(path, params=None):
    """GET request to Printful API with retry."""
    for attempt in range(3):
        r = requests.get(
            f"https://api.printful.com{path}",
            headers=PF_HEADERS,
            params=params or {},
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  [Rate limit] Waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code == 200:
            return r.json()
        print(f"  [Printful] GET {path} -> {r.status_code}: {r.text[:200]}")
        return None
    return None


def pf_post(path, payload):
    """POST request to Printful API with retry."""
    for attempt in range(3):
        r = requests.post(
            f"https://api.printful.com{path}",
            headers=PF_HEADERS,
            json=payload,
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  [Rate limit] Waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code == 200:
            return r.json()
        print(f"  [Printful] POST {path} -> {r.status_code}: {r.text[:200]}")
        return None
    return None


def supa_get(table, params=None):
    """GET request to Supabase REST API."""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=SUPA_HEADERS,
        params=params or {},
    )
    if r.status_code != 200:
        raise Exception(f"Supabase GET {table}: {r.status_code} {r.text[:200]}")
    return r.json()


def supa_patch(table, match_params, data):
    """PATCH request to Supabase REST API."""
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=SUPA_HEADERS,
        json=data,
        params=match_params,
    )
    if r.status_code not in (200, 204):
        raise Exception(f"Supabase PATCH {table}: {r.status_code} {r.text[:200]}")
    return r.json() if r.text else []


def supa_post(table, data, on_conflict=None):
    """POST (upsert) to Supabase REST API."""
    headers = dict(SUPA_HEADERS)
    params = {}
    if on_conflict:
        headers["Prefer"] = "return=representation,resolution=merge-duplicates"
        params["on_conflict"] = on_conflict
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=headers,
        json=data,
        params=params,
    )
    if r.status_code not in (200, 201):
        raise Exception(f"Supabase POST {table}: {r.status_code} {r.text[:200]}")
    return r.json()


# --- Main Logic ---


def get_pft_products():
    """Get all PFT- products from Supabase."""
    all_products = []
    offset = 0
    while True:
        rows = supa_get("products", {
            "select": "sku,name,cost_eur,cost_pln,source,printful_product_id,printful_variant_id",
            "sku": "like.PFT-%",
            "order": "sku",
            "limit": "1000",
            "offset": str(offset),
        })
        all_products.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return all_products


def parse_sku(sku):
    """Parse PFT-{bl_product_id}-{catalog_variant_id} SKU.

    Returns (bl_product_id, catalog_variant_id) or (bl_product_id, None) for parents.
    """
    parts = sku.split("-")
    if len(parts) == 3:
        try:
            return parts[1], int(parts[2])
        except ValueError:
            return parts[1], None
    elif len(parts) == 2:
        return parts[1], None
    return None, None


def get_catalog_variant_info(variant_ids):
    """Get catalog product_id and blank price for each variant ID via Printful v1 API."""
    info = {}
    total = len(variant_ids)
    for i, vid in enumerate(sorted(variant_ids)):
        data = pf_get(f"/products/variant/{vid}")
        if data and data.get("result"):
            v = data["result"]["variant"]
            p = data["result"]["product"]
            info[vid] = {
                "product_id": v["product_id"],
                "blank_price": float(v["price"]),
                "product_type": p.get("type", ""),
                "variant_name": v["name"],
            }
        else:
            print(f"  WARNING: Could not fetch variant {vid}")
        if (i + 1) % 10 == 0:
            print(f"  Fetched {i+1}/{total} catalog variants...")
        time.sleep(0.25)  # rate limit
    return info


def get_v2_technique_prices(product_ids):
    """Get per-variant technique prices from Printful v2 catalog pricing API.

    Returns {variant_id: {"technique": price, ...}} for each product.
    """
    prices = {}  # variant_id -> {technique_key: price_eur}
    placement_info = {}  # product_id -> [{id, type, price}]

    for pid in sorted(product_ids):
        data = pf_get(f"/v2/catalog-products/{pid}/prices")
        if not data or "data" not in data:
            print(f"  WARNING: No v2 pricing for product {pid}")
            continue

        product_data = data["data"].get("product", {})
        placements = product_data.get("placements", [])
        placement_info[pid] = [
            {"id": p["id"], "type": p.get("type", ""), "price": float(p.get("price", 0))}
            for p in placements
        ]

        variants = data["data"].get("variants", [])
        for v in variants:
            vid = v["id"]
            techniques = {}
            for t in v.get("techniques", []):
                techniques[t["technique_key"]] = float(t["price"])
                if t.get("discounted_price") and float(t["discounted_price"]) < float(t["price"]):
                    techniques[t["technique_key"]] = float(t["discounted_price"])
            prices[vid] = techniques

        print(f"  Product {pid}: {len(variants)} variants, {len(placements)} placements")
        time.sleep(0.25)

    return prices, placement_info


def get_sync_product_costs():
    """Get actual production costs for sync products via estimate endpoint.

    Returns {catalog_variant_id: cost_eur}.
    """
    costs = {}

    # List all sync products
    offset = 0
    sync_products = []
    while True:
        data = pf_get("/store/products", {"limit": 100, "offset": offset})
        if not data or not data.get("result"):
            break
        prods = data["result"]
        sync_products.extend(prods)
        paging = data.get("paging", {})
        if offset + len(prods) >= paging.get("total", 0):
            break
        offset += len(prods)

    print(f"  Found {len(sync_products)} sync products in Printful store")

    for sp in sync_products:
        sp_id = sp["id"]
        detail = pf_get(f"/store/products/{sp_id}")
        if not detail or not detail.get("result"):
            continue

        sync_variants = detail["result"].get("sync_variants", [])
        print(f"  Sync product {sp_id} ({sp['name']}): {len(sync_variants)} variants")

        for sv in sync_variants:
            sv_id = sv["id"]
            catalog_vid = sv["variant_id"]

            estimate = pf_post("/orders/estimate-costs", {
                "recipient": {
                    "address1": "Marszalkowska 1",
                    "city": "Warsaw",
                    "country_code": "PL",
                    "zip": "00-001",
                },
                "items": [{"sync_variant_id": sv_id, "quantity": 1}],
            })

            if estimate and estimate.get("result"):
                cost_data = estimate["result"].get("costs", {})
                subtotal = cost_data.get("subtotal", 0)
                costs[catalog_vid] = float(subtotal)
                print(f"    Variant {catalog_vid}: {subtotal} EUR (exact estimate)")
            else:
                print(f"    WARNING: Could not estimate variant {catalog_vid}")

            time.sleep(0.3)

    return costs


def get_eur_pln_rate():
    """Get latest EUR/PLN rate from Supabase fx_rates."""
    rows = supa_get("fx_rates", {
        "select": "rate_pln",
        "currency": "eq.EUR",
        "order": "date.desc",
        "limit": "1",
    })
    if rows:
        return float(rows[0]["rate_pln"])
    # Fallback: fetch from NBP API
    try:
        r = requests.get("https://api.nbp.pl/api/exchangerates/rates/a/eur/?format=json")
        if r.status_code == 200:
            return float(r.json()["rates"][0]["mid"])
    except Exception:
        pass
    print("  WARNING: Using fallback EUR/PLN rate 4.30")
    return 4.30


def compute_costs(pft_products, variant_info, v2_prices, sync_costs):
    """Compute cost_eur for each PFT- variant SKU.

    Priority:
    1. Sync product estimate (most accurate, from orders/estimate-costs)
    2. v2 technique price (catalog price = blank + technique, for the primary technique)
    3. Blank price from v1 API (fallback, just the blank product cost)

    For parent SKUs: use average of child variant costs.
    """
    sku_costs = {}  # sku -> cost_eur
    parent_children = defaultdict(list)  # parent_sku -> [child_sku]

    # First pass: compute variant (child) costs
    for p in pft_products:
        sku = p["sku"]
        bl_pid, catalog_vid = parse_sku(sku)

        if catalog_vid is None:
            # Parent SKU — skip for now, compute after children
            continue

        cost = None
        source = None

        # Priority 1: Sync product estimate
        if catalog_vid in sync_costs:
            cost = sync_costs[catalog_vid]
            source = "sync_estimate"

        # Priority 2: v2 technique price
        elif catalog_vid in v2_prices:
            techniques = v2_prices[catalog_vid]
            # Pick the primary technique based on product type
            vi = variant_info.get(catalog_vid, {})
            ptype = vi.get("product_type", "").upper()

            if "EMBROIDERY" in ptype and "embroidery" in techniques:
                cost = techniques["embroidery"]
                source = "v2_embroidery"
            elif "MUG" in ptype and "sublimation" in techniques:
                cost = techniques["sublimation"]
                source = "v2_sublimation"
            elif "T-SHIRT" in ptype or "TOTE" in ptype.upper():
                # Tote bags — prefer DTG for print, embroidery for embroidered items
                if "dtg" in techniques:
                    cost = techniques["dtg"]
                    source = "v2_dtg"
                elif "embroidery" in techniques:
                    cost = techniques["embroidery"]
                    source = "v2_embroidery"

            # Fallback: pick first technique
            if cost is None and techniques:
                first_key = sorted(techniques.keys())[0]
                cost = techniques[first_key]
                source = f"v2_{first_key}"

        # Priority 3: blank price
        if cost is None and catalog_vid in variant_info:
            cost = variant_info[catalog_vid]["blank_price"]
            source = "blank_only"

        if cost is not None:
            sku_costs[sku] = (round(cost, 2), source)
            # Track parent-child relationship
            parent_sku = f"PFT-{bl_pid}"
            parent_children[parent_sku].append(cost)

    # Second pass: compute parent costs as average of children
    for p in pft_products:
        sku = p["sku"]
        _, catalog_vid = parse_sku(sku)
        if catalog_vid is not None:
            continue  # not a parent

        children_costs = parent_children.get(sku, [])
        if children_costs:
            avg_cost = round(sum(children_costs) / len(children_costs), 2)
            sku_costs[sku] = (avg_cost, "parent_avg")

    return sku_costs


def update_supabase(sku_costs, eur_pln_rate):
    """Update products table and log to cost_history."""
    updated = 0
    skipped = 0
    errors = 0
    cost_history_rows = []
    today = str(date.today())

    total = len(sku_costs)
    for i, (sku, (cost_eur, source)) in enumerate(sorted(sku_costs.items())):
        cost_pln = round(cost_eur * eur_pln_rate, 2)

        try:
            result = supa_patch("products", {"sku": f"eq.{sku}"}, {
                "cost_eur": cost_eur,
                "cost_pln": cost_pln,
                "source": "printful",
            })
            updated += 1

            cost_history_rows.append({
                "sku": sku,
                "cost_eur": cost_eur,
                "cost_pln": cost_pln,
                "source": f"printful_{source}",
                "notes": f"Auto-fetched from Printful API ({source})",
                "effective_from": today,
            })
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  ERROR updating {sku}: {e}")

        if (i + 1) % 50 == 0:
            print(f"  Updated {i+1}/{total} products...")

    # Batch insert cost history
    if cost_history_rows:
        for i in range(0, len(cost_history_rows), 500):
            chunk = cost_history_rows[i:i + 500]
            try:
                supa_post("cost_history", chunk)
            except Exception as e:
                print(f"  WARNING: cost_history insert error: {e}")

    return updated, skipped, errors


def main():
    print("=" * 60)
    print("Printful Cost Sync -> Supabase")
    print("=" * 60)

    # Step 1: Get PFT- products from DB
    print("\n[1/6] Fetching PFT- products from Supabase...")
    pft_products = get_pft_products()
    print(f"  Found {len(pft_products)} PFT- products")

    # Parse SKUs to get unique catalog variant IDs
    variant_ids = set()
    for p in pft_products:
        _, vid = parse_sku(p["sku"])
        if vid is not None:
            variant_ids.add(vid)
    print(f"  Unique catalog variant IDs: {len(variant_ids)}")

    # Step 2: Get catalog variant info (product_id + blank price)
    print("\n[2/6] Fetching catalog variant info from Printful v1 API...")
    variant_info = get_catalog_variant_info(variant_ids)
    print(f"  Got info for {len(variant_info)} variants")

    product_ids = set(v["product_id"] for v in variant_info.values())
    print(f"  Unique catalog products: {sorted(product_ids)}")

    # Step 3: Get v2 technique pricing per product
    print("\n[3/6] Fetching v2 technique pricing from Printful...")
    v2_prices, placement_info = get_v2_technique_prices(product_ids)
    print(f"  Got pricing for {len(v2_prices)} variants across {len(product_ids)} products")

    # Step 4: Get exact costs for sync products via estimate
    print("\n[4/6] Fetching exact costs for sync products via estimate...")
    sync_costs = get_sync_product_costs()
    print(f"  Got exact costs for {len(sync_costs)} sync variants")

    # Step 5: Compute final costs
    print("\n[5/6] Computing production costs...")
    sku_costs = compute_costs(pft_products, variant_info, v2_prices, sync_costs)

    # Print summary by source
    source_counts = defaultdict(int)
    source_examples = defaultdict(list)
    for sku, (cost, source) in sku_costs.items():
        source_counts[source] += 1
        if len(source_examples[source]) < 2:
            source_examples[source].append(f"{sku}={cost}")

    print(f"  Total costs computed: {len(sku_costs)}/{len(pft_products)}")
    for src, cnt in sorted(source_counts.items()):
        examples = ", ".join(source_examples[src])
        print(f"    {src}: {cnt} SKUs (e.g. {examples})")

    # Step 6: Get EUR/PLN rate and update DB
    print("\n[6/6] Updating Supabase...")
    eur_pln = get_eur_pln_rate()
    print(f"  EUR/PLN rate: {eur_pln}")

    updated, skipped, errors = update_supabase(sku_costs, eur_pln)

    print(f"\n{'=' * 60}")
    print(f"DONE: {updated} products updated, {skipped} skipped, {errors} errors")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
