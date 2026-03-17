"""
Temu Product Feed Builder v2
Pulls products from Baselinker inventory 30229, cross-references with order history,
and builds a top-20 product feed ready for Temu upload.

Key insight: average_cost is only in getInventoryProductsData (detailed), not in list.
Also uses price group 50910 ("Średnia cena zakupu netto PLN") as cost fallback.
"""

import requests
import json
import time
import csv
import os
from datetime import datetime, timedelta
from collections import defaultdict

# --- Config ---
TOKEN = ""
URL = "https://api.baselinker.com/connector.php"
INVENTORY_ID = 30229
COST_PRICE_GROUP = "50910"  # "Średnia cena zakupu netto PLN"
TEMU_COMMISSION = 0.12
SHIPPING_COST = 15.0
MARGIN_THRESHOLD = 0.15
MARKUP = 0.20

# Load token
env_path = os.path.expanduser("~/.keys/baselinker.env")
with open(env_path) as f:
    for line in f:
        if line.startswith("BASELINKER_API_TOKEN="):
            TOKEN = line.strip().split("=", 1)[1]

_last_request = 0.0

def bl_api(method, params=None):
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < 0.65:
        time.sleep(0.65 - elapsed)
    resp = requests.post(URL, headers={"X-BLToken": TOKEN}, data={
        "method": method, "parameters": json.dumps(params or {})
    })
    _last_request = time.time()
    data = resp.json()
    if data.get("status") == "ERROR":
        code = data.get("error_code", "UNKNOWN")
        msg = data.get("error_message", "Unknown error")
        if code == "TOO_MANY_REQUESTS":
            print(f"  Rate limited, waiting 60s...")
            time.sleep(60)
            return bl_api(method, params)
        raise RuntimeError(f"Baselinker API error [{code}]: {msg}")
    return data


# ========== STEP 1: Build SKU -> product_id map from inventory ==========
print("=" * 60)
print("STEP 1: Building SKU map from inventory 30229...")
print("=" * 60)

all_products = {}
page = 1
while True:
    data = bl_api("getInventoryProductsList", {
        "inventory_id": INVENTORY_ID,
        "include_variants": True,
        "page": page
    })
    products = data.get("products", {})
    all_products.update(products)
    print(f"  Page {page}: {len(products)} products (total: {len(all_products)})")
    if len(products) < 1000:
        break
    page += 1

# Build SKU -> product_id map (both parent and variant SKUs)
sku_to_pid = {}
variant_sku_to_parent = {}
for pid, p in all_products.items():
    sku = p.get("sku", "")
    parent_id = p.get("parent_id", 0) or 0
    if sku:
        sku_to_pid[sku] = pid
        if parent_id != 0:
            variant_sku_to_parent[sku] = str(parent_id)

print(f"\nTotal products: {len(all_products)}")
print(f"SKUs mapped: {len(sku_to_pid)} (variants pointing to parent: {len(variant_sku_to_parent)})")


# ========== STEP 2: Fetch orders from last 90 days ==========
print("\n" + "=" * 60)
print("STEP 2: Fetching orders from last 90 days...")
print("=" * 60)

date_90_days_ago = int((datetime.now() - timedelta(days=90)).timestamp())
all_orders = []
params = {"date_confirmed_from": date_90_days_ago, "get_unconfirmed_orders": False}

while True:
    data = bl_api("getOrders", params)
    orders = data.get("orders", [])
    all_orders.extend(orders)
    print(f"  Batch: {len(orders)} orders (total: {len(all_orders)})")
    if len(orders) < 100:
        break
    params["date_confirmed_from"] = orders[-1]["date_confirmed"] + 1

print(f"\nTotal orders (last 90 days): {len(all_orders)}")


# ========== STEP 3: Count quantity sold per SKU ==========
print("\n" + "=" * 60)
print("STEP 3: Counting quantity sold per SKU...")
print("=" * 60)

sku_sales = defaultdict(int)
for order in all_orders:
    for item in order.get("products", []):
        sku = item.get("sku", "").strip()
        qty = int(item.get("quantity", 0))
        if sku and qty > 0:
            sku_sales[sku] += qty

top_skus = sorted(sku_sales.items(), key=lambda x: x[1], reverse=True)
print(f"Total unique SKUs sold: {len(top_skus)}")

# Group variant SKUs under their parent for ranking
parent_sales = defaultdict(lambda: {"qty": 0, "variant_skus": []})
for sku, qty in top_skus:
    parent_pid = variant_sku_to_parent.get(sku)
    if parent_pid:
        parent_sales[parent_pid]["qty"] += qty
        parent_sales[parent_pid]["variant_skus"].append((sku, qty))
    else:
        pid = sku_to_pid.get(sku)
        if pid:
            parent_sales[pid]["qty"] += qty
            parent_sales[pid]["variant_skus"].append((sku, qty))

# Also track SKUs without inventory match
unmatched = [(sku, qty) for sku, qty in top_skus if sku not in sku_to_pid]
print(f"SKUs with inventory match: {len(top_skus) - len(unmatched)}")
print(f"SKUs without inventory match: {len(unmatched)}")
if unmatched[:5]:
    print(f"  Top unmatched: {unmatched[:5]}")

# Rank parents by total quantity
ranked_parents = sorted(parent_sales.items(), key=lambda x: x[1]["qty"], reverse=True)
print(f"\nTop 25 parent products by quantity:")
for pid, info in ranked_parents[:25]:
    name = all_products.get(pid, {}).get("name", "?")
    print(f"  PID={pid}: {info['qty']} sold | {name[:60]}")


# ========== STEP 4: Select top 20 and fetch detailed data ==========
print("\n" + "=" * 60)
print("STEP 4: Fetching detailed data for top candidates...")
print("=" * 60)

# Take top 40 candidates (we'll filter down to 20 after getting costs)
candidate_pids = [int(pid) for pid, _ in ranked_parents[:40]]
print(f"Fetching detailed data for {len(candidate_pids)} candidates...")

detailed_products = {}
for i in range(0, len(candidate_pids), 50):
    batch = candidate_pids[i:i+50]
    data = bl_api("getInventoryProductsData", {
        "inventory_id": INVENTORY_ID,
        "products": batch
    })
    detailed_products.update(data.get("products", {}))
    print(f"  Fetched batch {i//50+1}: {len(batch)} products")

# Check cost availability
print(f"\nCost analysis:")
for pid_str, p in detailed_products.items():
    avg_cost = p.get("average_cost", 0) or 0
    price_cost = p.get("prices", {}).get(COST_PRICE_GROUP, 0) or 0
    cost = float(avg_cost) if float(avg_cost) > 0 else float(price_cost)
    name = p.get("text_fields", {}).get("name|pl", p.get("text_fields", {}).get("name", "?"))
    if cost > 0:
        print(f"  PID={pid_str}: cost={cost:.2f} (avg_cost={avg_cost}, price_group={price_cost}) | {str(name)[:50]}")


# ========== STEP 5: Also fetch stock levels ==========
print("\nFetching stock levels...")
all_stock = {}
page = 1
while True:
    data = bl_api("getInventoryProductsStock", {
        "inventory_id": INVENTORY_ID,
        "page": page
    })
    stock_data = data.get("products", {})
    all_stock.update(stock_data)
    if len(stock_data) < 1000:
        break
    page += 1
print(f"Stock data for {len(all_stock)} products")


# ========== STEP 6: Build the feed ==========
print("\n" + "=" * 60)
print("STEP 6: Building Temu product feed...")
print("=" * 60)

feed = []
excluded_feed = []

for pid_str, info in ranked_parents:
    if len(feed) >= 20 and len(excluded_feed) >= 10:
        break  # Enough data

    p_detail = detailed_products.get(str(pid_str), detailed_products.get(int(pid_str), {}))
    p_list = all_products.get(str(pid_str), {})

    if not p_detail:
        continue

    # Cost: prefer average_cost, fallback to price group 50910
    avg_cost_raw = p_detail.get("average_cost", 0) or 0
    price_group_cost = p_detail.get("prices", {}).get(COST_PRICE_GROUP, 0) or 0
    avg_cost = float(avg_cost_raw) if float(avg_cost_raw) > 0 else float(price_group_cost)

    if avg_cost <= 0:
        continue  # Skip products without cost data

    # Name
    text_fields = p_detail.get("text_fields", {})
    name_pl = text_fields.get("name|pl", text_fields.get("name", p_list.get("name", "")))
    name_en = text_fields.get("name|en", "")

    # EAN
    ean = p_detail.get("ean", p_list.get("ean", ""))

    # Price: 20% markup over average_cost (gross, includes 23% VAT for PL)
    sell_price = round(avg_cost * (1 + MARKUP), 2)

    # Stock - structure: {pid: {"product_id": ..., "stock": {warehouse_id: qty}}}
    stock_entry = all_stock.get(str(pid_str), {})
    stock_dict = stock_entry.get("stock", {}) if isinstance(stock_entry, dict) else {}
    total_stock = sum(int(v) for v in stock_dict.values() if isinstance(v, (int, float, str)))

    # Images
    images = p_detail.get("images", {})
    image_urls = [url for key, url in sorted(images.items(), key=lambda x: str(x[0])) if url]

    # Description
    desc_pl = text_fields.get("description|pl", text_fields.get("description", ""))
    desc_en = text_fields.get("description|en", "")

    # Features
    features = text_fields.get("features", {})
    if isinstance(features, str):
        features = {}

    # SKU
    product_sku = p_list.get("sku", "")

    # Weight
    weight = p_detail.get("weight", p_list.get("weight", 0)) or 0

    # --- Margin calculation ---
    revenue_after_commission = sell_price * (1 - TEMU_COMMISSION)
    profit = revenue_after_commission - avg_cost - SHIPPING_COST
    temu_margin_pct = round((profit / sell_price) * 100, 2) if sell_price > 0 else 0
    margin_ok = temu_margin_pct >= (MARGIN_THRESHOLD * 100)

    item = {
        "sku": product_sku,
        "name_pl": name_pl,
        "name_en": name_en,
        "ean": ean,
        "sell_price_pln": sell_price,
        "average_cost_pln": avg_cost,
        "stock": total_stock,
        "qty_sold_90d": info["qty"],
        "image_1": image_urls[0] if len(image_urls) > 0 else "",
        "image_2": image_urls[1] if len(image_urls) > 1 else "",
        "image_3": image_urls[2] if len(image_urls) > 2 else "",
        "image_4": image_urls[3] if len(image_urls) > 3 else "",
        "image_5": image_urls[4] if len(image_urls) > 4 else "",
        "description_pl": desc_pl[:500] if desc_pl else "",
        "description_en": desc_en[:500] if desc_en else "",
        "features": json.dumps(features, ensure_ascii=False) if features else "",
        "weight_kg": weight,
        "temu_commission_pct": TEMU_COMMISSION * 100,
        "shipping_cost_pln": SHIPPING_COST,
        "temu_margin_pct": temu_margin_pct,
        "margin_ok": margin_ok,
        "exclude_reason": "" if margin_ok else f"Margin {temu_margin_pct}% < {MARGIN_THRESHOLD*100}% threshold"
    }

    if margin_ok and len(feed) < 20:
        feed.append(item)
        print(f"  [OK]      {product_sku}: {name_pl[:45]}... | cost={avg_cost:.2f} sell={sell_price:.2f} margin={temu_margin_pct:.1f}% qty={info['qty']}")
    else:
        excluded_feed.append(item)
        print(f"  [EXCLUDE] {product_sku}: {name_pl[:45]}... | cost={avg_cost:.2f} sell={sell_price:.2f} margin={temu_margin_pct:.1f}% qty={info['qty']}")


# ========== STEP 7: Save files ==========
print("\n" + "=" * 60)
print("STEP 7: Saving feed files...")
print("=" * 60)

output_dir = os.path.expanduser("~/nesell-analytics/reports")
os.makedirs(output_dir, exist_ok=True)

# JSON - full data including excluded
json_path = os.path.join(output_dir, "temu-product-feed.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump({
        "generated_at": datetime.now().isoformat(),
        "inventory_id": INVENTORY_ID,
        "total_orders_analyzed": len(all_orders),
        "date_range_days": 90,
        "temu_commission_pct": TEMU_COMMISSION * 100,
        "shipping_cost_pln": SHIPPING_COST,
        "margin_threshold_pct": MARGIN_THRESHOLD * 100,
        "markup_over_cost_pct": MARKUP * 100,
        "cost_source": f"average_cost from getInventoryProductsData, fallback to price_group {COST_PRICE_GROUP}",
        "products_included": feed,
        "products_excluded": excluded_feed,
        "summary": {
            "total_candidates_with_cost": len(feed) + len(excluded_feed),
            "included": len(feed),
            "excluded": len(excluded_feed),
            "avg_margin_included": round(sum(p["temu_margin_pct"] for p in feed) / len(feed), 2) if feed else 0,
        }
    }, f, indent=2, ensure_ascii=False)
print(f"  JSON saved: {json_path}")

# CSV - only included products (Temu-ready)
csv_path = os.path.join(output_dir, "temu-product-feed.csv")
if feed:
    fieldnames = list(feed[0].keys())
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(feed)
    print(f"  CSV saved: {csv_path}")
else:
    print("  WARNING: No products met margin threshold for CSV!")
    # Save all candidates as CSV anyway for review
    csv_path = os.path.join(output_dir, "temu-product-feed-all-candidates.csv")
    all_items = feed + excluded_feed
    if all_items:
        fieldnames = list(all_items[0].keys())
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_items)
        print(f"  CSV (all candidates) saved: {csv_path}")


# ========== SUMMARY ==========
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Orders analyzed: {len(all_orders)} (last 90 days)")
print(f"Unique SKUs sold: {len(sku_sales)}")
print(f"Parent products ranked: {len(ranked_parents)}")
print(f"\nFeed results:")
print(f"  Included (margin >= {MARGIN_THRESHOLD*100}%): {len(feed)}")
print(f"  Excluded (margin < {MARGIN_THRESHOLD*100}%): {len(excluded_feed)}")

if feed:
    print(f"\n--- TEMU-READY PRODUCTS ({len(feed)}) ---")
    for i, p in enumerate(feed, 1):
        print(f"  {i:2d}. {p['sku']:<25s} margin={p['temu_margin_pct']:5.1f}% | cost={p['average_cost_pln']:7.2f} sell={p['sell_price_pln']:7.2f} | stock={p['stock']:4d} | sold={p['qty_sold_90d']:3d} | {p['name_pl'][:50]}")

if excluded_feed:
    print(f"\n--- EXCLUDED ({len(excluded_feed)}) ---")
    for p in excluded_feed:
        print(f"  {p['sku']:<25s} margin={p['temu_margin_pct']:5.1f}% | cost={p['average_cost_pln']:7.2f} sell={p['sell_price_pln']:7.2f} | {p['name_pl'][:50]}")

# English translation status
has_en_name = sum(1 for p in feed if p.get("name_en"))
has_en_desc = sum(1 for p in feed if p.get("description_en"))
print(f"\nTranslation status (included products):")
print(f"  English names: {has_en_name}/{len(feed)}")
print(f"  English descriptions: {has_en_desc}/{len(feed)}")
if has_en_name < len(feed):
    print("  NOTE: Temu requires English product data. Missing translations need to be prepared.")
    missing_en = [p["sku"] for p in feed if not p.get("name_en")]
    print(f"  SKUs needing EN translation: {missing_en}")
