"""BL Repricing — price floor + target price enforcement.

Reads average_cost from BL inventory, calculates minimum viable price
and target price per SKU, then updates price groups 31059 (EUR) and 30673 (PLN).

Strategy:
  floor_eur  = (avg_cost_pln / eur_rate) * FLOOR_MULT    [default 1.5x — covers fees+shipping]
  target_eur = (avg_cost_pln / eur_rate) * TARGET_MULT   [default 1.9x — existing formula]

  If current price < floor  → set to floor   (price protection, always)
  If current price < target → log suggestion (don't auto-raise above floor)
  Never lowers prices automatically.

Price groups:
  31059 = "Głowna EUR"  (Amazon EU — all 8 markets)
  30673 = "Główna PLN"  (Allegro / fallback)

Usage:
  python3.11 -m etl.bl_repricing                    # dry-run
  python3.11 -m etl.bl_repricing --apply             # push to BL
  python3.11 -m etl.bl_repricing --inventory 52954   # Printful inventory
  python3.11 -m etl.bl_repricing --floor 1.4         # custom floor multiplier
"""
import argparse
import json
import math
import time
from datetime import datetime

from . import config, db
from .baselinker import bl_api
from .discord_notify import send_embed, GREEN, YELLOW, RED, ORANGE

# ── Config ──────────────────────────────────────────────────────────────────

INVENTORY_ID    = 30229       # test-exportivo (physical stock)
PRICE_GROUP_EUR = 31059       # "Głowna EUR" — Amazon EU
PRICE_GROUP_PLN = 30673       # "Główna PLN"  — Allegro
FLOOR_MULT      = 1.5         # minimum: cost * 1.5 (covers 15.45% fees + shipping + ~20% margin)
TARGET_MULT     = 1.9         # target:  cost * 1.9 (existing netto formula)
MAX_RAISE_PCT   = 0.20        # never raise more than 20% per run
MIN_COST_PLN    = 5.0         # skip products with cost < 5 PLN (noise / samples)

# ── Helpers ──────────────────────────────────────────────────────────────────

def get_eur_rate() -> float:
    """Get latest EUR/PLN rate from Supabase."""
    rows = db._get("fx_rates", {
        "select": "rate_pln",
        "currency": "eq.EUR",
        "order": "date.desc",
        "limit": "1",
    })
    if rows:
        return float(rows[0]["rate_pln"])
    # Fallback to safe estimate
    print("  [WARN] No EUR rate in DB — using fallback 4.25")
    return 4.25


def get_products_page(inventory_id: int, page: int) -> dict:
    """Fetch one page of products (1000 per page)."""
    return bl_api("getInventoryProductsList", {
        "inventory_id": inventory_id,
        "page": page,
    }).get("products", {})


def get_product_details(inventory_id: int, product_ids: list) -> dict:
    """Fetch detailed data for up to 1000 products at once."""
    return bl_api("getInventoryProductsData", {
        "inventory_id": inventory_id,
        "products": product_ids,
    }).get("products", {})


def push_prices(inventory_id: int, updates: dict, dry_run: bool):
    """Push price updates to BL in batches of 1000."""
    if not updates:
        return 0

    if dry_run:
        print(f"  [DRY-RUN] Would update {len(updates)} products")
        return len(updates)

    pushed = 0
    items = list(updates.items())
    for i in range(0, len(items), 1000):
        batch = dict(items[i:i + 1000])
        bl_api("updateInventoryProductsPrices", {
            "inventory_id": inventory_id,
            "products": batch,
        })
        pushed += len(batch)
        if len(items) > 1000:
            time.sleep(1)

    return pushed


# ── Main ─────────────────────────────────────────────────────────────────────

def run(inventory_id: int = INVENTORY_ID, floor_mult: float = FLOOR_MULT,
        target_mult: float = TARGET_MULT, apply: bool = False):

    print(f"\n{'='*60}")
    print(f"BL Repricing — {'APPLY' if apply else 'DRY-RUN'}")
    print(f"Inventory: {inventory_id}  Floor: {floor_mult}x  Target: {target_mult}x")
    print(f"{'='*60}")

    eur_rate = get_eur_rate()
    print(f"EUR/PLN rate: {eur_rate}")

    # Pull all products (paginated)
    all_products = {}
    page = 1
    while True:
        batch = get_products_page(inventory_id, page)
        all_products.update(batch)
        print(f"  Page {page}: {len(batch)} products (total: {len(all_products)})")
        if len(batch) < 1000:
            break
        page += 1
        time.sleep(0.7)

    product_ids = list(all_products.keys())
    print(f"\nFetching detailed data for {len(product_ids)} products...")

    # Fetch details in chunks of 200 (API limit for getInventoryProductsData)
    details = {}
    for i in range(0, len(product_ids), 200):
        chunk = product_ids[i:i + 200]
        chunk_data = get_product_details(inventory_id, [int(p) for p in chunk])
        details.update(chunk_data)
        time.sleep(0.7)

    # Analyze and build updates
    price_updates = {}      # product_id -> {group_id: new_price}
    floor_fixes   = []      # products raised to floor
    suggestions   = []      # products below target (not auto-raised)
    skipped_no_cost = 0

    for pid, prod in details.items():
        sku          = prod.get("sku") or pid
        avg_cost_pln = float(prod.get("average_cost") or 0)
        prices       = prod.get("prices") or {}

        cur_eur = float(prices.get(str(PRICE_GROUP_EUR)) or 0)
        cur_pln = float(prices.get(str(PRICE_GROUP_PLN)) or 0)

        # Skip products with no cost data
        if avg_cost_pln < MIN_COST_PLN:
            skipped_no_cost += 1
            continue

        cost_eur  = avg_cost_pln / eur_rate
        floor_eur = round(cost_eur * floor_mult, 2)
        tgt_eur   = round(cost_eur * target_mult, 2)
        tgt_pln   = round(tgt_eur * eur_rate, 2)

        update = {}

        # Price floor enforcement
        if cur_eur > 0 and cur_eur < floor_eur:
            # Cap raise at MAX_RAISE_PCT per run
            max_new = round(cur_eur * (1 + MAX_RAISE_PCT), 2)
            new_eur = min(floor_eur, max_new)
            update[str(PRICE_GROUP_EUR)] = new_eur
            floor_fixes.append({
                "sku": sku, "cost_pln": avg_cost_pln, "cost_eur": round(cost_eur, 2),
                "floor_eur": floor_eur, "current_eur": cur_eur, "new_eur": new_eur,
            })

        # PLN price sync (if EUR changed or PLN is 0)
        target_new_eur = update.get(str(PRICE_GROUP_EUR), cur_eur)
        if target_new_eur > 0:
            new_pln = round(target_new_eur * eur_rate, 2)
            if abs(new_pln - cur_pln) > 0.5:  # only update if diff > 0.50 PLN
                update[str(PRICE_GROUP_PLN)] = new_pln

        # Log suggestions (below target but above floor — don't auto-raise)
        if cur_eur > 0 and cur_eur < tgt_eur and cur_eur >= floor_eur:
            suggestions.append({
                "sku": sku, "current_eur": cur_eur,
                "target_eur": tgt_eur, "gap_pct": round((tgt_eur - cur_eur) / cur_eur * 100, 1),
            })

        if update:
            price_updates[pid] = update

    # Push updates
    pushed = push_prices(inventory_id, price_updates, dry_run=not apply)

    # Report
    print(f"\n{'─'*60}")
    print(f"Results:")
    print(f"  Products analyzed:    {len(details)}")
    print(f"  Skipped (no cost):    {skipped_no_cost}")
    print(f"  Floor fixes:          {len(floor_fixes)}")
    print(f"  Below target (log):   {len(suggestions)}")
    print(f"  Price updates pushed: {pushed}")

    if floor_fixes:
        print(f"\nFloor fixes (below {floor_mult}x cost):")
        for f in floor_fixes[:20]:
            print(f"  {f['sku']}: {f['current_eur']} → {f['new_eur']} EUR (cost: {f['cost_eur']} EUR)")
        if len(floor_fixes) > 20:
            print(f"  ... and {len(floor_fixes) - 20} more")

    if suggestions[:10]:
        print(f"\nSuggested raises (below {target_mult}x, above floor — manual review):")
        for s in sorted(suggestions, key=lambda x: -x["gap_pct"])[:10]:
            print(f"  {s['sku']}: {s['current_eur']} EUR (target {s['target_eur']}, gap {s['gap_pct']}%)")

    # Discord notification
    color = RED if floor_fixes else GREEN
    status = "APPLIED" if apply else "DRY-RUN"
    fields = [
        {"name": "Analyzed", "value": str(len(details)), "inline": True},
        {"name": "Floor fixes", "value": str(len(floor_fixes)), "inline": True},
        {"name": "Below target", "value": str(len(suggestions)), "inline": True},
        {"name": "Pushed", "value": str(pushed), "inline": True},
        {"name": "EUR/PLN", "value": str(eur_rate), "inline": True},
    ]
    if floor_fixes:
        sample = floor_fixes[:5]
        fields.append({
            "name": "Floor fixes (sample)",
            "value": "\n".join(f"{f['sku']}: {f['current_eur']}→{f['new_eur']} EUR" for f in sample),
            "inline": False,
        })
    send_embed(
        title=f"[{status}] BL Repricing — {len(floor_fixes)} floor fixes",
        description=f"Inventory {inventory_id} | {len(details)} products | EUR/PLN {eur_rate}",
        color=color,
        fields=fields,
        footer=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )

    return {"floor_fixes": len(floor_fixes), "suggestions": len(suggestions), "pushed": pushed}


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="BL Price Floor Enforcer")
    p.add_argument("--apply", action="store_true", help="Push changes to BL (default: dry-run)")
    p.add_argument("--inventory", type=int, default=INVENTORY_ID)
    p.add_argument("--floor", type=float, default=FLOOR_MULT)
    p.add_argument("--target", type=float, default=TARGET_MULT)
    args = p.parse_args()
    run(inventory_id=args.inventory, floor_mult=args.floor,
        target_mult=args.target, apply=args.apply)


if __name__ == "__main__":
    main()
