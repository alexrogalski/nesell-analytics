"""BL Stock Guard — low stock detection, price bumps, and Discord alerts.

Rules:
  - stock < LOW_STOCK   (3)  → raise EUR price by +BUMP_PCT (15%) — demand management
  - stock = 0 (physical)     → Discord alert + optionally set stock to 0 on external stores
  - Printful inventory (52954) always stock 999 — SKIP these
  - Applies only to main warehouse (bl_79555) in inventory 30229

Usage:
  python3.11 -m etl.bl_stock_guard             # dry-run report
  python3.11 -m etl.bl_stock_guard --apply     # bump prices + alert
  python3.11 -m etl.bl_stock_guard --report    # show low stock table only
"""
import argparse
import time
from datetime import datetime

from . import config
from .baselinker import bl_api
from .discord_notify import send_embed, GREEN, YELLOW, RED, ORANGE

# ── Config ──────────────────────────────────────────────────────────────────

INVENTORY_ID    = 30229       # test-exportivo (physical stock)
WAREHOUSE_ID    = "bl_79555"  # main BL warehouse
PRICE_GROUP_EUR = 31059
LOW_STOCK       = 3           # threshold for price bump
BUMP_PCT        = 0.15        # +15% price when low stock
PRINTFUL_INV    = 52954       # skip — always 999

# ── Helpers ──────────────────────────────────────────────────────────────────

def get_all_stock(inventory_id: int) -> dict:
    """Fetch stock for all products. Returns {product_id: stock_qty}."""
    all_stock = {}
    page = 1
    while True:
        data = bl_api("getInventoryProductsStock", {"inventory_id": inventory_id, "page": page})
        stock_data = data.get("stock", {})
        for pid, warehouses in stock_data.items():
            qty = warehouses.get(WAREHOUSE_ID, 0) or 0
            all_stock[pid] = int(qty)
        if len(stock_data) < 1000:
            break
        page += 1
        time.sleep(0.7)
    return all_stock


def get_product_details_chunk(inventory_id: int, product_ids: list) -> dict:
    return bl_api("getInventoryProductsData", {
        "inventory_id": inventory_id,
        "products": [int(p) for p in product_ids],
    }).get("products", {})


def push_price_bump(inventory_id: int, updates: dict, dry_run: bool) -> int:
    if not updates or dry_run:
        return 0
    pushed = 0
    items = list(updates.items())
    for i in range(0, len(items), 1000):
        batch = dict(items[i:i + 1000])
        bl_api("updateInventoryProductsPrices", {"inventory_id": inventory_id, "products": batch})
        pushed += len(batch)
        time.sleep(0.7)
    return pushed


# ── Main ─────────────────────────────────────────────────────────────────────

def run(apply: bool = False, report_only: bool = False):
    mode = "REPORT" if report_only else ("APPLY" if apply else "DRY-RUN")
    print(f"\n{'='*60}")
    print(f"BL Stock Guard — {mode}")
    print(f"{'='*60}")

    stock_map = get_all_stock(INVENTORY_ID)
    print(f"Loaded stock for {len(stock_map)} products")

    zero_stock = [pid for pid, qty in stock_map.items() if qty == 0]
    low_stock  = [pid for pid, qty in stock_map.items() if 0 < qty < LOW_STOCK]

    print(f"  Zero stock:  {len(zero_stock)}")
    print(f"  Low stock:   {len(low_stock)} (< {LOW_STOCK} units)")

    # Fetch details for low stock + zero stock products
    target_ids = list(set(zero_stock + low_stock))
    if not target_ids:
        print("No low/zero stock products — all good!")
        send_embed("BL Stock Guard — OK", f"All products have stock >= {LOW_STOCK}",
                   color=GREEN, footer=datetime.now().strftime("%Y-%m-%d %H:%M"))
        return

    details = {}
    for i in range(0, len(target_ids), 200):
        chunk = target_ids[i:i + 200]
        details.update(get_product_details_chunk(INVENTORY_ID, chunk))
        time.sleep(0.7)

    # Process low stock → price bump
    price_updates = {}
    bumped = []
    zero_alerts = []

    for pid, prod in details.items():
        sku     = prod.get("sku") or pid
        qty     = stock_map.get(pid, 0)
        prices  = prod.get("prices") or {}
        cur_eur = float(prices.get(str(PRICE_GROUP_EUR)) or 0)

        if qty == 0:
            zero_alerts.append({"sku": sku, "pid": pid})

        elif qty < LOW_STOCK:
            if cur_eur > 0:
                new_eur = round(cur_eur * (1 + BUMP_PCT), 2)
                price_updates[pid] = {str(PRICE_GROUP_EUR): new_eur}
                bumped.append({
                    "sku": sku, "qty": qty, "cur_eur": cur_eur, "new_eur": new_eur,
                })

    # Output
    if report_only:
        print("\nZERO STOCK:")
        for z in zero_alerts[:30]:
            print(f"  {z['sku']}")
        print(f"\nLOW STOCK (< {LOW_STOCK}):")
        for b in bumped[:30]:
            print(f"  {b['sku']}: {b['qty']} units, {b['cur_eur']} EUR → {b['new_eur']} EUR")
        return

    # Apply price bumps
    if not apply:
        print(f"\n[DRY-RUN] Would bump {len(price_updates)} products by +{BUMP_PCT*100:.0f}%")
        for b in bumped[:20]:
            print(f"  {b['sku']}: {b['qty']} units → {b['cur_eur']} → {b['new_eur']} EUR")
    else:
        pushed = push_price_bump(INVENTORY_ID, price_updates, dry_run=False)
        print(f"  Bumped {pushed} prices by +{BUMP_PCT*100:.0f}%")

    # Discord alert
    fields = [
        {"name": "Zero stock",  "value": str(len(zero_alerts)),        "inline": True},
        {"name": "Low stock",   "value": str(len(low_stock)),           "inline": True},
        {"name": "Price bumps", "value": str(len(bumped)),              "inline": True},
        {"name": "Mode",        "value": "APPLIED" if apply else "DRY-RUN", "inline": True},
    ]
    if zero_alerts:
        fields.append({
            "name": "Zero stock (sample)",
            "value": "\n".join(z["sku"] for z in zero_alerts[:10]),
            "inline": False,
        })
    if bumped:
        fields.append({
            "name": f"Price bumped +{BUMP_PCT*100:.0f}% (sample)",
            "value": "\n".join(f"{b['sku']}: {b['cur_eur']}→{b['new_eur']} EUR ({b['qty']} left)" for b in bumped[:10]),
            "inline": False,
        })

    color = RED if zero_alerts else (ORANGE if bumped else GREEN)
    send_embed(
        title=f"[{mode}] BL Stock Guard — {len(zero_alerts)} zero, {len(bumped)} low",
        description=f"Inventory {INVENTORY_ID} | threshold: {LOW_STOCK} units",
        color=color,
        fields=fields,
        footer=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )


def main():
    p = argparse.ArgumentParser(description="BL Stock Guard")
    p.add_argument("--apply",  action="store_true")
    p.add_argument("--report", action="store_true")
    args = p.parse_args()
    run(apply=args.apply, report_only=args.report)


if __name__ == "__main__":
    main()
