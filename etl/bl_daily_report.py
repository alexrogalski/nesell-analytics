"""BL Daily Report — sends daily summary to Discord #agents-log.

Covers:
  - Orders: today / last 7d / last 30d (count + revenue PLN)
  - Returns opened in last 7 days
  - Low stock alerts (top 10 SKUs)
  - Platform breakdown (Amazon / Allegro / other)
  - Latest ETL run status

Usage:
  python3.11 -m etl.bl_daily_report           # send report now
  python3.11 -m etl.bl_daily_report --print   # print only, no Discord

Cron: launchd daily at 08:00 via com.nesell.bl-daily-report
"""
import argparse
import time
from datetime import datetime, timedelta, date
from collections import defaultdict

from . import db
from .baselinker import bl_api
from .discord_notify import send_embed, send, GREEN, YELLOW, RED, BLUE, ORANGE

# ── Config ──────────────────────────────────────────────────────────────────

INVENTORY_ID    = 30229
WAREHOUSE_ID    = "bl_79555"
PRICE_GROUP_EUR = 31059
LOW_STOCK_WARN  = 5

# Platform IDs from Supabase (adjust if differ)
PLATFORM_NAMES = {
    1:  "Amazon DE",
    2:  "Amazon FR",
    3:  "Amazon IT",
    4:  "Amazon ES",
    5:  "Amazon NL",
    6:  "Amazon SE",
    7:  "Amazon PL",
    8:  "Amazon BE",
    10: "Allegro",
    11: "Empik",
    12: "Temu",
}

# ── Data Fetchers ─────────────────────────────────────────────────────────────

def get_supabase_orders_summary() -> dict:
    """Get order stats from Supabase for today / 7d / 30d."""
    today     = date.today().isoformat()
    week_ago  = (date.today() - timedelta(days=7)).isoformat()
    month_ago = (date.today() - timedelta(days=30)).isoformat()

    def query(since):
        rows = db._get("orders", {
            "select": "platform_id,total_paid_pln,status",
            "order_date": f"gte.{since}",
            "status": "neq.cancelled",
        })
        total_rev = sum(float(r.get("total_paid_pln") or 0) for r in rows)
        by_platform = defaultdict(lambda: {"count": 0, "revenue": 0.0})
        for r in rows:
            pid = r.get("platform_id") or 0
            by_platform[pid]["count"] += 1
            by_platform[pid]["revenue"] += float(r.get("total_paid_pln") or 0)
        return {"count": len(rows), "revenue": round(total_rev, 2), "by_platform": dict(by_platform)}

    return {
        "today":    query(today),
        "7d":       query(week_ago),
        "30d":      query(month_ago),
    }


def get_returns_last_7d() -> dict:
    """Count returns from BL in last 7 days."""
    date_from = int((datetime.now() - timedelta(days=7)).timestamp())
    returns = []
    params = {"date_from": date_from}
    while True:
        data = bl_api("getOrderReturns", params)
        batch = data.get("returns", [])
        returns.extend(batch)
        if len(batch) < 100:
            break
        params["date_from"] = batch[-1].get("date_add", 0) + 1
        time.sleep(0.7)

    open_count = sum(1 for r in returns if r.get("status_id") not in (101527, 101528))
    return {"total": len(returns), "open": open_count}


def get_low_stock(top_n: int = 10) -> list:
    """Get top N products with lowest stock > 0."""
    all_stock = {}
    page = 1
    while True:
        data = bl_api("getInventoryProductsStock", {"inventory_id": INVENTORY_ID, "page": page})
        stock_data = data.get("stock", {})
        for pid, wh in stock_data.items():
            qty = wh.get(WAREHOUSE_ID, 0) or 0
            if 0 < int(qty) < LOW_STOCK_WARN:
                all_stock[pid] = int(qty)
        if len(stock_data) < 1000:
            break
        page += 1
        time.sleep(0.7)

    # Fetch SKUs for top N
    sorted_pids = sorted(all_stock.items(), key=lambda x: x[1])[:top_n]
    if not sorted_pids:
        return []

    details = bl_api("getInventoryProductsData", {
        "inventory_id": INVENTORY_ID,
        "products": [int(p) for p, _ in sorted_pids],
    }).get("products", {})

    result = []
    for pid, qty in sorted_pids:
        prod = details.get(str(pid)) or details.get(pid) or {}
        result.append({"sku": prod.get("sku") or str(pid), "qty": qty})
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def run(print_only: bool = False):
    now = datetime.now()
    print(f"\n{'='*60}")
    print(f"BL Daily Report — {now.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    # Gather data
    try:
        orders = get_supabase_orders_summary()
    except Exception as e:
        print(f"  [WARN] Orders fetch failed: {e}")
        orders = {"today": {"count": 0, "revenue": 0}, "7d": {"count": 0, "revenue": 0},
                  "30d": {"count": 0, "revenue": 0}}

    try:
        returns = get_returns_last_7d()
    except Exception as e:
        print(f"  [WARN] Returns fetch failed: {e}")
        returns = {"total": 0, "open": 0}

    try:
        low_stock = get_low_stock()
    except Exception as e:
        print(f"  [WARN] Stock fetch failed: {e}")
        low_stock = []

    # Print summary
    o_today = orders["today"]
    o_7d    = orders["7d"]
    o_30d   = orders["30d"]

    print(f"\nOrders today:   {o_today['count']} | {o_today['revenue']:.2f} PLN")
    print(f"Orders 7d:      {o_7d['count']} | {o_7d['revenue']:.2f} PLN")
    print(f"Orders 30d:     {o_30d['count']} | {o_30d['revenue']:.2f} PLN")
    print(f"Returns 7d:     {returns['total']} total, {returns['open']} open")
    if low_stock:
        print(f"\nLow stock (<{LOW_STOCK_WARN}):")
        for s in low_stock:
            print(f"  {s['sku']}: {s['qty']} units")

    if print_only:
        return

    # Platform breakdown for 7d
    platform_lines = []
    by_plat = o_7d.get("by_platform", {})
    for pid, stats in sorted(by_plat.items(), key=lambda x: -x[1]["revenue"])[:6]:
        name = PLATFORM_NAMES.get(pid, f"Platform {pid}")
        platform_lines.append(f"{name}: {stats['count']} zam. | {stats['revenue']:.0f} PLN")

    # Build Discord embed
    fields = [
        {
            "name": "Dzisiaj",
            "value": f"{o_today['count']} zamówień\n{o_today['revenue']:.2f} PLN",
            "inline": True,
        },
        {
            "name": "Ostatnie 7 dni",
            "value": f"{o_7d['count']} zamówień\n{o_7d['revenue']:.2f} PLN",
            "inline": True,
        },
        {
            "name": "Ostatnie 30 dni",
            "value": f"{o_30d['count']} zamówień\n{o_30d['revenue']:.2f} PLN",
            "inline": True,
        },
        {
            "name": f"Zwroty (7d)",
            "value": f"Łącznie: {returns['total']}\nOtwarte: **{returns['open']}**",
            "inline": True,
        },
    ]

    if platform_lines:
        fields.append({
            "name": "Platformy (7d)",
            "value": "\n".join(platform_lines),
            "inline": False,
        })

    if low_stock:
        stock_lines = "\n".join(f"{s['sku']}: **{s['qty']} szt.**" for s in low_stock[:8])
        fields.append({
            "name": f"Niski stan (<{LOW_STOCK_WARN}) — wymaga uzupełnienia",
            "value": stock_lines,
            "inline": False,
        })

    color = RED if returns["open"] > 3 or len(low_stock) > 5 else (
            YELLOW if returns["open"] > 0 or low_stock else GREEN)

    send_embed(
        title=f"Nesell Daily Report — {now.strftime('%d.%m.%Y')}",
        description=(
            f"Dziś: **{o_today['count']} zamówień** | {o_today['revenue']:.2f} PLN\n"
            f"7d: {o_7d['count']} zam. | {o_7d['revenue']:.2f} PLN | "
            f"30d: {o_30d['count']} zam. | {o_30d['revenue']:.2f} PLN"
        ),
        color=color,
        fields=fields,
        footer=f"Baselinker Daily Report • {now.strftime('%Y-%m-%d %H:%M')}",
    )
    print("\nReport sent to Discord.")


def main():
    p = argparse.ArgumentParser(description="BL Daily Report")
    p.add_argument("--print", dest="print_only", action="store_true")
    args = p.parse_args()
    run(print_only=args.print_only)


if __name__ == "__main__":
    main()
