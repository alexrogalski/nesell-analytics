"""Monthly Exportivo shipment count report.

Identifies Exportivo orders by custom extra field "Exportivo shipped" (id=158931).
This field is set automatically by a Baselinker auto-action when order enters
status "Wysłane Exportivo" (122371).

For historical orders (before the auto-action was created), falls back to
module-based heuristic (dpd, temu_shipping, amazoneasyshippl, allegrokurier, blpaczka).

Sends report to Discord #agents-log.
Run on 1st of each month via launchd.

Usage:
    python3.11 -m etl.exportivo_report          # previous month
    python3.11 -m etl.exportivo_report 2026-02   # specific month
"""
import sys, json, requests, time
from datetime import datetime, timedelta
from calendar import monthrange
from collections import Counter
from . import config

# Custom extra field set by BL auto-action
EXPORTIVO_FIELD_ID = "158931"

# Fallback: modules that indicate Exportivo handled the order (for historical data)
EXPORTIVO_MODULES = {"dpd", "temu_shipping", "amazoneasyshippl", "allegrokurier", "blpaczka"}
NOT_EXPORTIVO_MODULES = {"fulfillmentallegro"}
EXCLUDED_STATUSES = {122372, 369243, 369244, 262219, 262220, 262221, 262222, 189036}

# Discord bot token + channel from ~/.keys/discord.env
_discord_env = config._load_env_file(config.KEYS_DIR / "discord.env")
DISCORD_BOT_TOKEN = _discord_env.get("DISCORD_BOT_TOKEN", "")
DISCORD_CHANNEL_ID = _discord_env.get("DISCORD_CHANNEL_ID", "")


def _get_all_orders_for_month(year: int, month: int) -> list:
    """Fetch ALL orders for given month with custom extra fields."""
    start_dt = datetime(year, month, 1)
    days_in_month = monthrange(year, month)[1]
    end_dt = datetime(year, month, days_in_month, 23, 59, 59)

    date_from = int(start_dt.timestamp())
    date_to = int(end_dt.timestamp())

    all_orders = []
    params = {
        "date_confirmed_from": date_from,
        "date_confirmed_to": date_to,
        "include_custom_extra_fields": True,
    }

    while True:
        resp = requests.post(config.BASELINKER_URL, data={
            "token": config.BASELINKER_TOKEN,
            "method": "getOrders",
            "parameters": json.dumps(params),
        }, timeout=60)
        data = resp.json()
        if data.get("status") == "ERROR":
            raise Exception(f"BL API: {data.get('error_message')}")

        orders = data.get("orders", [])
        raw_count = len(orders)

        # Filter out orders beyond our date range (pagination can overshoot)
        orders = [o for o in orders if o["date_confirmed"] <= date_to]
        all_orders.extend(orders)

        if raw_count < 100:
            break

        last_date = orders[-1]["date_confirmed"] if orders else date_to
        if last_date >= date_to:
            break
        params["date_confirmed_from"] = last_date + 1
        time.sleep(0.7)

    return all_orders


def _is_exportivo(order: dict) -> bool:
    """Check if order was handled by Exportivo.

    Primary: custom extra field 158931 = "Tak" (set by BL auto-action).
    Fallback: module-based heuristic for historical orders.
    """
    # Check custom extra field
    custom = order.get("custom_extra_fields", {})
    if custom.get(EXPORTIVO_FIELD_ID) == "Tak":
        return True

    # Fallback: module-based heuristic
    module = order.get("delivery_package_module", "")
    status = order.get("order_status_id")
    if module in EXPORTIVO_MODULES and status not in EXCLUDED_STATUSES:
        return True

    return False


def _filter_exportivo(orders: list) -> list:
    """Filter orders handled by Exportivo."""
    return [o for o in orders if _is_exportivo(o)]


def _build_report(year: int, month: int, exportivo_orders: list, total_orders: int) -> str:
    """Build text report from orders."""
    month_name = datetime(year, month, 1).strftime("%B %Y")
    total = len(exportivo_orders)

    # Breakdown by shipping module
    modules = Counter(o.get("delivery_package_module", "?") for o in exportivo_orders)
    sorted_modules = sorted(modules.items(), key=lambda x: -x[1])

    # Breakdown by order source
    sources = {}
    for o in exportivo_orders:
        src = (o.get("order_source") or "unknown").lower()
        if "amazon" in src:
            src = "Amazon"
        elif "allegro" in src:
            src = "Allegro"
        elif "temu" in src:
            src = "Temu"
        elif "empik" in src:
            src = "Empik"
        else:
            src = src.capitalize()
        sources[src] = sources.get(src, 0) + 1
    sorted_sources = sorted(sources.items(), key=lambda x: -x[1])

    # Count items
    total_items = sum(
        sum(p.get("quantity", 1) for p in o.get("products", []))
        for o in exportivo_orders
    )

    # Breakdown by delivery country
    countries = Counter(o.get("delivery_country_code", "??") for o in exportivo_orders)
    sorted_countries = sorted(countries.items(), key=lambda x: -x[1])

    lines = [
        f"**Exportivo Shipment Report: {month_name}**",
        "",
        f"Paczek wysłanych: **{total}**",
        f"Produktów w paczkach: **{total_items}**",
        f"Wszystkich zamówień w miesiącu: {total_orders}",
        "",
        "Per kurier:",
    ]
    for mod, cnt in sorted_modules:
        lines.append(f"  {mod}: {cnt}")

    lines.append("")
    lines.append("Per platforma:")
    for src, cnt in sorted_sources:
        lines.append(f"  {src}: {cnt}")

    lines.append("")
    lines.append("Per kraj docelowy:")
    for cc, cnt in sorted_countries:
        lines.append(f"  {cc}: {cnt}")

    return "\n".join(lines)


def _send_discord(message: str):
    """Send message to Discord #agents-log via bot."""
    if not DISCORD_BOT_TOKEN or not DISCORD_CHANNEL_ID:
        print("[Discord] No credentials, skipping")
        return

    url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
    resp = requests.post(url, headers={
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "Content-Type": "application/json",
    }, json={"content": message})

    if resp.status_code in (200, 201):
        print(f"[Discord] Report sent to #{DISCORD_CHANNEL_ID}")
    else:
        print(f"[Discord] Error {resp.status_code}: {resp.text[:200]}")


def run(month_str: str = None):
    """Run the report. month_str format: YYYY-MM (default: previous month)."""
    if month_str:
        year, month = map(int, month_str.split("-"))
    else:
        today = datetime.now()
        first_of_this_month = today.replace(day=1)
        prev = first_of_this_month - timedelta(days=1)
        year, month = prev.year, prev.month

    print(f"[Exportivo] Counting shipments for {year}-{month:02d}...")
    all_orders = _get_all_orders_for_month(year, month)
    exportivo = _filter_exportivo(all_orders)
    report = _build_report(year, month, exportivo, len(all_orders))

    print(report)
    print()
    _send_discord(report)

    return len(exportivo)


if __name__ == "__main__":
    month_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(month_arg)
