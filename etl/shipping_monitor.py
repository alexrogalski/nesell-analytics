"""
Shipping Problem Monitor — detects, classifies, and alerts on delivery issues.

Data source: Baselinker getOrderPackages + getCourierPackagesStatusHistory
(more reliable than DPD TrackTrace API which is frequently WAF-blocked)

Problem types:
- address_issue: failed delivery due to incorrect/incomplete address
- stuck_in_transit: no tracking movement for X days
- delivery_failed: failed attempts (other reasons)
- returned: package returning to sender
- lost: no updates for extended period

Severity:
- critical: package returning, high-value order, A-to-Z risk
- high: address issue (fixable but urgent), multiple failed attempts
- medium: stuck in transit > 3 days
- low: minor delay, single failed attempt

Run: python3.11 -m etl.shipping_monitor [--days 30] [--list] [--resolve ORDER_ID]
"""

import argparse
import time
from datetime import datetime, timedelta, timezone
from . import config, db
from .baselinker import bl_api
from .discord_notify import send_embed, RED, ORANGE, YELLOW, GREEN

# --- Problem detection thresholds ---
STUCK_DAYS_MEDIUM = 3
STUCK_DAYS_HIGH = 5
STUCK_DAYS_CRITICAL = 7
HIGH_VALUE_EUR = 30.0

# --- Baselinker tracking_status mapping ---
# From BL docs: 0=unknown, 1=dispatched, 2=in_transit, 3=delivery_attempt_failed,
# 4=not_delivered, 5=delivered, 6=returned, 11=hub_transfer, 12=waiting_pickup, 13=international_transit
BL_STATUS_DELIVERED = {5}
BL_STATUS_RETURNED = {6}
BL_STATUS_FAILED = {3, 4}         # delivery attempt failed / not delivered
BL_STATUS_IN_TRANSIT = {1, 2, 11, 13}
BL_STATUS_PICKUP = {12}

# --- DPD courier_status_code patterns (from getCourierPackagesStatusHistory) ---
# 5xxxxx = delivery events, 500011 = address issue, 500400 = not delivered
ADDRESS_ISSUE_CODES = {"500011"}
NOT_DELIVERED_CODES = {"500300", "500400"}
RETURNED_CODES = {"600100", "600200", "600300"}


def _get_active_shipments(days_back=30):
    """Get all orders with DPD tracking from Baselinker within last N days."""
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp())
    all_orders = []
    date_from = cutoff

    while True:
        data = bl_api("getOrders", {
            "date_confirmed_from": date_from,
            "get_unconfirmed_orders": False
        })
        orders = data.get("orders", [])
        if not orders:
            break

        for order in orders:
            pkg_module = (order.get("delivery_package_module") or "").lower()
            tracking = (order.get("delivery_package_nr") or "").strip()

            if pkg_module == "dpd" and tracking:
                all_orders.append({
                    "bl_order_id": order["order_id"],
                    "external_order_id": order.get("extra_field_1", "") or str(order.get("order_id", "")),
                    "tracking_number": tracking,
                    "platform": _detect_platform(order),
                    "buyer_name": order.get("delivery_fullname", ""),
                    "buyer_email": order.get("email", ""),
                    "buyer_phone": order.get("phone", ""),
                    "buyer_address": f"{order.get('delivery_address', '')}, {order.get('delivery_postcode', '')} {order.get('delivery_city', '')}, {order.get('delivery_country_code', '')}",
                    "destination_country": order.get("delivery_country_code", ""),
                    "order_value_eur": _calc_order_value(order),
                    "order_status_id": order.get("order_status_id"),
                })

        last_ts = orders[-1].get("date_confirmed", 0)
        if last_ts <= date_from:
            break
        date_from = last_ts
        time.sleep(0.5)

    print(f"  Found {len(all_orders)} DPD shipments in last {days_back} days")
    return all_orders


def _get_package_info(order_id):
    """Get package details from Baselinker (package_id, tracking_status, etc.)."""
    data = bl_api("getOrderPackages", {"order_id": order_id})
    packages = data.get("packages", [])
    dpd_pkgs = [p for p in packages if (p.get("courier_code") or "").lower() == "dpd"]
    return dpd_pkgs


def _get_status_history(package_ids):
    """Get courier status history for multiple packages at once."""
    if not package_ids:
        return {}
    data = bl_api("getCourierPackagesStatusHistory", {"package_ids": package_ids})
    return data.get("packages_history", {})


def _detect_platform(order):
    """Detect platform from Baselinker order."""
    source = order.get("order_source", "").lower()
    extra = order.get("extra_field_1", "") or ""

    if "amazon" in source or extra.startswith(("1", "4")):
        marketplace_map = config.MARKETPLACE_TO_PLATFORM if hasattr(config, 'MARKETPLACE_TO_PLATFORM') else {}
        for mid, pcode in marketplace_map.items():
            if mid in source:
                return pcode
        if ".be" in source:
            return "amazon_be"
        if ".de" in source:
            return "amazon_de"
        return "amazon"
    elif "allegro" in source:
        return "allegro"
    elif "empik" in source:
        return "empik"
    return source[:30] if source else "unknown"


def _calc_order_value(order):
    """Calculate order value in EUR."""
    total = 0
    for p in order.get("products", []):
        total += float(p.get("price_brutto", 0)) * int(p.get("quantity", 1))
    currency = order.get("currency", "EUR")
    if currency == "PLN":
        total = total / 4.27
    return round(total, 2)


def classify_problem(pkg_info, status_history, shipment):
    """Classify a shipping problem based on Baselinker package data.

    pkg_info: dict from getOrderPackages (tracking_status, tracking_status_date, etc.)
    status_history: list of events from getCourierPackagesStatusHistory
    shipment: order metadata dict

    Returns (problem_type, problem_detail, severity, delivery_attempts) or None if no problem.
    """
    tracking_status = int(pkg_info.get("tracking_status", 0))
    last_status_ts = int(pkg_info.get("tracking_status_date", 0))

    # Delivered or at pickup point = no problem
    if tracking_status in BL_STATUS_DELIVERED or tracking_status in BL_STATUS_PICKUP:
        return None

    # Calculate days since last status update
    days_since = 999
    if last_status_ts > 0:
        last_date = datetime.fromtimestamp(last_status_ts, tz=timezone.utc)
        days_since = (datetime.now(timezone.utc) - last_date).days

    # Analyze courier status codes from history
    history_codes = [str(e.get("courier_status_code", "")) for e in (status_history or [])]
    failed_attempts = sum(1 for c in history_codes if c in NOT_DELIVERED_CODES or c in ADDRESS_ISSUE_CODES)
    has_address_issue = any(c in ADDRESS_ISSUE_CODES for c in history_codes)
    is_returned = tracking_status in BL_STATUS_RETURNED or any(c in RETURNED_CODES for c in history_codes)

    # --- Classification logic ---

    # 1. Returned to sender
    if is_returned:
        return ("returned", "Paczka wraca do nadawcy", "critical", failed_attempts)

    # 2. Address issue
    if has_address_issue:
        severity = "critical" if failed_attempts >= 3 or shipment.get("order_value_eur", 0) > HIGH_VALUE_EUR else "high"
        return ("address_issue", f"Bledny adres - {failed_attempts}x proba dostawy", severity, failed_attempts)

    # 3. Delivery failed (status 3 or 4)
    if tracking_status in BL_STATUS_FAILED:
        if failed_attempts >= 3:
            severity = "high"
        elif failed_attempts >= 2:
            severity = "medium"
        else:
            severity = "low"
        return ("delivery_failed", f"{failed_attempts}x nieudana dostawa (status: {tracking_status})", severity, failed_attempts)

    # 4. Stuck in transit
    if tracking_status in BL_STATUS_IN_TRANSIT and days_since >= STUCK_DAYS_MEDIUM:
        if days_since >= STUCK_DAYS_CRITICAL:
            return ("lost", f"Brak aktualizacji od {days_since} dni - prawdopodobnie zagubiona", "critical", 0)
        elif days_since >= STUCK_DAYS_HIGH:
            return ("stuck_in_transit", f"Brak ruchu od {days_since} dni", "high", 0)
        else:
            return ("stuck_in_transit", f"Brak ruchu od {days_since} dni", "medium", 0)

    # 5. Unknown status with no updates
    if tracking_status == 0 and days_since >= STUCK_DAYS_HIGH:
        return ("lost", f"Status nieznany, brak aktualizacji od {days_since} dni", "high", 0)

    return None


def _get_existing_problems():
    """Get all open/in_progress problems from DB."""
    try:
        rows = db._get("shipping_problems", {
            "status": "in.(open,in_progress)",
            "select": "bl_order_id,tracking_number,problem_type,status,detected_at"
        })
        return {(r["bl_order_id"], r["tracking_number"], r["problem_type"]): r for r in rows}
    except Exception as e:
        print(f"  [WARN] Cannot read shipping_problems: {e}")
        return {}


def _upsert_problem(problem_data):
    """Insert or update a shipping problem."""
    try:
        db._post("shipping_problems", problem_data,
                 on_conflict="bl_order_id,tracking_number,problem_type")
    except Exception as e:
        print(f"  [ERROR] Failed to upsert shipping problem: {e}")


def _auto_resolve_delivered(existing_problems, pkg_status_map):
    """Auto-resolve problems where package has since been delivered."""
    resolved_count = 0
    for key, problem in existing_problems.items():
        bl_id, tracking, ptype = key
        pkg = pkg_status_map.get(bl_id)
        if pkg and int(pkg.get("tracking_status", 0)) in (BL_STATUS_DELIVERED | BL_STATUS_PICKUP):
            try:
                db._patch("shipping_problems",
                         {"bl_order_id": f"eq.{bl_id}", "tracking_number": f"eq.{tracking}", "status": "eq.open"},
                         {"status": "auto_resolved", "resolution": "Paczka doreczona (Baselinker status=delivered)",
                          "resolved_at": datetime.now(timezone.utc).isoformat()})
                resolved_count += 1
            except Exception:
                pass
    return resolved_count


def _suggest_resolution(problem_type, problem_detail, platform, tracking_number):
    """Suggest resolution actions based on problem type."""
    suggestions = []

    if problem_type == "address_issue":
        suggestions = [
            "1. Skontaktuj kupujacego - popros o poprawny adres",
            f"2. Przekieruj paczke do DPD Pickup: https://mojapaczka.dpd.com.pl/login?parcel={tracking_number}",
            f"3. DPD tracking: https://tracktrace.dpd.com.pl/EN/parcelDetails?typ=1&p1={tracking_number}",
        ]
        if "amazon" in (platform or ""):
            suggestions.append("4. Wyslij wiadomosc przez Amazon Buyer-Seller Messaging")
            suggestions.append("5. UWAGA: Ryzyko A-to-Z claim jesli nie rozwiazane w 48h")

    elif problem_type == "stuck_in_transit":
        suggestions = [
            f"1. Sprawdz DPD tracking: https://tracktrace.dpd.com.pl/EN/parcelDetails?typ=1&p1={tracking_number}",
            "2. Zadzwon do DPD: Aleksandra Drozka (adrozka@dpd.com.pl)",
            "3. Jesli >7 dni - zgloszenie reklamacyjne DPD",
        ]

    elif problem_type == "returned":
        suggestions = [
            "1. Czekaj na zwrot do Exportivo (Siedlce)",
            "2. Skontaktuj kupujacego - czy chce ponowna wysylke?",
            "3. Przygotuj zwrot pieniedzy jesli klient nie odpowiada",
        ]
        if "amazon" in (platform or ""):
            suggestions.append("4. Wydaj refund ZANIM klient zlozy A-to-Z claim")

    elif problem_type == "lost":
        suggestions = [
            "1. Zgloszenie reklamacyjne DPD (ubezpieczenie do 1000 PLN w kontrakcie)",
            "2. DPD kontakt: adrozka@dpd.com.pl / mpolit@dpd.com.pl",
            "3. Wydaj refund klientowi",
            "4. Jesli Amazon - wyprzedz A-to-Z claim, refunduj proaktywnie",
        ]

    elif problem_type == "delivery_failed":
        suggestions = [
            "1. DPD zrobi automatycznie 2-ga probe dostawy",
            "2. Kupujacy moze przekierowac na DPD Pickup",
            f"3. Tracking: https://tracktrace.dpd.com.pl/EN/parcelDetails?typ=1&p1={tracking_number}",
        ]

    return "\n".join(suggestions)


def _send_discord_alert(new_problems, resolved_count):
    """Send Discord alert with all new/updated problems."""
    if not new_problems and resolved_count == 0:
        return

    critical = [p for p in new_problems if p["severity"] == "critical"]
    high = [p for p in new_problems if p["severity"] == "high"]
    medium = [p for p in new_problems if p["severity"] == "medium"]
    low = [p for p in new_problems if p["severity"] == "low"]

    if critical:
        color = RED
    elif high:
        color = ORANGE
    elif medium:
        color = YELLOW
    else:
        color = GREEN

    title = f"Shipping Monitor: {len(new_problems)} problemow"
    if resolved_count:
        title += f" (+{resolved_count} resolved)"

    desc_parts = []
    if critical:
        desc_parts.append(f"**KRYTYCZNE: {len(critical)}**")
    if high:
        desc_parts.append(f"Wysokie: {len(high)}")
    if medium:
        desc_parts.append(f"Srednie: {len(medium)}")
    if low:
        desc_parts.append(f"Niskie: {len(low)}")

    description = " | ".join(desc_parts) if desc_parts else "Brak nowych problemow"

    fields = []
    for p in (critical + high + medium + low)[:8]:
        sev = p['severity']
        icon = '\U0001f534' if sev == 'critical' else '\U0001f7e0' if sev == 'high' else '\U0001f7e1' if sev == 'medium' else '\U0001f7e2'
        field_name = f"{icon} {p['problem_type'].upper()}: #{p['bl_order_id']}"
        field_value = (
            f"**{p['buyer_name']}** \u2192 {p['destination_country']}\n"
            f"{p['problem_detail']}\n"
            f"Tracking: `{p['tracking_number']}`\n"
            f"Wartosc: {p['order_value_eur']} EUR | Platform: {p['platform']}"
        )
        fields.append({"name": field_name[:256], "value": field_value[:1024], "inline": False})

    if len(new_problems) > 8:
        fields.append({"name": "...", "value": f"+ {len(new_problems) - 8} wiecej problemow", "inline": False})

    footer = f"Shipping Monitor | {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    send_embed(title, description, color, fields=fields, footer=footer)


def scan(days_back=30, list_only=False):
    """Main scan: detect shipping problems, classify, alert, upsert to DB."""
    print(f"\n{'='*60}")
    print(f"  SHIPPING MONITOR - scanning last {days_back} days")
    print(f"{'='*60}")

    # 1. Get all active DPD shipments from Baselinker
    shipments = _get_active_shipments(days_back)
    if not shipments:
        print("  No DPD shipments found.")
        return {"scanned": 0, "problems": 0, "resolved": 0}

    # 2. Get existing open problems from DB
    existing = _get_existing_problems()

    # 3. Get package details + status history from Baselinker
    print(f"  Fetching package details for {len(shipments)} shipments...")
    pkg_status_map = {}  # bl_order_id -> pkg_info
    pkg_id_to_order = {}  # package_id -> bl_order_id
    all_pkg_ids = []

    for i, shipment in enumerate(shipments):
        pkgs = _get_package_info(shipment["bl_order_id"])
        for p in pkgs:
            if p.get("courier_package_nr") == shipment["tracking_number"]:
                pkg_status_map[shipment["bl_order_id"]] = p
                pkg_id = int(p["package_id"])
                pkg_id_to_order[pkg_id] = shipment["bl_order_id"]
                all_pkg_ids.append(pkg_id)
                break
        if (i + 1) % 20 == 0:
            print(f"  ... fetched {i+1}/{len(shipments)}")
        time.sleep(0.3)

    # 4. Batch fetch status history
    print(f"  Fetching status history for {len(all_pkg_ids)} packages...")
    history_map = {}  # bl_order_id -> [events]
    # getCourierPackagesStatusHistory supports batches
    batch_size = 50
    for batch_start in range(0, len(all_pkg_ids), batch_size):
        batch = all_pkg_ids[batch_start:batch_start + batch_size]
        hist = _get_status_history(batch)
        for pkg_id_str, events in hist.items():
            bl_id = pkg_id_to_order.get(int(pkg_id_str))
            if bl_id:
                history_map[bl_id] = events
        time.sleep(0.5)

    # 5. Classify problems
    new_problems = []
    delivered_count = 0

    for shipment in shipments:
        bl_id = shipment["bl_order_id"]
        pkg = pkg_status_map.get(bl_id)
        if not pkg:
            continue

        tracking_status = int(pkg.get("tracking_status", 0))
        if tracking_status in BL_STATUS_DELIVERED or tracking_status in BL_STATUS_PICKUP:
            delivered_count += 1
            continue

        history = history_map.get(bl_id, [])
        result = classify_problem(pkg, history, shipment)
        if result is None:
            continue

        problem_type, problem_detail, severity, delivery_attempts = result

        key = (bl_id, shipment["tracking_number"], problem_type)
        if key in existing:
            continue

        # Format last event info
        last_status_ts = int(pkg.get("tracking_status_date", 0))
        last_event_date = datetime.fromtimestamp(last_status_ts, tz=timezone.utc).strftime("%Y-%m-%d") if last_status_ts else None
        days_since = (datetime.now(timezone.utc) - datetime.fromtimestamp(last_status_ts, tz=timezone.utc)).days if last_status_ts else None
        last_event_text = f"tracking_status={tracking_status}"
        if history:
            last_event_text += f", last_code={history[-1].get('courier_status_code', '?')}"

        problem_data = {
            "bl_order_id": bl_id,
            "external_order_id": shipment.get("external_order_id"),
            "tracking_number": shipment["tracking_number"],
            "platform": shipment.get("platform"),
            "courier": "DPD",
            "problem_type": problem_type,
            "problem_detail": problem_detail,
            "severity": severity,
            "last_event_date": last_event_date,
            "last_event_text": last_event_text,
            "days_since_last_event": days_since,
            "delivery_attempts": delivery_attempts,
            "buyer_name": shipment.get("buyer_name"),
            "buyer_email": shipment.get("buyer_email"),
            "buyer_phone": shipment.get("buyer_phone"),
            "buyer_address": shipment.get("buyer_address"),
            "destination_country": shipment.get("destination_country"),
            "order_value_eur": shipment.get("order_value_eur"),
            "status": "open",
        }

        new_problems.append(problem_data)

        suggestions = _suggest_resolution(problem_type, problem_detail, shipment.get("platform"), shipment["tracking_number"])
        print(f"\n  {'!'*3} [{severity.upper()}] {problem_type}: #{bl_id}")
        print(f"      {problem_detail}")
        print(f"      {shipment['buyer_name']} -> {shipment.get('destination_country')}")
        print(f"      Tracking: {shipment['tracking_number']}")
        print(f"      Wartosc: {shipment.get('order_value_eur')} EUR")
        print(f"      Sugestie:\n      {suggestions.replace(chr(10), chr(10) + '      ')}")

    # 6. Auto-resolve delivered problems
    resolved_count = _auto_resolve_delivered(existing, pkg_status_map)
    if resolved_count:
        print(f"\n  Auto-resolved {resolved_count} problems (packages delivered)")

    # 7. Upsert new problems to DB
    if not list_only:
        for p in new_problems:
            _upsert_problem(p)
        if new_problems:
            print(f"\n  Upserted {len(new_problems)} new problems to DB")

    # 8. Discord alert
    if not list_only and new_problems:
        _send_discord_alert(new_problems, resolved_count)
        print("  Discord alert sent")

    summary = {
        "scanned": len(shipments),
        "delivered": delivered_count,
        "problems_found": len(new_problems),
        "auto_resolved": resolved_count,
        "critical": len([p for p in new_problems if p["severity"] == "critical"]),
        "high": len([p for p in new_problems if p["severity"] == "high"]),
    }

    print(f"\n  SUMMARY: {len(shipments)} scanned, {delivered_count} delivered OK, "
          f"{len(new_problems)} new problems, {resolved_count} auto-resolved")
    print(f"{'='*60}\n")

    return summary


def get_open_problems():
    """Get all open problems for dashboard/reporting."""
    try:
        return db._get("shipping_problems", {
            "status": "in.(open,in_progress)",
            "order": "severity.asc,detected_at.desc"
        })
    except Exception as e:
        print(f"[ERROR] Cannot fetch problems: {e}")
        return []


def resolve_problem(bl_order_id, tracking_number, resolution_text):
    """Manually resolve a problem."""
    try:
        db._patch("shipping_problems",
                 {"bl_order_id": f"eq.{bl_order_id}", "tracking_number": f"eq.{tracking_number}", "status": "in.(open,in_progress)"},
                 {"status": "resolved", "resolution": resolution_text,
                  "resolved_at": datetime.now(timezone.utc).isoformat()})
        print(f"  Resolved problem for #{bl_order_id}")
        return True
    except Exception as e:
        print(f"  [ERROR] Failed to resolve: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Shipping Problem Monitor")
    parser.add_argument("--days", type=int, default=30, help="Lookback days (default: 30)")
    parser.add_argument("--list", action="store_true", help="List only, don't upsert to DB or alert")
    parser.add_argument("--open", action="store_true", help="Show open problems from DB")
    parser.add_argument("--resolve", type=str, help="Resolve problem: ORDER_ID:TRACKING:resolution text")
    args = parser.parse_args()

    if args.open:
        problems = get_open_problems()
        if not problems:
            print("No open problems!")
            return
        for p in problems:
            print(f"  [{p['severity'].upper()}] {p['problem_type']}: #{p['bl_order_id']} - {p['problem_detail']}")
            print(f"    {p['buyer_name']} -> {p['destination_country']} | {p['tracking_number']}")
            print(f"    Detected: {p['detected_at']} | Status: {p['status']}")
            print()
        return

    if args.resolve:
        parts = args.resolve.split(":", 2)
        if len(parts) < 3:
            print("Usage: --resolve ORDER_ID:TRACKING:resolution text")
            return
        resolve_problem(int(parts[0]), parts[1], parts[2])
        return

    scan(days_back=args.days, list_only=args.list)


if __name__ == "__main__":
    main()
