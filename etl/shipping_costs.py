"""
Shipping costs ETL: estimate and track actual DPD courier costs per order.

The orders.shipping_cost field stores what the BUYER paid for delivery (revenue).
This module tracks what the SELLER pays DPD for each shipment (cost).

Data sources (in order of accuracy):
1. DPD invoice CSV import (exact costs from monthly DPD billing)
2. Manual rate table (contract rates by destination country/weight)
3. Estimation from known DPD contract rates

Usage:
    python3.11 -m etl.run --shipping           # estimate costs for recent orders
    python3.11 -m etl.run --shipping --days 30  # last 30 days only
"""
import csv
import io
import json
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from collections import defaultdict
from . import config, db, fx_rates
from .baselinker import bl_api


# ---------------------------------------------------------------------------
# DPD contract rate table (EUR, net)
# These are typical DPD PL contract rates for e-commerce sellers.
# UPDATE these values to match your actual DPD contract.
# Rates are per-parcel, domestic and international, up to 31.5 kg standard.
# ---------------------------------------------------------------------------

# Rate structure: destination_country -> cost_eur_net per parcel
# Based on DPD PL standard e-commerce contract zones:
#   Zone 0: Poland (domestic)
#   Zone 1: DE, AT, CZ, SK (neighboring)
#   Zone 2: FR, BE, NL, LU, DK (Western EU)
#   Zone 3: IT, ES, SE, HU, RO, BG, HR, SI, LT, LV, EE, FI (further EU)
#   Zone 4: GR, PT, IE (periphery EU)
#   Zone 5: GB, NO, CH (non-EU Europe)

DPD_ZONE_RATES_EUR_NET = {
    0: 2.50,   # Poland domestic (PLN ~10-12, but billed in EUR if contract is EUR)
    1: 4.50,   # DE, AT, CZ, SK
    2: 6.50,   # FR, BE, NL, LU, DK
    3: 8.50,   # IT, ES, SE, HU, RO, BG, HR, SI, LT, LV, EE, FI
    4: 10.50,  # GR, PT, IE
    5: 12.50,  # GB, NO, CH
}

# Poland domestic rate in PLN (net) -- separate because domestic is billed in PLN
DPD_DOMESTIC_PLN_NET = 10.50

COUNTRY_TO_ZONE = {
    # Zone 0 - Domestic
    "PL": 0,
    # Zone 1 - Neighboring
    "DE": 1, "AT": 1, "CZ": 1, "SK": 1,
    # Zone 2 - Western EU
    "FR": 2, "BE": 2, "NL": 2, "LU": 2, "DK": 2,
    # Zone 3 - Further EU
    "IT": 3, "ES": 3, "SE": 3, "HU": 3, "RO": 3, "BG": 3,
    "HR": 3, "SI": 3, "LT": 3, "LV": 3, "EE": 3, "FI": 3,
    # Zone 4 - Periphery
    "GR": 4, "PT": 4, "IE": 4,
    # Zone 5 - Non-EU
    "GB": 5, "NO": 5, "CH": 5,
}

# VAT rate for DPD services (Polish VAT on courier services)
DPD_VAT_RATE = 0.23


def _estimate_dpd_cost(country_code: str) -> tuple[float, float, str]:
    """Estimate DPD shipping cost for a destination country.

    Returns (cost_net, cost_currency, zone_info).
    """
    country = (country_code or "").upper().strip()
    zone = COUNTRY_TO_ZONE.get(country)

    if zone is None:
        # Unknown destination -- use Zone 3 as default (mid-range)
        zone = 3

    if country == "PL":
        # Domestic shipments billed in PLN
        return DPD_DOMESTIC_PLN_NET, "PLN", f"zone_{zone}_domestic"
    else:
        rate = DPD_ZONE_RATES_EUR_NET.get(zone, DPD_ZONE_RATES_EUR_NET[3])
        return rate, "EUR", f"zone_{zone}"


def _get_baselinker_orders_with_courier(days_back: int = 90) -> list[dict]:
    """Fetch orders from Baselinker that have DPD courier packages.

    Returns enriched order data with courier info.
    """
    since = int((datetime.now() - timedelta(days=days_back)).timestamp())
    all_orders = []
    cursor_date = since
    page = 0

    while True:
        params = {
            "date_confirmed_from": cursor_date,
            "get_unconfirmed_orders": False,
            "include_custom_extra_fields": False,
        }
        data = bl_api("getOrders", params)
        orders = data.get("orders", [])
        if not orders:
            break

        for o in orders:
            courier = (o.get("delivery_package_module") or "").lower()
            if courier:
                all_orders.append({
                    "bl_order_id": str(o["order_id"]),
                    "courier": courier,
                    "tracking_number": o.get("delivery_package_nr", ""),
                    "destination_country": o.get("delivery_country_code", ""),
                    "delivery_method": o.get("delivery_method", ""),
                    "delivery_price": float(o.get("delivery_price", 0) or 0),
                    "currency": o.get("currency", "EUR"),
                    "order_date": datetime.fromtimestamp(
                        o.get("date_confirmed", 0)
                    ).strftime("%Y-%m-%d"),
                    "order_source": o.get("order_source", ""),
                })

        # Pagination
        last_date = max(o.get("date_confirmed", 0) for o in orders)
        if last_date <= cursor_date and len(orders) >= 100:
            cursor_date = last_date + 1
        else:
            cursor_date = last_date + 1

        page += 1
        if page % 20 == 0:
            print(f"    Page {page}: {len(all_orders)} orders with courier")
        time.sleep(0.3)

    return all_orders


def _get_existing_shipping_costs(conn) -> set[str]:
    """Get set of external_order_ids that already have shipping costs."""
    all_ids = set()
    offset = 0
    while True:
        rows = db._get("shipping_costs", {
            "select": "external_order_id",
            "limit": "1000",
            "offset": str(offset),
        })
        for r in rows:
            all_ids.add(r["external_order_id"])
        if len(rows) < 1000:
            break
        offset += 1000
    return all_ids


def sync_shipping_costs(conn, days_back: int = 90):
    """Estimate and upsert shipping costs for FBM orders shipped via DPD.

    For each Baselinker order with a DPD package:
    1. Look up the order in our DB
    2. Estimate DPD cost based on destination country
    3. Convert to PLN
    4. Save to shipping_costs table
    5. Update orders.seller_shipping_cost for aggregator
    """
    print("  Fetching Baselinker orders with courier packages...")
    bl_orders = _get_baselinker_orders_with_courier(days_back)
    print(f"  Found {len(bl_orders)} orders with courier packages")

    # Filter to DPD only
    dpd_orders = [o for o in bl_orders if o["courier"] == "dpd"]
    other_couriers = defaultdict(int)
    for o in bl_orders:
        if o["courier"] != "dpd":
            other_couriers[o["courier"]] += 1
    if other_couriers:
        print(f"  Non-DPD couriers found: {dict(other_couriers)}")
    print(f"  DPD orders: {len(dpd_orders)}")

    if not dpd_orders:
        print("  No DPD orders to process")
        return 0

    # Check which already have costs (to avoid re-estimating invoice-imported ones)
    existing = _get_existing_shipping_costs(conn)
    print(f"  Already have shipping costs for {len(existing)} orders")

    # Pre-load order ID mapping: external_id -> (db_id, ...) to avoid N+1 queries
    print("  Loading order ID mapping from DB...")
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    all_db_orders = []
    offset = 0
    while True:
        rows = db._get("orders", {
            "select": "id,external_id",
            "order_date": f"gte.{cutoff}",
            "limit": "1000",
            "offset": str(offset),
        })
        all_db_orders.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
        time.sleep(0.2)

    # Build external_id -> db_id mapping
    ext_to_db_id = {}
    for r in all_db_orders:
        ext_to_db_id[str(r["external_id"])] = r["id"]
    print(f"  Loaded {len(ext_to_db_id)} order mappings")

    # Also check which existing records are from invoices (don't overwrite)
    invoice_order_ids = set()
    if existing:
        offset = 0
        while True:
            rows = db._get("shipping_costs", {
                "select": "external_order_id,cost_source",
                "cost_source": "in.(invoice_csv,manual)",
                "limit": "1000",
                "offset": str(offset),
            })
            for r in rows:
                invoice_order_ids.add(r["external_order_id"])
            if len(rows) < 1000:
                break
            offset += 1000

    # Process each DPD order
    costs_to_upsert = []
    orders_to_update = []
    stats = defaultdict(int)

    for o in dpd_orders:
        bl_id = o["bl_order_id"]

        # Skip if already has invoice-based cost (don't overwrite with estimate)
        if bl_id in invoice_order_ids:
            stats["skipped_invoice"] += 1
            continue

        # Find DB order using pre-loaded mapping
        order_db_id = ext_to_db_id.get(bl_id)

        if not order_db_id:
            stats["not_in_db"] += 1
            continue

        # Estimate DPD cost
        cost_net, cost_currency, zone_info = _estimate_dpd_cost(o["destination_country"])
        cost_gross = round(cost_net * (1 + DPD_VAT_RATE), 2)

        # Convert to PLN
        if cost_currency == "PLN":
            cost_pln = cost_gross
        else:
            fx = fx_rates.convert_to_pln(conn, cost_gross, cost_currency, o["order_date"])
            cost_pln = round(fx, 2) if fx else round(cost_gross * 4.30, 2)  # fallback EUR/PLN

        cost_record = {
            "order_id": order_db_id,
            "external_order_id": bl_id,
            "courier": "dpd",
            "tracking_number": o.get("tracking_number", ""),
            "destination_country": o["destination_country"],
            "cost_net": cost_net,
            "cost_gross": cost_gross,
            "cost_currency": cost_currency,
            "cost_pln": cost_pln,
            "cost_source": "estimate",
            "ship_date": o["order_date"],
            "notes": zone_info,
        }
        costs_to_upsert.append(cost_record)

        # Also prepare order update
        orders_to_update.append({
            "order_db_id": order_db_id,
            "seller_shipping_cost": cost_gross,
            "seller_shipping_cost_pln": cost_pln,
            "delivery_method": o.get("delivery_method", ""),
            "courier_code": "dpd",
        })

        stats["estimated"] += 1

    # Batch upsert shipping costs
    if costs_to_upsert:
        total = 0
        for i in range(0, len(costs_to_upsert), 500):
            chunk = costs_to_upsert[i:i + 500]
            db._post("shipping_costs", chunk, on_conflict="order_id")
            total += len(chunk)
        print(f"  Upserted {total} shipping cost records")

    # Batch update orders with seller shipping cost (batch by order_id ranges)
    updated = 0
    for i in range(0, len(orders_to_update), 50):
        batch = orders_to_update[i:i + 50]
        for upd in batch:
            try:
                db._patch("orders", {"id": f"eq.{upd['order_db_id']}"}, {
                    "seller_shipping_cost": upd["seller_shipping_cost"],
                    "seller_shipping_cost_pln": upd["seller_shipping_cost_pln"],
                    "delivery_method": upd["delivery_method"],
                    "courier_code": upd["courier_code"],
                })
                updated += 1
            except Exception:
                pass  # Non-critical, aggregator can fall back to shipping_costs table
        if i > 0 and i % 100 == 0:
            time.sleep(0.5)  # Avoid Supabase rate limits

    print(f"  Updated {updated} orders with seller_shipping_cost")

    # Summary
    print(f"\n  Shipping cost sync summary:")
    print(f"    Estimated:        {stats['estimated']}")
    print(f"    Skipped (invoice): {stats.get('skipped_invoice', 0)}")
    print(f"    Not in DB:        {stats.get('not_in_db', 0)}")

    # Country breakdown
    country_stats = defaultdict(lambda: {"count": 0, "total_pln": 0.0})
    for c in costs_to_upsert:
        cc = c["destination_country"]
        country_stats[cc]["count"] += 1
        country_stats[cc]["total_pln"] += c["cost_pln"]

    if country_stats:
        print(f"\n  Cost by destination country:")
        for cc, s in sorted(country_stats.items(), key=lambda x: -x[1]["total_pln"]):
            avg = s["total_pln"] / s["count"]
            print(f"    {cc}: {s['count']} shipments, total {s['total_pln']:.2f} PLN (avg {avg:.2f})")

    total_pln = sum(c["cost_pln"] for c in costs_to_upsert)
    if costs_to_upsert:
        print(f"\n  Total estimated DPD cost: {total_pln:.2f} PLN ({len(costs_to_upsert)} shipments)")

    return len(costs_to_upsert)


def import_dpd_csv(conn, csv_path: str):
    """Import actual DPD costs from invoice CSV export.

    DPD Poland provides CSV exports with columns like:
    - Numer listu przewozowego (tracking number)
    - Kwota netto (net cost)
    - Kwota brutto (gross cost)
    - Data nadania (ship date)
    - Kraj docelowy (destination country)
    - Nr faktury (invoice number)

    This function matches tracking numbers to orders and updates costs.
    Column names may vary -- adjust the mapping below to match your DPD CSV format.
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"  [ERROR] CSV file not found: {csv_path}")
        return 0

    print(f"  Reading DPD invoice CSV: {csv_path}")

    # Read CSV (try different encodings/delimiters)
    content = csv_file.read_text(encoding="utf-8-sig")  # BOM-safe
    delimiter = ";" if ";" in content.split("\n")[0] else ","

    reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    rows = list(reader)
    print(f"  Found {len(rows)} rows in CSV")

    if not rows:
        return 0

    # Detect column names (DPD CSVs may have Polish headers)
    sample_cols = list(rows[0].keys())
    print(f"  CSV columns: {sample_cols}")

    # Column mapping (adjust to match actual DPD CSV format)
    col_map = _detect_csv_columns(sample_cols)
    if not col_map.get("tracking"):
        print(f"  [ERROR] Cannot find tracking number column in CSV. Available: {sample_cols}")
        return 0

    # Build tracking_number -> cost mapping
    csv_costs = {}
    for row in rows:
        tracking = (row.get(col_map["tracking"], "") or "").strip()
        if not tracking:
            continue

        cost_net = _parse_decimal(row.get(col_map.get("cost_net", ""), "0"))
        cost_gross = _parse_decimal(row.get(col_map.get("cost_gross", ""), "0"))
        invoice_nr = (row.get(col_map.get("invoice", ""), "") or "").strip()
        ship_date = (row.get(col_map.get("ship_date", ""), "") or "").strip()
        country = (row.get(col_map.get("country", ""), "") or "").strip()

        # If only net provided, calculate gross
        if cost_net > 0 and cost_gross == 0:
            cost_gross = round(cost_net * (1 + DPD_VAT_RATE), 2)
        elif cost_gross > 0 and cost_net == 0:
            cost_net = round(cost_gross / (1 + DPD_VAT_RATE), 2)

        csv_costs[tracking] = {
            "cost_net": cost_net,
            "cost_gross": cost_gross,
            "invoice_nr": invoice_nr,
            "ship_date": ship_date,
            "country": country,
        }

    print(f"  Parsed {len(csv_costs)} tracking numbers with costs")

    # Match to existing shipping_costs records by tracking number
    updated = 0
    for tracking, cost_data in csv_costs.items():
        rows = db._get("shipping_costs", {
            "select": "id,order_id,cost_currency",
            "tracking_number": f"eq.{tracking}",
            "limit": "1",
        })

        if not rows:
            # Try to find order by tracking in orders table raw_data
            # or via Baselinker API lookup -- skip for now
            continue

        record = rows[0]
        currency = record.get("cost_currency", "EUR")

        # Convert to PLN
        cost_pln = cost_data["cost_gross"]
        if currency != "PLN":
            ship_date = cost_data.get("ship_date") or str(date.today())
            fx = fx_rates.convert_to_pln(conn, cost_data["cost_gross"], currency, ship_date)
            cost_pln = round(fx, 2) if fx else round(cost_data["cost_gross"] * 4.30, 2)

        # Update shipping_costs record with real invoice data
        try:
            db._patch("shipping_costs", {"id": f"eq.{record['id']}"}, {
                "cost_net": cost_data["cost_net"],
                "cost_gross": cost_data["cost_gross"],
                "cost_pln": cost_pln,
                "cost_source": "invoice_csv",
                "dpd_invoice_number": cost_data.get("invoice_nr", ""),
            })
            # Also update order
            db._patch("orders", {"id": f"eq.{record['order_id']}"}, {
                "seller_shipping_cost": cost_data["cost_gross"],
                "seller_shipping_cost_pln": cost_pln,
            })
            updated += 1
        except Exception as e:
            print(f"    [WARN] Failed to update tracking {tracking}: {e}")

    print(f"  Updated {updated} records from DPD invoice CSV")
    return updated


def _detect_csv_columns(columns: list[str]) -> dict:
    """Auto-detect column names from DPD CSV headers (Polish or English)."""
    mapping = {}

    tracking_candidates = ["numer listu", "tracking", "nr przesylki", "nr paczki",
                           "parcel number", "list przewozowy", "shipment number"]
    cost_net_candidates = ["kwota netto", "netto", "net", "cost net", "wartosc netto"]
    cost_gross_candidates = ["kwota brutto", "brutto", "gross", "cost gross", "wartosc brutto"]
    invoice_candidates = ["nr faktury", "faktura", "invoice", "invoice number"]
    date_candidates = ["data nadania", "data", "date", "ship date", "data wysylki"]
    country_candidates = ["kraj", "country", "kraj docelowy", "destination"]

    for col in columns:
        col_lower = col.lower().strip()
        if not mapping.get("tracking") and any(c in col_lower for c in tracking_candidates):
            mapping["tracking"] = col
        if not mapping.get("cost_net") and any(c in col_lower for c in cost_net_candidates):
            mapping["cost_net"] = col
        if not mapping.get("cost_gross") and any(c in col_lower for c in cost_gross_candidates):
            mapping["cost_gross"] = col
        if not mapping.get("invoice") and any(c in col_lower for c in invoice_candidates):
            mapping["invoice"] = col
        if not mapping.get("ship_date") and any(c in col_lower for c in date_candidates):
            mapping["ship_date"] = col
        if not mapping.get("country") and any(c in col_lower for c in country_candidates):
            mapping["country"] = col

    return mapping


def _parse_decimal(value: str) -> float:
    """Parse decimal value from CSV (handles comma as decimal separator)."""
    if not value:
        return 0.0
    # Remove whitespace, currency symbols
    clean = value.strip().replace(" ", "").replace("PLN", "").replace("EUR", "").replace("zl", "")
    # Handle comma as decimal separator (Polish format: 12,50)
    if "," in clean and "." not in clean:
        clean = clean.replace(",", ".")
    elif "," in clean and "." in clean:
        # Thousands separator: 1.234,56 -> 1234.56
        clean = clean.replace(".", "").replace(",", ".")
    try:
        return float(clean)
    except ValueError:
        return 0.0


def get_shipping_cost_summary(conn, days_back: int = 90) -> dict:
    """Get shipping cost summary for reporting."""
    cutoff = str(date.today() - timedelta(days=days_back))

    costs = db._get("shipping_costs", {
        "select": "destination_country,cost_pln,cost_source,courier",
        "ship_date": f"gte.{cutoff}",
    })

    summary = {
        "total_cost_pln": sum(float(c.get("cost_pln", 0) or 0) for c in costs),
        "total_shipments": len(costs),
        "by_source": defaultdict(lambda: {"count": 0, "total_pln": 0.0}),
        "by_country": defaultdict(lambda: {"count": 0, "total_pln": 0.0}),
        "by_courier": defaultdict(lambda: {"count": 0, "total_pln": 0.0}),
    }

    for c in costs:
        src = c.get("cost_source", "estimate")
        cc = c.get("destination_country", "??")
        courier = c.get("courier", "dpd")
        cost = float(c.get("cost_pln", 0) or 0)

        summary["by_source"][src]["count"] += 1
        summary["by_source"][src]["total_pln"] += cost
        summary["by_country"][cc]["count"] += 1
        summary["by_country"][cc]["total_pln"] += cost
        summary["by_courier"][courier]["count"] += 1
        summary["by_courier"][courier]["total_pln"] += cost

    return summary
