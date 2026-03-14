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
# DPD Classic Export contract rates (EUR netto, per package)
# Source: "DPD POLSKA oferta Export" -- Nesell actual contract rates
# Last updated: 2026-03-14
# ---------------------------------------------------------------------------

# Per-package fixed fees (EUR netto)
DPD_SECURITY_FEE_EUR = 0.45      # opłata bezpieczeństwa per package

# Fuel surcharge percentage -- check dpd.com.pl for current value
# Typically 15-20%, set to 17% as default; update when DPD changes it
DPD_FUEL_SURCHARGE_PCT = 0.217  # 21.70% as of Feb 2026, check dpd.com.pl monthly

# Fallback EUR/PLN rate when fx_rates table has no data
EUR_PLN_FALLBACK = 4.27

# Weight brackets in kg (contract columns)
WEIGHT_BRACKETS = [1.0, 3.0, 10.0, 20.0, 31.5]

# DPD Classic Export rates by country code (EUR netto)
# Each country maps to: zone, rates by weight bracket [do 1kg, do 3kg, do 10kg, do 20kg, do 31.5kg]
# Zone surcharges are NOT included by default (edge cases for specific postcodes only)
DPD_COUNTRY_RATES = {
    # Zone A
    "CZ": {"zone": "A", "rates": [2.00, 2.51, 3.73, 4.90, 7.62]},
    "LT": {"zone": "A", "rates": [2.23, 2.76, 3.98, 5.17, 7.93]},
    "LV": {"zone": "A", "rates": [2.63, 3.28, 4.76, 6.17, 9.56]},
    "DE": {"zone": "A", "rates": [2.86, 3.62, 5.30, 6.87, 10.76]},
    "SK": {"zone": "A", "rates": [1.89, 2.30, 3.28, 4.27, 6.43]},
    "HU": {"zone": "A", "rates": [1.70, 2.17, 3.31, 4.42, 6.94]},
    "UA": {"zone": "A", "rates": [1.98, 3.03, 5.31, 7.38, 12.75]},
    # Zone B
    "AT": {"zone": "B", "rates": [3.18, 4.02, 5.89, 7.61, 11.96]},
    "BE": {"zone": "B", "rates": [3.56, 4.46, 6.44, 8.25, 12.88]},
    "EE": {"zone": "B", "rates": [3.78, 4.50, 6.12, 7.64, 11.37]},
    "NL": {"zone": "B", "rates": [2.92, 3.86, 5.93, 7.82, 12.68]},
    "LU": {"zone": "B", "rates": [3.68, 4.87, 7.45, 9.76, 15.88]},
    "SI": {"zone": "B", "rates": [2.67, 3.65, 5.79, 7.74, 12.77]},
    # Zone C
    "BG": {"zone": "C", "rates": [5.13, 5.13, 8.46, 10.85, 13.55]},
    "DK": {"zone": "C", "rates": [3.99, 5.50, 8.71, 11.57, 19.28]},
    "FR": {"zone": "C", "rates": [3.65, 4.83, 7.37, 9.65, 15.69]},
    "RO": {"zone": "C", "rates": [4.33, 4.68, 5.54, 7.00, 8.27]},
    "GB": {"zone": "C", "rates": [3.29, 4.80, 7.99, 10.82, 18.49]},
    "IT": {"zone": "C", "rates": [3.72, 4.79, 7.09, 9.18, 14.63]},
    "CH": {"zone": "C", "rates": [4.47, 5.33, 7.21, 8.95, 13.35]},
    # Zone D
    "HR": {"zone": "D", "rates": [2.46, 3.41, 5.50, 7.40, 12.29]},
    "ES": {"zone": "D", "rates": [3.81, 5.34, 8.60, 11.48, 19.31]},
    "IE": {"zone": "D", "rates": [3.71, 5.79, 10.13, 13.91, 24.44]},
    "PT": {"zone": "D", "rates": [2.55, 3.97, 6.99, 9.68, 16.92]},
    "SE": {"zone": "D", "rates": [5.03, 6.71, 10.24, 13.35, 21.85]},
    "FI": {"zone": "D", "rates": [6.94, 8.34, 11.32, 13.97, 21.10]},
    # Zone E
    "GR": {"zone": "E", "rates": [9.00, 9.00, 12.80, 17.56, 22.31]},
    "NO": {"zone": "E", "rates": [6.14, 7.86, 11.46, 14.63, 23.33]},
}

# Poland domestic: NOT in this DPD Export contract (rates too high)
# DPD domestic shipments should not go through this module.
# If a PL order somehow appears, use a high fallback to flag it.
DPD_DOMESTIC_PLN_NET = 15.00  # fallback only, not real contract rate

# VAT rate for DPD services (Polish VAT on courier services)
DPD_VAT_RATE = 0.23


def _get_rate_for_weight(rates: list[float], weight_kg: float) -> float:
    """Look up the correct rate from the weight bracket list.

    Brackets: [do 1kg, do 3kg, do 10kg, do 20kg, do 31.5kg]
    """
    for i, bracket_max in enumerate(WEIGHT_BRACKETS):
        if weight_kg <= bracket_max:
            return rates[i]
    # Over 31.5kg: use the highest bracket
    return rates[-1]


def _estimate_dpd_cost(country_code: str, weight_kg: float = 0.8) -> tuple[float, float, str]:
    """Estimate DPD shipping cost for a destination country and weight.

    Uses actual Nesell DPD Classic Export contract rates.
    Adds security fee (0.45 EUR) and fuel surcharge (default 17%) on top.

    Args:
        country_code: ISO 2-letter country code
        weight_kg: package weight in kg (default 0.8, since 90% of packages < 1kg)

    Returns (cost_net_total_eur, cost_currency, zone_info).
        cost_net_total_eur includes base rate + security fee + fuel surcharge.
    """
    country = (country_code or "").upper().strip()

    if country == "PL":
        # Domestic: not in DPD Export contract, use fallback
        return DPD_DOMESTIC_PLN_NET, "PLN", "domestic_fallback"

    country_data = DPD_COUNTRY_RATES.get(country)
    if country_data is None:
        # Unknown destination: use DE rates as mid-range fallback
        country_data = DPD_COUNTRY_RATES["DE"]
        zone_info = f"unknown_country_{country}_fallback_DE"
    else:
        zone_info = f"zone_{country_data['zone']}_{country}"

    # Base rate from weight bracket
    base_rate = _get_rate_for_weight(country_data["rates"], weight_kg)

    # Add per-package security fee
    subtotal = base_rate + DPD_SECURITY_FEE_EUR

    # Apply fuel surcharge on the subtotal
    total_net = round(subtotal * (1 + DPD_FUEL_SURCHARGE_PCT), 2)

    return total_net, "EUR", zone_info


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
            cost_pln = round(fx, 2) if fx else round(cost_gross * EUR_PLN_FALLBACK, 2)

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
            cost_pln = round(fx, 2) if fx else round(cost_data["cost_gross"] * EUR_PLN_FALLBACK, 2)

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
