"""COGS CSV Import/Export: bulk import product costs from CSV file.

Export: generates a CSV template with top-N products missing COGS (sorted by revenue)
Import: reads a filled CSV and updates Supabase products table + logs to cost_history

CSV format:
  sku, name, source, revenue_pln_90d, units_90d, cost_pln

Usage:
  # Generate template (fill in cost_pln column, then import):
  python3.11 -m etl.run --cogs-export missing_cogs.csv

  # Import after filling:
  python3.11 -m etl.run --cogs-csv missing_cogs.csv
"""
import csv
import os
from collections import defaultdict
from datetime import date
from . import db


def export_missing_cogs_csv(output_path: str, top_n: int = 100) -> dict:
    """Export top-N revenue products with missing COGS to a CSV template.

    Queries products without cost_pln, cross-references daily_metrics for revenue,
    and writes a sorted CSV with empty cost_pln column for manual filling.
    """
    print(f"[COGS CSV Export] Fetching products without COGS...")

    # Fetch all products missing COGS
    missing_products: list[dict] = []
    offset = 0
    while True:
        rows = db._get("products", {
            "select": "sku,name,source",
            "or": "(cost_pln.is.null,cost_pln.eq.0)",
            "order": "sku",
            "limit": "1000",
            "offset": str(offset),
        })
        missing_products.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000

    print(f"  Found {len(missing_products)} products without COGS")
    if not missing_products:
        print("  All products have COGS! Nothing to export.")
        return {"exported": 0}

    missing_by_sku = {p["sku"]: p for p in missing_products}
    missing_set = set(missing_by_sku.keys())

    # Aggregate revenue from daily_metrics for these SKUs
    print("  Aggregating revenue from daily_metrics...")
    sku_revenue: dict[str, dict] = defaultdict(lambda: {"revenue_pln": 0.0, "units": 0})
    offset = 0
    while True:
        rows = db._get("daily_metrics", {
            "select": "sku,revenue_pln,units",
            "limit": "1000",
            "offset": str(offset),
        })
        for row in rows:
            sku = row.get("sku", "")
            if sku in missing_set:
                sku_revenue[sku]["revenue_pln"] += float(row.get("revenue_pln") or 0)
                sku_revenue[sku]["units"] += int(row.get("units") or 0)
        if len(rows) < 1000:
            break
        offset += 1000

    # Sort by revenue descending, take top_n from those with any revenue
    with_revenue = sorted(
        [s for s in missing_set if sku_revenue[s]["revenue_pln"] > 0],
        key=lambda s: sku_revenue[s]["revenue_pln"],
        reverse=True,
    )[:top_n]

    # Fill remaining slots with SKUs that have no revenue data
    without_revenue = sorted(missing_set - set(with_revenue))
    remaining = top_n - len(with_revenue)
    export_skus = with_revenue + without_revenue[:remaining]

    print(f"  Exporting {len(export_skus)} SKUs to: {output_path}")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "sku", "name", "source", "revenue_pln_90d", "units_90d", "cost_pln",
        ])
        writer.writeheader()
        for sku in export_skus:
            p = missing_by_sku[sku]
            rev = sku_revenue.get(sku, {"revenue_pln": 0, "units": 0})
            writer.writerow({
                "sku": sku,
                "name": (p.get("name") or "")[:80],
                "source": p.get("source") or "",
                "revenue_pln_90d": round(rev["revenue_pln"], 2),
                "units_90d": rev["units"],
                "cost_pln": "",  # fill this in manually
            })

    print(f"  Done. Fill in 'cost_pln' column, then run:")
    print(f"    python3.11 -m etl.run --cogs-csv {output_path}")
    return {"exported": len(export_skus), "total_missing": len(missing_products)}


def import_cogs_csv(csv_path: str) -> dict:
    """Import COGS from a filled CSV file and update Supabase.

    Only processes rows where cost_pln > 0. Never overwrites existing cost > 0.
    Logs all updates to cost_history table.

    Args:
        csv_path: path to CSV file with at minimum 'sku' and 'cost_pln' columns.

    Returns:
        dict with imported, skipped, errors counts.
    """
    print(f"[COGS CSV Import] Reading: {csv_path}")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Parse CSV
    candidates: list[dict] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # start=2 (header is row 1)
            sku = (row.get("sku") or "").strip()
            cost_raw = (row.get("cost_pln") or "").strip().replace(",", ".")
            if not sku:
                continue
            if not cost_raw:
                continue
            try:
                cost_pln = float(cost_raw)
            except ValueError:
                print(f"  SKIP row {i}: {sku!r} — invalid cost_pln: {cost_raw!r}")
                continue
            if cost_pln <= 0:
                continue
            candidates.append({"sku": sku, "cost_pln": cost_pln})

    print(f"  Parsed {len(candidates)} rows with valid cost_pln > 0")
    if not candidates:
        print("  Nothing to import.")
        return {"imported": 0, "skipped": 0, "errors": 0}

    # Fetch current product costs (avoid overwriting existing > 0 values)
    print("  Fetching current product costs from Supabase...")
    existing: dict[str, float] = {}
    offset = 0
    while True:
        rows = db._get("products", {
            "select": "sku,cost_pln",
            "limit": "1000",
            "offset": str(offset),
        })
        for p in rows:
            existing[p["sku"]] = float(p.get("cost_pln") or 0)
        if len(rows) < 1000:
            break
        offset += 1000

    # Process updates
    imported = 0
    skipped = 0
    errors = 0
    today = str(date.today())
    cost_history_entries = []

    for item in candidates:
        sku = item["sku"]
        cost_pln = item["cost_pln"]

        if sku not in existing:
            print(f"  SKIP: {sku!r} — not found in products table")
            skipped += 1
            continue

        current = existing[sku]
        if current > 0:
            print(f"  SKIP: {sku!r} — already has cost {current:.2f} PLN (not overwriting)")
            skipped += 1
            continue

        try:
            db._patch("products", {"sku": f"eq.{sku}"}, {"cost_pln": round(cost_pln, 2)})
            print(f"  SET: {sku} = {cost_pln:.2f} PLN")
            imported += 1
            cost_history_entries.append({
                "sku": sku,
                "cost_eur": None,
                "cost_pln": round(cost_pln, 2),
                "source": "cogs_csv_import",
                "notes": f"Imported from CSV: {os.path.basename(csv_path)}",
                "effective_from": today,
            })
        except Exception as e:
            print(f"  ERROR: {sku}: {e}")
            errors += 1

    # Log to cost_history
    if cost_history_entries:
        from . import cogs_filler
        cogs_filler._log_cost_history(cost_history_entries)

    print(f"\n[COGS CSV Import] Done: {imported} imported, {skipped} skipped, {errors} errors")
    return {"imported": imported, "skipped": skipped, "errors": errors}
