"""Amazon Advertising / PPC spend ETL module.

This module handles importing PPC (Pay-Per-Click) advertising costs into the
nesell-analytics database. Currently supports CSV import from Seller Central.

## How to get PPC data:

### Option 1: CSV Export from Seller Central (current)
1. Go to Amazon Seller Central > Advertising > Reports
2. Select "Sponsored Products" (or Brands/Display)
3. Choose report type: "Campaign" and date range
4. Download CSV
5. Run: python3.11 -m etl.run --ads-csv path/to/report.csv

### Option 2: Amazon Advertising API (future)
The Amazon Advertising API is SEPARATE from the SP-API. You need:
- A separate Amazon Advertising API application
- Register at: https://advertising.amazon.com/API
- Credentials: client_id, client_secret, refresh_token (different from SP-API)
- Scopes: advertising::campaign_management
- Store credentials in ~/.keys/amazon-ads-api.json

The SP-API does NOT provide detailed PPC data. However, settlement reports
contain aggregated advertising deductions (already captured in amazon_settlements
table when available).

### Option 3: Settlement-based estimation (fallback)
Settlement reports contain "Advertising" amount_type entries that show total
ad spend per settlement period. This provides aggregate data but no campaign-level
detail.
"""

import csv
import io
from datetime import date, timedelta
from pathlib import Path
from . import db, config

# Common CSV column name mappings for different Amazon report formats
_COLUMN_MAP = {
    # English
    "date": "date",
    "start date": "date",
    "campaign name": "campaign_name",
    "campaign type": "campaign_type",
    "impressions": "impressions",
    "clicks": "clicks",
    "spend": "spend",
    "cost": "spend",
    "7 day total sales": "sales",
    "14 day total sales": "sales",
    "total sales": "sales",
    "sales": "sales",
    "acos": "acos",
    "total advertising cost of sales (acos)": "acos",
    "roas": "roas",
    "total return on advertising spend (roas)": "roas",
    "7 day total orders (#)": "orders",
    "14 day total orders (#)": "orders",
    "total orders": "orders",
    "orders": "orders",
    "currency": "currency",
    # German (common in EU Seller Central)
    "datum": "date",
    "startdatum": "date",
    "kampagnenname": "campaign_name",
    "kampagnentyp": "campaign_type",
    "impressionen": "impressions",
    "klicks": "clicks",
    "ausgaben": "spend",
    "kosten": "spend",
    "umsatz": "sales",
    "bestellungen": "orders",
}


def _normalize_columns(headers: list[str]) -> dict[str, str]:
    """Map CSV headers to our standard column names."""
    mapping = {}
    for h in headers:
        h_lower = h.strip().lower()
        if h_lower in _COLUMN_MAP:
            mapping[h.strip()] = _COLUMN_MAP[h_lower]
    return mapping


def _parse_number(val: str) -> float:
    """Parse a number from CSV, handling various formats."""
    if not val:
        return 0.0
    # Remove currency symbols, spaces, percentage signs
    val = val.strip().replace("$", "").replace("\u20ac", "").replace("%", "").replace(" ", "")
    # Handle German number format (1.234,56 -> 1234.56)
    if "," in val and "." in val:
        if val.index(",") > val.index("."):
            # German format: 1.234,56
            val = val.replace(".", "").replace(",", ".")
        # else US format: 1,234.56 -> just remove commas
        else:
            val = val.replace(",", "")
    elif "," in val:
        # Could be German decimal (1234,56) or US thousands (1,234)
        parts = val.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            # Likely decimal: 1234,56
            val = val.replace(",", ".")
        else:
            # Likely thousands: 1,234
            val = val.replace(",", "")
    try:
        return float(val)
    except ValueError:
        return 0.0


def import_ads_csv(conn, csv_path: str, marketplace_id: str = None):
    """Import Amazon advertising report CSV into amazon_ad_spend table.

    Args:
        conn: DB connection (unused, REST API)
        csv_path: Path to the CSV file
        marketplace_id: Optional marketplace ID override (auto-detected if possible)

    Returns:
        Number of rows imported
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    print(f"  Reading advertising CSV: {path.name}")

    # Read file (handle BOM and different encodings)
    try:
        content = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        content = path.read_text(encoding="latin-1")

    # Parse CSV
    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames or []

    if not headers:
        print("  [WARN] Empty CSV file or no headers found")
        return 0

    col_map = _normalize_columns(headers)
    print(f"  Mapped columns: {col_map}")

    # Check required columns
    mapped_cols = set(col_map.values())
    if "date" not in mapped_cols or "spend" not in mapped_cols:
        print(f"  [ERROR] CSV missing required columns (date, spend). Found headers: {headers}")
        return 0

    records = []
    for row in reader:
        mapped = {}
        for orig_col, std_col in col_map.items():
            mapped[std_col] = row.get(orig_col, "")

        date_val = mapped.get("date", "").strip()
        if not date_val:
            continue

        # Parse date (try common formats)
        parsed_date = None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                from datetime import datetime
                parsed_date = datetime.strptime(date_val, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        if not parsed_date:
            print(f"  [WARN] Cannot parse date: {date_val}")
            continue

        spend = _parse_number(mapped.get("spend", "0"))
        if spend == 0:
            continue  # Skip rows with no spend

        record = {
            "date": parsed_date,
            "campaign_name": mapped.get("campaign_name", "Unknown Campaign").strip(),
            "campaign_type": _classify_campaign_type(mapped.get("campaign_type", "").strip()),
            "marketplace_id": marketplace_id or config.AMZ_CREDS.get("marketplace_id", "A1PA6795UKMFR9"),
            "impressions": int(_parse_number(mapped.get("impressions", "0"))),
            "clicks": int(_parse_number(mapped.get("clicks", "0"))),
            "spend": round(spend, 4),
            "sales": round(_parse_number(mapped.get("sales", "0")), 4),
            "acos": round(_parse_number(mapped.get("acos", "0")), 4),
            "roas": round(_parse_number(mapped.get("roas", "0")), 4),
            "orders": int(_parse_number(mapped.get("orders", "0"))),
            "currency": mapped.get("currency", "EUR").strip() or "EUR",
        }

        # Calculate ACOS/ROAS if not provided
        if record["acos"] == 0 and record["sales"] > 0:
            record["acos"] = round(record["spend"] / record["sales"] * 100, 4)
        if record["roas"] == 0 and record["spend"] > 0:
            record["roas"] = round(record["sales"] / record["spend"], 4)

        records.append(record)

    if not records:
        print("  No valid advertising records found in CSV")
        return 0

    # Upsert to database
    count = db.upsert_amazon_ad_spend(conn, records)
    total_spend = sum(r["spend"] for r in records)
    date_range = f"{min(r['date'] for r in records)} to {max(r['date'] for r in records)}"
    print(f"  Imported {count} ad spend records ({date_range})")
    print(f"  Total spend: {total_spend:,.2f} {records[0]['currency']}")
    return count


def _classify_campaign_type(raw_type: str) -> str:
    """Normalize campaign type to SP/SB/SD."""
    raw_lower = raw_type.lower() if raw_type else ""
    if "product" in raw_lower or raw_lower == "sp":
        return "SP"
    elif "brand" in raw_lower or raw_lower == "sb":
        return "SB"
    elif "display" in raw_lower or raw_lower == "sd":
        return "SD"
    elif raw_type:
        return raw_type[:10]
    return "SP"  # Default to Sponsored Products


def get_daily_ad_spend(conn, days_back: int = 90) -> list[dict]:
    """Load daily aggregated ad spend from database.

    Returns list of dicts with keys: date, spend, spend_pln, currency
    Used by aggregator and P&L page.
    """
    from datetime import date as date_type
    cutoff = str(date_type.today() - timedelta(days=days_back))

    try:
        rows = db._get("amazon_ad_spend", {
            "select": "date,spend,sales,currency",
            "date": f"gte.{cutoff}",
            "order": "date.asc",
        })
    except Exception:
        return []

    if not rows:
        return []

    # Aggregate by date
    from collections import defaultdict
    daily = defaultdict(lambda: {"spend": 0.0, "sales": 0.0, "currency": "EUR"})
    for r in rows:
        d = r["date"][:10]
        daily[d]["spend"] += float(r.get("spend", 0) or 0)
        daily[d]["sales"] += float(r.get("sales", 0) or 0)
        daily[d]["currency"] = r.get("currency", "EUR")

    return [
        {"date": d, "spend": v["spend"], "sales": v["sales"], "currency": v["currency"]}
        for d, v in sorted(daily.items())
    ]


def get_settlement_ad_spend(conn, days_back: int = 90) -> list[dict]:
    """Fallback: extract advertising costs from settlement data.

    Settlement reports may contain advertising deductions with
    amount_type = 'Advertising' or amount_description containing 'Sponsored'.
    """
    from datetime import date as date_type
    cutoff = str(date_type.today() - timedelta(days=days_back))

    try:
        # Look for advertising entries in settlements
        rows = db._get("amazon_settlements", {
            "select": "settlement_start_date,settlement_end_date,amount_type,amount_description,amount,currency",
            "amount_type": "ilike.*advertis*",
        })
        if not rows:
            # Try amount_description
            rows = db._get("amazon_settlements", {
                "select": "settlement_start_date,settlement_end_date,amount_type,amount_description,amount,currency",
                "amount_description": "ilike.*sponsor*",
            })
    except Exception:
        return []

    if not rows:
        return []

    # Sum up advertising amounts (they are typically negative = deductions)
    total = sum(abs(float(r.get("amount", 0) or 0)) for r in rows)
    print(f"  Found {len(rows)} advertising entries in settlements, total: {total:,.2f}")
    return rows
