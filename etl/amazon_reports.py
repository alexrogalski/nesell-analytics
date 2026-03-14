"""Amazon SP-API Reports: fetch all report-based data.

Report types:
  - Sales & Traffic (sessions, page views, Buy Box %)
  - FBA Inventory by Country
  - Storage Fees
  - Estimated FBA Fees per SKU
  - Customer Returns
  - Reimbursements
  - Settlement Reports
"""
import time
from datetime import datetime, timedelta
from . import amazon_api, db, config


def _safe_float(val, default=0.0):
    """Parse a float value, handling European comma decimals (e.g. '18,39' -> 18.39)."""
    if not val and val != 0:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(val)
    except (ValueError, TypeError):
        try:
            return float(str(val).replace(",", "."))
        except (ValueError, TypeError):
            return default


# ── Sales & Traffic Report ───────────────────────────────────────────

def sync_traffic(conn, days_back=30):
    """Fetch Sales & Traffic Report (JSON) — Buy Box %, sessions, page views per ASIN.

    GET_SALES_AND_TRAFFIC_REPORT returns JSON with per-ASIN and per-date data.
    Max 30 days per request. Uses CHILD asin granularity.
    """
    print("  [Traffic] Fetching Sales & Traffic Report...")
    end = datetime.utcnow()
    start = end - timedelta(days=min(days_back, 30))
    from datetime import date as date_type
    today = str(date_type.today())

    # Each marketplace gets its own report (traffic data is per-marketplace)
    all_traffic = []
    for mkt_id, plat_code in config.MARKETPLACE_TO_PLATFORM.items():
        print(f"    Fetching traffic for {plat_code}...")
        data = amazon_api.fetch_report_json(
            "GET_SALES_AND_TRAFFIC_REPORT",
            marketplace_ids=[mkt_id],
            start_date=start,
            end_date=end,
            report_options={
                "dateGranularity": "DAY",
                "asinGranularity": "CHILD",
            },
        )
        if not data:
            print(f"    No traffic data for {plat_code}")
            continue

        # Parse per-date aggregate data (daily totals per marketplace)
        for item in data.get("salesAndTrafficByDate", []):
            day = item.get("date", "")[:10]
            t = item.get("trafficByDate", {})
            s = item.get("salesByDate", {})
            sessions = (t.get("browserSessions", 0) or 0) + (t.get("mobileAppSessions", 0) or 0)
            page_views = (t.get("browserPageViews", 0) or 0) + (t.get("mobileAppPageViews", 0) or 0)
            buy_box_pct = t.get("buyBoxPercentage", 0) or 0
            revenue_data = s.get("orderedProductSales", {})

            all_traffic.append({
                "date": day,
                "asin": "__TOTAL__",
                "parent_asin": "",
                "marketplace_id": mkt_id,
                "sessions": sessions,
                "page_views": page_views,
                "buy_box_pct": round(buy_box_pct, 2),
                "units_ordered": s.get("unitsOrdered", 0) or 0,
                "units_ordered_b2b": s.get("unitsOrderedB2B", 0) or 0,
                "ordered_product_sales": float(revenue_data.get("amount", 0) or 0),
                "total_order_items": s.get("totalOrderItems", 0) or 0,
                "currency": revenue_data.get("currencyCode", "EUR"),
            })

        # Parse per-ASIN data (aggregated over period — use today as date)
        for item in data.get("salesAndTrafficByAsin", []):
            traffic = item.get("trafficByAsin", {})
            sales = item.get("salesByAsin", {})
            child_asin = item.get("childAsin", "")
            parent_asin = item.get("parentAsin", "")

            sessions = (traffic.get("browserSessions", 0) or 0) + (traffic.get("mobileAppSessions", 0) or 0)
            page_views = (traffic.get("browserPageViews", 0) or 0) + (traffic.get("mobileAppPageViews", 0) or 0)
            buy_box_pct = traffic.get("buyBoxPercentage", 0) or 0
            units = sales.get("unitsOrdered", 0) or 0
            units_b2b = sales.get("unitsOrderedB2B", 0) or 0
            revenue_data = sales.get("orderedProductSales", {})
            revenue = float(revenue_data.get("amount", 0) or 0)
            currency = revenue_data.get("currencyCode", "EUR")
            total_items = sales.get("totalOrderItems", 0) or 0

            all_traffic.append({
                "date": today,
                "asin": child_asin or parent_asin,
                "parent_asin": parent_asin,
                "marketplace_id": mkt_id,
                "sessions": sessions,
                "page_views": page_views,
                "buy_box_pct": round(buy_box_pct, 2),
                "units_ordered": units,
                "units_ordered_b2b": units_b2b,
                "ordered_product_sales": revenue,
                "total_order_items": total_items,
                "currency": currency,
            })

        time.sleep(3)  # respect rate limits between marketplace requests

    if all_traffic:
        count = db.upsert_amazon_traffic(conn, all_traffic)
        print(f"  [Traffic] Upserted {count} traffic records")
    else:
        print("  [Traffic] No data received")
    return len(all_traffic)


# ── FBA Inventory Report ─────────────────────────────────────────────

def sync_inventory_report(conn):
    """Fetch FBA Inventory by Country report (TSV).

    GET_AFN_INVENTORY_DATA_BY_COUNTRY: stock per SKU per country.
    """
    print("  [Inventory] Fetching FBA Inventory by Country...")
    rows = amazon_api.fetch_report_tsv("GET_AFN_INVENTORY_DATA_BY_COUNTRY")

    records = []
    for r in rows:
        records.append({
            "sku": r.get("seller-sku", ""),
            "fnsku": r.get("fulfillment-channel-sku", ""),
            "asin": r.get("asin", ""),
            "product_name": r.get("product-name", "")[:200],
            "country": r.get("country", ""),
            "fulfillable_qty": int(r.get("quantity-available", 0) or 0),
        })

    if records:
        count = db.upsert_amazon_inventory(conn, records)
        print(f"  [Inventory] Upserted {count} inventory records")
    return len(records)


# ── Storage Fees Report ──────────────────────────────────────────────

def _find_existing_report(report_type, status="DONE", page_size=20, prefer_widest=False):
    """Find an existing DONE report of the given type. Returns document ID or None.

    If prefer_widest=True, returns the report with the widest date range
    (useful for reimbursements where we want maximum coverage).
    Otherwise returns the most recent DONE report.
    """
    data = amazon_api.api_get("/reports/2021-06-30/reports", {
        "reportTypes": report_type,
        "pageSize": page_size,
        "processingStatuses": status,
    })
    reports = data.get("reports", [])

    if prefer_widest:
        # Find report with widest date range
        best = None
        best_range = 0
        for r in reports:
            doc_id = r.get("reportDocumentId")
            if not doc_id:
                continue
            try:
                s = datetime.fromisoformat(r["dataStartTime"].replace("+00:00", "").replace("Z", ""))
                e = datetime.fromisoformat(r["dataEndTime"].replace("+00:00", "").replace("Z", ""))
                rng = (e - s).days
                if rng > best_range:
                    best_range = rng
                    best = r
            except (KeyError, ValueError):
                pass
        if best:
            print(f"    Found existing report: {best.get('reportId')} "
                  f"(range: {best_range}d, created {best.get('createdTime', '?')[:16]})")
            return best.get("reportDocumentId")
    else:
        for r in reports:
            doc_id = r.get("reportDocumentId")
            if doc_id:
                print(f"    Found existing {status} report: {r.get('reportId')} "
                      f"(created {r.get('createdTime', '?')[:16]})")
                return doc_id
    return None


def sync_storage_fees(conn, months_back=3):
    """Fetch FBA Monthly Storage Fee charges (TSV).

    GET_FBA_STORAGE_FEE_CHARGES_DATA: estimated monthly storage fees per ASIN.
    This report requires a monthly date range (dataStartTime/dataEndTime).
    TSV columns use underscores (e.g. month_of_charge, not month-of-charge).
    Same ASIN can appear across multiple fulfillment centers, so we aggregate
    per (month, asin) since DB has UNIQUE(month, asin).
    """
    print("  [Storage] Fetching FBA Storage Fees...")

    all_records = []

    # Fetch storage fees month by month (report requires monthly range)
    now = datetime.utcnow()
    for i in range(months_back):
        # Calculate month boundaries
        month_end = now.replace(day=1) - timedelta(days=i * 1)
        if i > 0:
            month_end = month_end.replace(day=1) - timedelta(days=1)
            month_end = month_end.replace(day=1)
            # Go back to first of target month
            for _ in range(i - 1):
                month_end = month_end - timedelta(days=1)
                month_end = month_end.replace(day=1)

        # Simpler: just compute first/last of each past month
        year = now.year
        month = now.month - 1 - i  # previous months (storage fees are for completed months)
        while month <= 0:
            month += 12
            year -= 1
        # Last day of that month
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        start_date = datetime(year, month, 1)

        month_label = start_date.strftime("%Y-%m")
        print(f"    Requesting storage fees for {month_label}...")

        # First try to find an existing DONE report for this period
        rows = []
        existing_doc = _find_existing_report("GET_FBA_STORAGE_FEE_CHARGES_DATA")
        if existing_doc:
            rows = amazon_api.download_report_tsv(existing_doc)
            # Filter to the month we want
            rows = [r for r in rows if r.get("month_of_charge", "") == month_label]
            if rows:
                print(f"    Reusing existing report: {len(rows)} rows for {month_label}")

        if not rows:
            # Create a new report with the required date range
            rows = amazon_api.fetch_report_tsv(
                "GET_FBA_STORAGE_FEE_CHARGES_DATA",
                start_date=start_date,
                end_date=end_date,
            )

        if not rows:
            print(f"    No storage fee data for {month_label}")
            continue

        # Aggregate per (month, asin) since same ASIN may appear across
        # multiple fulfillment centers, but DB has UNIQUE(month, asin)
        asin_data = {}
        for r in rows:
            month_str = r.get("month_of_charge", "")
            asin = r.get("asin", "")
            if not month_str or not asin:
                continue
            key = (month_str, asin)
            if key not in asin_data:
                asin_data[key] = {
                    "month": month_str,
                    "asin": asin,
                    "fnsku": r.get("fnsku", ""),
                    "product_name": (r.get("product_name") or "")[:200],
                    "fulfillment_center": "AGGREGATED",
                    "country_code": r.get("country_code", ""),
                    "avg_qty": 0,
                    "avg_qty_pending_removal": 0,
                    "estimated_storage_fee": 0.0,
                    "currency": r.get("currency", "EUR"),
                    "product_size_tier": r.get("product_size_tier", ""),
                }
            entry = asin_data[key]
            entry["avg_qty"] += int(float(r.get("average_quantity_on_hand", 0) or 0))
            entry["avg_qty_pending_removal"] += int(float(r.get("average_quantity_pending_removal", 0) or 0))
            entry["estimated_storage_fee"] += float(r.get("estimated_monthly_storage_fee", 0) or 0)

        all_records.extend(asin_data.values())
        time.sleep(3)  # rate limit between month requests

    if all_records:
        count = db.upsert_amazon_storage_fees(conn, all_records)
        print(f"  [Storage] Upserted {count} storage fee records")
    else:
        print("  [Storage] No storage fee data received")
    return len(all_records)


# ── Estimated FBA Fees Report ────────────────────────────────────────

def sync_fba_fees(conn):
    """Fetch estimated FBA fees per SKU (TSV).

    GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA: referral, fulfillment, total fees per SKU.
    """
    print("  [FBA Fees] Fetching Estimated FBA Fees per SKU...")
    rows = amazon_api.fetch_report_tsv("GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA")

    records = []
    for r in rows:
        records.append({
            "sku": r.get("sku", ""),
            "fnsku": r.get("fnsku", ""),
            "asin": r.get("asin", ""),
            "product_name": (r.get("product-name") or "")[:200],
            "product_group": r.get("product-group", ""),
            "brand": r.get("brand", ""),
            "fulfillment_channel": r.get("fulfilled-by", ""),
            "your_price": float(r.get("your-price", 0) or 0),
            "sales_price": float(r.get("sales-price", 0) or 0),
            "estimated_referral_fee": float(r.get("estimated-referral-fee-per-unit", 0) or 0),
            "estimated_variable_closing_fee": float(r.get("estimated-variable-closing-fee", 0) or 0),
            "estimated_pick_pack_fee": float(r.get("estimated-pick-pack-fee-per-unit", 0) or 0),
            "estimated_weight_handling_fee": float(r.get("estimated-weight-handling-fee-per-unit", 0) or 0),
            "estimated_fee_total": float(r.get("estimated-fee-total", 0) or 0),
            "currency": r.get("currency", "EUR"),
            "size_tier": r.get("product-size-tier", ""),
            "item_weight_g": float(r.get("item-package-weight", 0) or 0),
        })

    if records:
        count = db.upsert_amazon_fba_fees(conn, records)
        print(f"  [FBA Fees] Upserted {count} FBA fee records")
    return len(records)


# ── Customer Returns Report ──────────────────────────────────────────

def sync_returns(conn, days_back=90):
    """Fetch FBA Customer Returns report (TSV).

    GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA: returns with reasons.
    """
    print("  [Returns] Fetching FBA Customer Returns...")
    start = datetime.utcnow() - timedelta(days=days_back)
    end = datetime.utcnow()

    rows = amazon_api.fetch_report_tsv(
        "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA",
        start_date=start,
        end_date=end,
    )

    records = []
    for r in rows:
        records.append({
            "return_date": r.get("return-date", "")[:10] or None,
            "order_id": r.get("order-id", ""),
            "sku": r.get("sku", ""),
            "asin": r.get("asin", ""),
            "fnsku": r.get("fnsku", ""),
            "product_name": (r.get("product-name") or "")[:200],
            "quantity": int(r.get("quantity", 1) or 1),
            "fulfillment_center_id": r.get("fulfillment-center-id", ""),
            "detailed_disposition": r.get("detailed-disposition", ""),
            "reason": r.get("reason", ""),
            "status": r.get("status", ""),
            "customer_comments": (r.get("customer-comments") or "")[:500],
        })

    if records:
        count = db.upsert_amazon_returns(conn, records)
        print(f"  [Returns] Inserted {count} return records")
    return len(records)


# ── Reimbursements Report ────────────────────────────────────────────

def sync_reimbursements(conn, days_back=90):
    """Fetch FBA Reimbursements report (TSV).

    GET_FBA_REIMBURSEMENTS_DATA: reimbursements for lost/damaged inventory.
    Strategy: first try to find existing DONE reports, then create new if needed.
    Report creation is heavily rate-limited so reusing existing reports is preferred.
    """
    print("  [Reimburse] Fetching FBA Reimbursements...")
    rows = []

    # Strategy 1: Try to find and download an existing DONE report (widest date range)
    existing_doc = _find_existing_report("GET_FBA_REIMBURSEMENTS_DATA", prefer_widest=True)
    if existing_doc:
        rows = amazon_api.download_report_tsv(existing_doc)
        if rows:
            print(f"    Reusing existing report: {len(rows)} rows")

    # Strategy 2: If no existing report, create a new one with longer wait
    if not rows:
        print("    No existing DONE report found, creating new...")
        start = datetime.utcnow() - timedelta(days=days_back)
        end = datetime.utcnow()

        # Use longer retry delay for report creation (rate limits are strict)
        time.sleep(10)  # pre-wait to avoid hitting rate limits from previous calls
        rows = amazon_api.fetch_report_tsv(
            "GET_FBA_REIMBURSEMENTS_DATA",
            start_date=start,
            end_date=end,
        )

    records = []
    for r in rows:
        records.append({
            "approval_date": r.get("approval-date", "")[:10] or None,
            "reimbursement_id": r.get("reimbursement-id", ""),
            "case_id": r.get("case-id", ""),
            "amazon_order_id": r.get("amazon-order-id", ""),
            "reason": r.get("reason", ""),
            "sku": r.get("sku", ""),
            "fnsku": r.get("fnsku", ""),
            "asin": r.get("asin", ""),
            "product_name": (r.get("product-name") or "")[:200],
            "condition": r.get("condition", ""),
            "currency_unit": r.get("currency-unit", "EUR"),
            "amount_per_unit": float(r.get("amount-per-unit", 0) or 0),
            "amount_total": float(r.get("amount-total", 0) or 0),
            "quantity_reimbursed_cash": int(r.get("quantity-reimbursed-cash", 0) or 0),
            "quantity_reimbursed_inventory": int(r.get("quantity-reimbursed-inventory", 0) or 0),
            "quantity_reimbursed_total": int(r.get("quantity-reimbursed-total", 0) or 0),
        })

    if records:
        count = db.upsert_amazon_reimbursements(conn, records)
        print(f"  [Reimburse] Upserted {count} reimbursement records")
    else:
        print("  [Reimburse] No reimbursement data available")
    return len(records)


# ── Settlement Report ────────────────────────────────────────────────

def sync_settlements(conn):
    """Fetch ALL available settlement reports (not just the latest).

    GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2: complete settlement data.
    Settlement reports are auto-generated by Amazon (~biweekly), we list all
    available DONE reports and download each one. Uses pagination to get all.
    Deduplication is handled by checking settlement_id in DB before inserting.
    """
    print("  [Settlement] Fetching all settlement reports...")

    # Collect all settlement report IDs with pagination
    all_reports = []
    next_token = None
    page = 0
    while True:
        params = {
            "reportTypes": "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
            "pageSize": 100,
            "processingStatuses": "DONE",
        }
        if next_token:
            params["nextToken"] = next_token

        data = amazon_api.api_get("/reports/2021-06-30/reports", params)
        reports = data.get("reports", [])
        all_reports.extend(reports)
        page += 1
        print(f"    Page {page}: found {len(reports)} reports")

        next_token = data.get("nextToken")
        if not next_token or not reports:
            break
        time.sleep(2)

    if not all_reports:
        print("    No settlement reports available")
        return 0

    print(f"    Total DONE settlement reports: {len(all_reports)}")

    # Get already-imported settlement IDs to avoid re-downloading
    existing_ids = set()
    try:
        existing = db._get("amazon_settlements", {
            "select": "settlement_id",
            "limit": "10000",
        })
        existing_ids = {r["settlement_id"] for r in existing if r.get("settlement_id")}
        if existing_ids:
            print(f"    Already have data for settlement IDs: {existing_ids}")
    except Exception:
        pass

    # Download and parse each report
    total_records = 0
    for report in all_reports:
        doc_id = report.get("reportDocumentId")
        if not doc_id:
            continue

        rows = amazon_api.download_report_tsv(doc_id)
        if not rows:
            continue

        # Check if we already have this settlement's data
        first_sid = ""
        for r in rows:
            sid = r.get("settlement-id", "")
            if sid:
                first_sid = sid
                break

        if first_sid and first_sid in existing_ids:
            print(f"    Skipping settlement {first_sid} (already imported)")
            continue

        # Parse records
        records = []
        for r in rows:
            amount_type = r.get("amount-type", "")
            if not amount_type:
                continue
            records.append({
                "settlement_id": r.get("settlement-id", ""),
                "settlement_start_date": r.get("settlement-start-date", ""),
                "settlement_end_date": r.get("settlement-end-date", ""),
                "order_id": r.get("order-id", ""),
                "sku": r.get("sku", ""),
                "amount_type": amount_type,
                "amount_description": r.get("amount-description", ""),
                "amount": _safe_float(r.get("amount")),
                "currency": r.get("currency", "EUR"),
                "marketplace_name": r.get("marketplace-name", ""),
            })

        if records:
            count = db.upsert_amazon_settlements(conn, records)
            settlement_id = records[0].get("settlement_id", "?")
            print(f"    Settlement {settlement_id}: stored {count} records")
            total_records += count
            existing_ids.add(settlement_id)

        time.sleep(5)  # rate limit between downloads (SP-API reports are heavily throttled)

    print(f"  [Settlement] Total stored: {total_records} settlement records")
    return total_records


# ── Master sync ──────────────────────────────────────────────────────

def sync_all_reports(conn, days_back=90):
    """Run all Amazon report syncs."""
    results = {}

    try:
        results["inventory"] = sync_inventory_report(conn)
    except Exception as e:
        print(f"  [ERROR] Inventory report: {e}")
        results["inventory"] = 0

    time.sleep(5)

    try:
        results["storage_fees"] = sync_storage_fees(conn)
    except Exception as e:
        print(f"  [ERROR] Storage fees report: {e}")
        results["storage_fees"] = 0

    time.sleep(5)

    try:
        results["fba_fees"] = sync_fba_fees(conn)
    except Exception as e:
        print(f"  [ERROR] FBA fees report: {e}")
        results["fba_fees"] = 0

    time.sleep(5)

    try:
        results["returns"] = sync_returns(conn, days_back)
    except Exception as e:
        print(f"  [ERROR] Returns report: {e}")
        results["returns"] = 0

    time.sleep(5)

    try:
        results["reimbursements"] = sync_reimbursements(conn, days_back)
    except Exception as e:
        print(f"  [ERROR] Reimbursements report: {e}")
        results["reimbursements"] = 0

    time.sleep(5)

    try:
        results["traffic"] = sync_traffic(conn, min(days_back, 30))
    except Exception as e:
        print(f"  [ERROR] Traffic report: {e}")
        results["traffic"] = 0

    print(f"\n  Reports sync complete: {results}")
    return results
