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


# ── Sales & Traffic Report ───────────────────────────────────────────

def sync_traffic(conn, days_back=30):
    """Fetch Sales & Traffic Report (JSON) — Buy Box %, sessions, page views per ASIN.

    GET_SALES_AND_TRAFFIC_REPORT returns JSON with per-ASIN and per-date data.
    Max 30 days per request. Uses CHILD asin granularity.
    """
    print("  [Traffic] Fetching Sales & Traffic Report...")
    end = datetime.utcnow()
    start = end - timedelta(days=min(days_back, 30))

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

        # Parse per-ASIN data
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

        # Parse per-date aggregate data
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

def sync_storage_fees(conn):
    """Fetch FBA Monthly Storage Fee charges (TSV).

    GET_FBA_STORAGE_FEE_CHARGES_DATA: estimated monthly storage fees per ASIN.
    """
    print("  [Storage] Fetching FBA Storage Fees...")
    rows = amazon_api.fetch_report_tsv("GET_FBA_STORAGE_FEE_CHARGES_DATA")

    records = []
    for r in rows:
        month_str = r.get("month-of-charge", "")
        records.append({
            "month": month_str,
            "asin": r.get("asin", ""),
            "fnsku": r.get("fnsku", ""),
            "product_name": (r.get("product-name") or "")[:200],
            "fulfillment_center": r.get("fulfillment-center", ""),
            "country_code": r.get("country-code", ""),
            "avg_qty": int(r.get("average-quantity-on-hand", 0) or 0),
            "avg_qty_pending_removal": int(r.get("average-quantity-pending-removal", 0) or 0),
            "estimated_storage_fee": float(r.get("estimated-monthly-storage-fee", 0) or 0),
            "currency": r.get("currency", "EUR"),
            "product_size_tier": r.get("product-size-tier", ""),
        })

    if records:
        count = db.upsert_amazon_storage_fees(conn, records)
        print(f"  [Storage] Upserted {count} storage fee records")
    return len(records)


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
    """
    print("  [Reimburse] Fetching FBA Reimbursements...")
    start = datetime.utcnow() - timedelta(days=days_back)
    end = datetime.utcnow()

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
        print(f"  [Reimburse] Inserted {count} reimbursement records")
    return len(records)


# ── Settlement Report ────────────────────────────────────────────────

def sync_settlements(conn):
    """Fetch latest settlement report.

    GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2: complete settlement data.
    Settlement reports are auto-generated, we just need to list and download.
    """
    print("  [Settlement] Fetching latest settlement reports...")

    # List available settlement reports (Amazon auto-generates these)
    data = amazon_api.api_get("/reports/2021-06-30/reports", {
        "reportTypes": "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
        "pageSize": 10,
    })
    reports = data.get("reports", [])
    if not reports:
        print("    No settlement reports available")
        return 0

    # Download the most recent one
    latest = reports[0]
    doc_id = latest.get("reportDocumentId")
    if not doc_id:
        print("    No document ID in latest settlement report")
        return 0

    rows = amazon_api.download_report_tsv(doc_id)
    print(f"  [Settlement] Got {len(rows)} settlement rows")

    # Parse and store key settlement data
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
            "amount": float(r.get("amount", 0) or 0),
            "currency": r.get("currency", "EUR"),
            "marketplace_name": r.get("marketplace-name", ""),
        })

    # Store as raw data for now (settlement analysis)
    if records:
        count = db.upsert_amazon_settlements(conn, records)
        print(f"  [Settlement] Stored {count} settlement records")
    return len(records)


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
