"""Fetch real Amazon fees from Finances API and update orders."""
import requests
import time
import json
from datetime import datetime, timedelta
from . import config, db


def _get_token():
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": config.AMZ_CREDS.get("refresh_token", ""),
        "client_id": config.AMZ_CREDS.get("client_id", ""),
        "client_secret": config.AMZ_CREDS.get("client_secret", ""),
    })
    return r.json()["access_token"]


_token = None
_token_time = 0


def _headers():
    global _token, _token_time
    now = time.time()
    if not _token or now - _token_time > 3000:  # refresh every 50 min
        _token = _get_token()
        _token_time = now
    return {"x-amz-access-token": _token}


def _api_get(path, params=None):
    """GET with retry for rate limits."""
    url = f"{config.AMZ_API_BASE}{path}"
    for attempt in range(8):
        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"    [ConnectionError] retry in {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            time.sleep(3)
            continue
        if resp.status_code >= 500:
            time.sleep(5 * (attempt + 1))
            continue
        return resp.json()
    return {}


def fetch_all_financial_events(days_back=90):
    """Fetch all financial events for the period. Returns dict of order_id -> fees."""
    after = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")

    all_shipments = []
    all_refunds = []
    next_token = None
    page = 0

    while True:
        if next_token:
            params = {"NextToken": next_token, "MaxResultsPerPage": 100}
        else:
            params = {"PostedAfter": after, "MaxResultsPerPage": 100}

        data = _api_get("/finances/v0/financialEvents", params)
        events = data.get("payload", {}).get("FinancialEvents", {})

        shipments = events.get("ShipmentEventList", [])
        refunds = events.get("RefundEventList", [])
        all_shipments.extend(shipments)
        all_refunds.extend(refunds)

        next_token = data.get("payload", {}).get("NextToken")
        page += 1

        if page % 10 == 0:
            print(f"  Finances page {page}: {len(all_shipments)} shipments, {len(all_refunds)} refunds")

        if not next_token:
            break
        time.sleep(2)

    print(f"  Total: {len(all_shipments)} shipment events, {len(all_refunds)} refund events")

    # Process into per-order fee summary
    order_fees = {}

    for s in all_shipments:
        order_id = s.get("AmazonOrderId")
        if not order_id:
            continue

        if order_id not in order_fees:
            order_fees[order_id] = {
                "total_revenue": 0,  # Principal + Shipping (what buyer paid excl tax)
                "total_tax": 0,
                "commission": 0,     # Referral fee
                "fba_fee": 0,        # FBA fulfillment
                "other_fees": 0,     # Digital services, closing fees, etc
                "total_fees": 0,     # Sum of all fees (negative)
                "net_proceeds": 0,   # What seller actually receives
                "currency": "EUR",
                "refund_amount": 0,
            }

        for item in s.get("ShipmentItemList", []):
            for charge in item.get("ItemChargeList", []):
                amt = float(charge["ChargeAmount"]["CurrencyAmount"])
                cur = charge["ChargeAmount"]["CurrencyCode"]
                ctype = charge["ChargeType"]
                order_fees[order_id]["currency"] = cur

                if ctype == "Principal":
                    order_fees[order_id]["total_revenue"] += amt
                elif ctype in ("ShippingCharge",):
                    order_fees[order_id]["total_revenue"] += amt
                elif ctype in ("Tax", "ShippingTax", "GiftWrapTax"):
                    order_fees[order_id]["total_tax"] += amt

            for fee in item.get("ItemFeeList", []):
                amt = float(fee["FeeAmount"]["CurrencyAmount"])  # negative
                ftype = fee["FeeType"]

                if ftype == "Commission":
                    order_fees[order_id]["commission"] += amt
                elif ftype in ("ShippingHB", "FBAPerUnitFulfillmentFee", "FBAPerOrderFulfillmentFee",
                               "FBAWeightBasedFee"):
                    order_fees[order_id]["fba_fee"] += amt
                else:
                    order_fees[order_id]["other_fees"] += amt

                order_fees[order_id]["total_fees"] += amt

    # Process refunds
    for r in all_refunds:
        order_id = r.get("AmazonOrderId")
        if not order_id:
            continue

        if order_id not in order_fees:
            order_fees[order_id] = {
                "total_revenue": 0, "total_tax": 0, "commission": 0,
                "fba_fee": 0, "other_fees": 0, "total_fees": 0,
                "net_proceeds": 0, "currency": "EUR", "refund_amount": 0,
            }

        for item in r.get("ShipmentItemAdjustmentList", []):
            for charge in item.get("ItemChargeAdjustmentList", []):
                amt = float(charge["ChargeAmount"]["CurrencyAmount"])  # negative = refund
                ctype = charge["ChargeType"]
                if ctype == "Principal":
                    order_fees[order_id]["refund_amount"] += amt

            for fee in item.get("ItemFeeAdjustmentList", []):
                amt = float(fee["FeeAmount"]["CurrencyAmount"])  # positive = fee reversal
                order_fees[order_id]["total_fees"] += amt

    # Calculate net proceeds
    for oid, f in order_fees.items():
        f["net_proceeds"] = (f["total_revenue"] + f["total_tax"] + f["total_fees"] + f["refund_amount"])

    return order_fees


def update_orders_with_fees(conn, order_fees):
    """Update orders table with real Amazon fees."""
    updated = 0
    _H = {
        "apikey": config.SUPABASE_KEY,
        "Authorization": f"Bearer {config.SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    for order_id_amz, fees in order_fees.items():
        # Total platform fee = abs(commission + fba_fee + other_fees)
        total_fee = abs(fees["commission"]) + abs(fees["fba_fee"]) + abs(fees["other_fees"])

        resp = requests.patch(
            f"{config.SUPABASE_URL}/rest/v1/orders",
            headers=_H,
            params={"external_id": f"eq.{order_id_amz}"},
            json={
                "platform_fee": round(total_fee, 2),
                "notes": json.dumps({
                    "commission": round(fees["commission"], 2),
                    "fba_fee": round(fees["fba_fee"], 2),
                    "other_fees": round(fees["other_fees"], 2),
                    "refund": round(fees["refund_amount"], 2),
                    "tax": round(fees["total_tax"], 2),
                }),
            },
        )
        if resp.status_code in (200, 204):
            updated += 1

        if updated % 200 == 0 and updated > 0:
            print(f"  Updated {updated} orders...")

    print(f"  Total: {updated} orders updated with real fees")
    return updated


def sync_fees(conn, days_back=90):
    """Main entry: fetch financial events and update orders."""
    print("  Fetching Amazon financial events...")
    order_fees = fetch_all_financial_events(days_back)
    print(f"  Got fees for {len(order_fees)} orders")

    # Stats
    total_commission = sum(abs(f["commission"]) for f in order_fees.values())
    total_fba = sum(abs(f["fba_fee"]) for f in order_fees.values())
    total_other = sum(abs(f["other_fees"]) for f in order_fees.values())
    total_refunds = sum(f["refund_amount"] for f in order_fees.values())

    print(f"  Commission (referral): {total_commission:,.2f}")
    print(f"  FBA fees:              {total_fba:,.2f}")
    print(f"  Other fees:            {total_other:,.2f}")
    print(f"  Refunds:               {total_refunds:,.2f}")
    print(f"  Total fees:            {total_commission + total_fba + total_other:,.2f}")

    print("\n  Updating orders in DB...")
    update_orders_with_fees(conn, order_fees)
    return len(order_fees)


if __name__ == "__main__":
    conn = db.get_conn()
    sync_fees(conn)
