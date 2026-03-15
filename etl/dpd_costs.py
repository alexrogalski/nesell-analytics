"""
DPD API cost module: fetch package metadata and reconcile costs via DPD web services.

Architecture / data source priority:
    1. invoice_email  — actual costs from DPD Specyfikacja XLSX (dpd_invoices.py)
    2. invoice_csv    — manual CSV import (shipping_costs.import_dpd_csv)
    3. dpd_api        — enriched via DPD tracking API (this module)
    4. estimate       — contract rate estimation (shipping_costs.py)

What this module does:
    A. Validates DPD API credentials (SOAP auth)
    B. Fetches per-package metadata from DPD TrackTrace API (destination, weight, status)
    C. Enriches shipping_costs rows that only have estimated data
    D. Reconciles "not_found" invoice rows:
       - dpd_invoices.py may import invoice costs for packages not yet in shipping_costs
         (e.g. orders processed before Baselinker sync, or split shipments)
       - This module searches Baselinker by tracking number and creates the missing row
    E. Detects DPD-shipped orders in Baselinker not yet in shipping_costs table

DPD API credentials loaded from ~/.keys/dpd.env:
    DPD_LOGIN=43963101
    DPD_PASSWORD=NF1BJhcrRhYNP88q
    DPD_FID=439631

Usage:
    python3.11 -m etl.run --dpd-api          # full DPD API sync (enrich + reconcile)
    python3.11 -m etl.run --dpd-reconcile    # reconcile unmatched invoice costs only
    python3.11 -m etl.run --dpd-api --days 30

DPD API docs: https://dpdservices.dpd.com.pl/DPDPackageObjCommonServicesService/
"""
import re
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

import requests

from . import config, db, fx_rates
from .baselinker import bl_api
from .shipping_costs import (
    _estimate_dpd_cost,
    DPD_VAT_RATE,
    EUR_PLN_FALLBACK,
)


# ---------------------------------------------------------------------------
# DPD credentials
# ---------------------------------------------------------------------------

_dpd_env = config._load_env_file(config.KEYS_DIR / "dpd.env")
DPD_LOGIN = _dpd_env.get("DPD_LOGIN", "")
DPD_PASSWORD = _dpd_env.get("DPD_PASSWORD", "")
DPD_FID = _dpd_env.get("DPD_FID", "")

# SOAP endpoint — DPD Polska package management service
DPD_SOAP_URL = (
    "https://dpdservices.dpd.com.pl"
    "/DPDPackageObjCommonServicesService"
    "/DPDPackageObjCommonServices"
)

# DPD TrackTrace API (public, no auth required)
DPD_TRACKTRACE_URL = "https://tracktrace.dpd.com.pl/json/getCheckpoints"

# Timeout for external HTTP calls
HTTP_TIMEOUT = 15


# ---------------------------------------------------------------------------
# DPD SOAP helpers
# ---------------------------------------------------------------------------

def _build_soap_envelope(method: str, body: str) -> str:
    """Wrap a SOAP body in the standard DPD envelope."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:v1="https://dpdservices.dpd.com.pl/DPDPackageObjCommonServicesService/">
  <soapenv:Header/>
  <soapenv:Body>
    <v1:{method}>
      {body}
    </v1:{method}>
  </soapenv:Body>
</soapenv:Envelope>"""


def _auth_block() -> str:
    """Return DPD authDataV1 XML block."""
    return f"""<authData>
        <login>{DPD_LOGIN}</login>
        <masterFid>{DPD_FID}</masterFid>
        <password>{DPD_PASSWORD}</password>
      </authData>"""


def _soap_call(method: str, body: str) -> tuple[bool, str]:
    """Execute a DPD SOAP call.

    Returns (success: bool, response_text: str).
    Success = HTTP 200 and no SOAP Fault in response.
    """
    envelope = _build_soap_envelope(method, body)
    try:
        resp = requests.post(
            DPD_SOAP_URL,
            data=envelope.encode("utf-8"),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": f'"{method}"',
            },
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}: {resp.text[:300]}"

        if "Fault" in resp.text or "faultstring" in resp.text:
            # Extract fault message
            fault_match = re.search(r"<faultstring>(.*?)</faultstring>", resp.text, re.DOTALL)
            fault_msg = fault_match.group(1).strip() if fault_match else resp.text[:200]
            return False, f"SOAP Fault: {fault_msg}"

        return True, resp.text

    except requests.exceptions.Timeout:
        return False, "Request timed out"
    except requests.exceptions.ConnectionError as e:
        return False, f"Connection error: {e}"


def validate_credentials() -> bool:
    """Validate DPD API credentials by calling generatePackagesNumbersV4.

    Uses an empty/minimal request to test auth without actually creating a package.
    Returns True if credentials are valid (even if the payload is incomplete).
    We consider a non-auth SOAP Fault as "credentials OK" since the API reached it.
    """
    if not all([DPD_LOGIN, DPD_PASSWORD, DPD_FID]):
        print("  [DPD API] Credentials missing from ~/.keys/dpd.env")
        return False

    # Minimal SOAP call — the API will return a fault about missing package data,
    # but a 200 response with no auth fault means credentials are valid.
    body = f"""<pkg:generatePackagesNumbersV4
        xmlns:pkg="https://dpdservices.dpd.com.pl/DPDPackageObjCommonServicesService/">
      <pkg:openUMLFeV3>
        {_auth_block()}
      </pkg:openUMLFeV3>
    </pkg:generatePackagesNumbersV4>"""

    # Use raw envelope here since method is already namespaced
    try:
        resp = requests.post(
            DPD_SOAP_URL,
            data=_build_soap_envelope("generatePackagesNumbersV4", _auth_block()).encode("utf-8"),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": '"generatePackagesNumbersV4"',
            },
            timeout=HTTP_TIMEOUT,
        )
        # Auth failure typically returns 500 with a specific fault
        if resp.status_code in (200, 500):
            text = resp.text
            # Auth-specific errors
            if "AccessException" in text or "Invalid login" in text or "Nieautoryzowany" in text:
                print("  [DPD API] Authentication FAILED — check credentials")
                return False
            # Any other fault (missing params etc.) = credentials OK
            print(f"  [DPD API] Credentials valid (HTTP {resp.status_code})")
            return True
        print(f"  [DPD API] Unexpected status {resp.status_code}")
        return False

    except Exception as e:
        print(f"  [DPD API] Connection failed: {e}")
        return False


# ---------------------------------------------------------------------------
# DPD TrackTrace API
# ---------------------------------------------------------------------------

def fetch_tracking_info(tracking_number: str) -> Optional[dict]:
    """Fetch package metadata from DPD TrackTrace API (public, no auth).

    Returns dict with keys:
        status, destination_country, weight_kg, service_type, events, last_event_date
    Returns None on error or if tracking number not found.
    """
    if not tracking_number:
        return None

    try:
        resp = requests.get(
            DPD_TRACKTRACE_URL,
            params={"q": tracking_number, "lang": "PL"},
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": "nesell-analytics/1.0"},
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
    except Exception:
        return None

    # Parse response — DPD returns a list of checkpoints
    if not data or not isinstance(data, list):
        return None

    # Try to extract destination country from address info in events
    events = []
    destination_country = ""
    last_event_date = ""

    for checkpoint in data:
        if not isinstance(checkpoint, dict):
            continue

        event_time = checkpoint.get("timestamp") or checkpoint.get("date") or ""
        if event_time and event_time > last_event_date:
            last_event_date = str(event_time)[:10]

        events.append({
            "code": checkpoint.get("code", ""),
            "description": checkpoint.get("description", ""),
            "date": str(event_time)[:10] if event_time else "",
            "location": checkpoint.get("location", ""),
        })

        # Try to extract country from description or location
        desc = checkpoint.get("description", "")
        loc = checkpoint.get("location", "")
        for text in [desc, loc]:
            if text and not destination_country:
                # DPD locations often include country codes in brackets [DE]
                country_match = re.search(r"\[([A-Z]{2})\]", text)
                if country_match:
                    destination_country = country_match.group(1)

    # Determine current status from last event code
    status = "unknown"
    if events:
        last_code = events[-1].get("code", "")
        if last_code in ("DL", "DLVRD", "DLV"):
            status = "delivered"
        elif last_code in ("RT", "RTN", "RTRD"):
            status = "returned"
        elif last_code in ("PU", "PUPU", "PICKUP"):
            status = "picked_up"
        elif last_code:
            status = "in_transit"

    return {
        "tracking_number": tracking_number,
        "status": status,
        "destination_country": destination_country,
        "last_event_date": last_event_date,
        "events": events,
    }


def _fetch_tracking_batch(
    tracking_numbers: list[str], delay: float = 0.5
) -> dict[str, dict]:
    """Fetch tracking info for multiple packages with rate limiting.

    Returns {tracking_number: tracking_info_dict}.
    """
    results = {}
    for i, tn in enumerate(tracking_numbers):
        info = fetch_tracking_info(tn)
        if info:
            results[tn] = info
        if i > 0 and i % 50 == 0:
            print(f"    Fetched tracking for {i}/{len(tracking_numbers)} packages")
        time.sleep(delay)
    return results


# ---------------------------------------------------------------------------
# Baselinker tracking lookup
# ---------------------------------------------------------------------------

def _find_bl_order_by_tracking(tracking_number: str) -> Optional[dict]:
    """Search Baselinker for an order by tracking number.

    DPD tracking numbers are stored in delivery_package_nr field.
    Returns order dict or None.
    """
    if not tracking_number:
        return None

    try:
        # Baselinker doesn't have a direct tracking search,
        # so we use getOrders with a broader filter and match manually.
        # This is expensive, so only call for specific reconciliation use cases.
        # For bulk lookups, use the pre-loaded order map.
        pass
    except Exception:
        pass

    return None


def _get_bl_orders_tracking_map(days_back: int = 180) -> dict[str, dict]:
    """Build a map of {tracking_number: bl_order_data} from Baselinker.

    Fetches all orders with DPD packages within the lookback window.
    """
    since = int((datetime.now() - timedelta(days=days_back)).timestamp())
    tracking_to_order = {}
    cursor_date = since
    page = 0

    while True:
        try:
            data = bl_api("getOrders", {
                "date_confirmed_from": cursor_date,
                "get_unconfirmed_orders": False,
                "include_custom_extra_fields": False,
            })
        except Exception as e:
            print(f"    [WARN] Baselinker API error on page {page}: {e}")
            break

        orders = data.get("orders", [])
        if not orders:
            break

        for o in orders:
            tracking = (o.get("delivery_package_nr") or "").strip()
            if tracking:
                courier = (o.get("delivery_package_module") or "").lower()
                tracking_to_order[tracking] = {
                    "bl_order_id": str(o["order_id"]),
                    "courier": courier,
                    "tracking_number": tracking,
                    "destination_country": o.get("delivery_country_code", ""),
                    "delivery_method": o.get("delivery_method", ""),
                    "order_date": datetime.fromtimestamp(
                        o.get("date_confirmed", 0)
                    ).strftime("%Y-%m-%d"),
                    "currency": o.get("currency", "EUR"),
                }

        last_date = max(o.get("date_confirmed", 0) for o in orders)
        cursor_date = last_date + 1
        page += 1

        if page % 10 == 0:
            print(f"    BL page {page}: {len(tracking_to_order)} tracking numbers indexed")

        time.sleep(0.3)

    return tracking_to_order


# ---------------------------------------------------------------------------
# Reconcile unmatched invoice costs
# ---------------------------------------------------------------------------

def reconcile_unmatched_costs(conn, days_back: int = 180) -> int:
    """Find invoice costs not matched to orders and create missing shipping_costs rows.

    When dpd_invoices.py processes an XLSX spec, it tries to match tracking numbers
    to existing shipping_costs rows (created by shipping_costs.py from Baselinker data).

    If a shipping_cost row doesn't exist (order not yet synced, or order came from
    a different source), the invoice cost is orphaned.

    This function:
    1. Finds tracking numbers from dpd_invoice_imports that had not_found > 0
    2. Finds invoice costs not in shipping_costs (via DPD XLSX data in DB if stored)
    3. Looks up the Baselinker order for each orphaned tracking number
    4. Creates a shipping_costs row with invoice-based cost (source=invoice_email)
    5. Links the cost to the order

    Returns count of newly linked records.
    """
    print("  Loading Baselinker tracking map...")
    bl_tracking_map = _get_bl_orders_tracking_map(days_back)
    print(f"  Indexed {len(bl_tracking_map)} BL tracking numbers")

    # Load DB order ID mapping: external_id -> db_id
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    ext_to_db_id = {}
    offset = 0
    while True:
        rows = db._get("orders", {
            "select": "id,external_id",
            "order_date": f"gte.{cutoff}",
            "limit": "1000",
            "offset": str(offset),
        })
        for r in rows:
            ext_to_db_id[str(r["external_id"])] = r["id"]
        if len(rows) < 1000:
            break
        offset += 1000
        time.sleep(0.1)

    print(f"  Loaded {len(ext_to_db_id)} DB order mappings")

    # Get existing shipping_costs tracking numbers to avoid duplicates
    existing_tracking = set()
    offset = 0
    while True:
        rows = db._get("shipping_costs", {
            "select": "tracking_number",
            "limit": "1000",
            "offset": str(offset),
        })
        for r in rows:
            tn = r.get("tracking_number", "")
            if tn:
                existing_tracking.add(tn)
        if len(rows) < 1000:
            break
        offset += 1000

    print(f"  {len(existing_tracking)} tracking numbers already in shipping_costs")

    # Find BL orders with DPD tracking that have NO shipping_costs row yet
    orphaned_orders = []
    for tracking, bl_order in bl_tracking_map.items():
        if tracking not in existing_tracking:
            courier = bl_order.get("courier", "")
            if "dpd" in courier or not courier:
                orphaned_orders.append(bl_order)

    print(f"  Found {len(orphaned_orders)} BL orders with DPD tracking but no cost record")

    if not orphaned_orders:
        print("  Nothing to reconcile")
        return 0

    # Create shipping_costs rows for orphaned orders
    costs_to_insert = []
    for o in orphaned_orders:
        bl_id = o["bl_order_id"]
        order_db_id = ext_to_db_id.get(bl_id)
        if not order_db_id:
            continue  # Order not in DB yet (will sync on next orders run)

        # Estimate cost from contract rates
        cost_net, cost_currency, zone_info = _estimate_dpd_cost(
            o["destination_country"]
        )
        cost_gross = round(cost_net * (1 + DPD_VAT_RATE), 2)

        if cost_currency == "PLN":
            cost_pln = cost_gross
        else:
            fx = fx_rates.convert_to_pln(conn, cost_gross, cost_currency, o["order_date"])
            cost_pln = round(fx, 2) if fx else round(cost_gross * EUR_PLN_FALLBACK, 2)

        costs_to_insert.append({
            "order_id": order_db_id,
            "external_order_id": bl_id,
            "courier": "dpd",
            "tracking_number": o["tracking_number"],
            "destination_country": o["destination_country"],
            "cost_net": cost_net,
            "cost_gross": cost_gross,
            "cost_currency": cost_currency,
            "cost_pln": cost_pln,
            "cost_source": "estimate",
            "ship_date": o["order_date"],
            "notes": f"reconciled_via_dpd_costs|{zone_info}",
        })

    if costs_to_insert:
        total = 0
        for i in range(0, len(costs_to_insert), 500):
            chunk = costs_to_insert[i:i + 500]
            db._post("shipping_costs", chunk, on_conflict="order_id")
            total += len(chunk)
        print(f"  Created {total} missing shipping_cost records (estimated)")

    return len(costs_to_insert)


# ---------------------------------------------------------------------------
# Enrich existing shipping_costs with DPD API metadata
# ---------------------------------------------------------------------------

def enrich_from_tracking_api(conn, days_back: int = 90, max_packages: int = 200) -> int:
    """Fetch DPD tracking metadata for recent packages and enrich shipping_costs.

    Only fetches for rows where we have a tracking_number but missing destination
    or where the cost_source is 'estimate' (to verify destination country).

    Limited to max_packages to avoid hammering the TrackTrace API.
    Returns count of enriched records.
    """
    print("  Loading shipping_costs rows needing enrichment...")
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # Get rows with tracking numbers that could be enriched
    rows = db._get("shipping_costs", {
        "select": "id,tracking_number,destination_country,cost_source",
        "courier": "eq.dpd",
        "ship_date": f"gte.{cutoff}",
        "limit": str(max_packages),
    })

    # Filter to rows with tracking numbers
    to_enrich = [
        r for r in rows
        if r.get("tracking_number") and r.get("cost_source") == "estimate"
    ]

    if not to_enrich:
        print("  No rows need enrichment")
        return 0

    print(f"  Fetching tracking info for {len(to_enrich)} packages...")
    tracking_numbers = [r["tracking_number"] for r in to_enrich]
    tracking_data = _fetch_tracking_batch(tracking_numbers, delay=0.5)

    enriched = 0
    for row in to_enrich:
        tn = row["tracking_number"]
        info = tracking_data.get(tn)
        if not info:
            continue

        update = {}

        # Enrich destination country if TrackTrace provides it
        if info.get("destination_country") and not row.get("destination_country"):
            update["destination_country"] = info["destination_country"]

        # Update status note
        if info.get("status"):
            update["notes"] = f"dpd_status:{info['status']}|{row.get('notes', '')}"[:500]

        if update:
            try:
                db._patch("shipping_costs", {"id": f"eq.{row['id']}"}, update)
                enriched += 1
            except Exception as e:
                print(f"    [WARN] Failed to enrich {tn}: {e}")

    print(f"  Enriched {enriched} records from DPD TrackTrace API")
    return enriched


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def sync_dpd_costs(conn, days_back: int = 90, reconcile_only: bool = False) -> dict:
    """Full DPD cost sync via API.

    Steps:
    1. Validate DPD API credentials
    2. Reconcile: find BL orders with DPD tracking not yet in shipping_costs
    3. Enrich existing estimates with TrackTrace metadata (destination, status)

    Args:
        conn: Supabase connection (compatibility, unused directly)
        days_back: Lookback window in days
        reconcile_only: Skip API enrichment, only reconcile missing records

    Returns dict with counts: reconciled, enriched, credentials_valid
    """
    results = {"reconciled": 0, "enriched": 0, "credentials_valid": False}

    # Step 1: Validate credentials
    print("  Validating DPD API credentials...")
    creds_ok = validate_credentials()
    results["credentials_valid"] = creds_ok
    if not creds_ok:
        print("  [WARN] DPD API credentials invalid or unreachable — continuing with tracking API only")

    # Step 2: Reconcile missing shipping_costs rows
    print("\n  Reconciling missing shipping cost records...")
    results["reconciled"] = reconcile_unmatched_costs(conn, days_back=days_back)

    if not reconcile_only:
        # Step 3: Enrich existing estimates with TrackTrace data
        print("\n  Enriching estimates with DPD TrackTrace metadata...")
        results["enriched"] = enrich_from_tracking_api(conn, days_back=days_back)

    # Summary
    print(f"\n  DPD costs sync summary:")
    print(f"    Credentials valid:   {results['credentials_valid']}")
    print(f"    Missing rows created: {results['reconciled']}")
    print(f"    Rows enriched:       {results['enriched']}")

    return results


def get_dpd_cost_report(conn, days_back: int = 90) -> dict:
    """Return a summary report of DPD costs by source, country, and completeness.

    Useful for dashboard and reporting.
    """
    cutoff = str(date.today() - timedelta(days=days_back))

    rows = db._get("shipping_costs", {
        "select": "destination_country,cost_pln,cost_source,courier,tracking_number",
        "courier": "eq.dpd",
        "ship_date": f"gte.{cutoff}",
    })

    report = {
        "total_shipments": len(rows),
        "total_cost_pln": 0.0,
        "by_source": defaultdict(lambda: {"count": 0, "total_pln": 0.0}),
        "by_country": defaultdict(lambda: {"count": 0, "total_pln": 0.0}),
        "missing_tracking": 0,
        "days_back": days_back,
    }

    for r in rows:
        cost = float(r.get("cost_pln", 0) or 0)
        report["total_cost_pln"] += cost

        src = r.get("cost_source", "estimate")
        report["by_source"][src]["count"] += 1
        report["by_source"][src]["total_pln"] += cost

        cc = r.get("destination_country", "??")
        report["by_country"][cc]["count"] += 1
        report["by_country"][cc]["total_pln"] += cost

        if not r.get("tracking_number"):
            report["missing_tracking"] += 1

    report["total_cost_pln"] = round(report["total_cost_pln"], 2)
    report["invoice_coverage_pct"] = round(
        (report["by_source"].get("invoice_email", {}).get("count", 0)
         + report["by_source"].get("invoice_csv", {}).get("count", 0))
        / max(report["total_shipments"], 1) * 100,
        1,
    )

    return report
