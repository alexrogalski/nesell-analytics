"""Sync tracking numbers from Printful back to Baselinker (and Amazon).

Flow:
1. Query printful_order_mappings for orders needing tracking sync
2. Check Printful API for shipment/tracking updates
3. Update Baselinker order with tracking number via setOrderFields
4. Confirm shipment on Amazon via SP-API Feeds (if Amazon order)
5. Update printful_order_mappings record with sync status

Uses the existing printful_order_mappings table (20260309 migration) with
additional columns added by 20260309_tracking_sync_columns migration:
  bl_tracking_synced, amz_tracking_synced, bl_synced_at, amz_synced_at, last_error

Requires:
- Printful API token (v1) from ~/.keys/printful.env
- Baselinker API token from ~/.keys/baselinker.env
- Amazon SP-API credentials from ~/.keys/amazon-sp-api.json
- Supabase credentials from ~/nesell-analytics/.env

Usage:
    python3.11 -m etl.tracking_sync              # sync all pending
    python3.11 -m etl.tracking_sync --dry-run    # check without updating
    python3.11 -m etl.tracking_sync --register 12345 --bl-order 67890
"""

import requests
import json
import time
import re
import argparse
from datetime import datetime
from pathlib import Path

# --- Config (reuse project patterns) ---

KEYS_DIR = Path.home() / ".keys"

TABLE = "printful_order_mappings"


def _load_env(path: Path) -> dict:
    vals = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip().replace("\r", "")
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip()
    return vals


# Printful
_pf = _load_env(KEYS_DIR / "printful.env")
PRINTFUL_TOKEN = _pf.get("PRINTFUL_API_TOKEN", "")
PRINTFUL_STORE_ID = _pf.get("PRINTFUL_STORE_ID", "15269225")

# Baselinker
_bl = _load_env(KEYS_DIR / "baselinker.env")
BASELINKER_TOKEN = _bl.get("BASELINKER_API_TOKEN", "")
BASELINKER_URL = "https://api.baselinker.com/connector.php"

# Supabase
_supa = _load_env(Path(__file__).parent.parent / ".env")
SUPABASE_URL = _supa.get("SUPABASE_URL", "")
SUPABASE_KEY = _supa.get("SUPABASE_KEY", "")

# Amazon SP-API
_amz_path = KEYS_DIR / "amazon-sp-api.json"
AMZ_CREDS = json.loads(_amz_path.read_text()) if _amz_path.exists() else {}
AMZ_API_BASE = "https://sellingpartnerapi-eu.amazon.com"

# All EU marketplace IDs (for Amazon feeds)
EU_MARKETPLACE_IDS = [
    "A1PA6795UKMFR9", "A13V1IB3VIYZZH", "A1RKKUPIHCS9HS",
    "APJ6JRA9NG5V4", "A1805IZSGTT6HS", "A1C3SOZRARQ6R3",
    "A2NODRKZP88ZB9", "AMEN7PMS3EDWL", "A1F83G8C2ARO7P",
]


# --- API Helpers ---

PF_HEADERS = {
    "Authorization": f"Bearer {PRINTFUL_TOKEN}",
    "X-PF-Store-Id": PRINTFUL_STORE_ID,
    "Content-Type": "application/json",
}

SUPA_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}


def pf_get(path: str, params: dict | None = None) -> dict | None:
    """GET request to Printful API with retry on rate limit."""
    for attempt in range(4):
        try:
            r = requests.get(
                f"https://api.printful.com{path}",
                headers=PF_HEADERS,
                params=params or {},
                timeout=30,
            )
        except requests.exceptions.ConnectionError:
            print(f"  [Printful] ConnectionError on {path}, retry {attempt+1}/4")
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  [Printful] Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return None
        print(f"  [Printful] GET {path} -> {r.status_code}: {r.text[:200]}")
        return None
    return None


def bl_api(method: str, params: dict | None = None) -> dict:
    """Call Baselinker API with rate limit retry (matches baselinker.py pattern)."""
    for attempt in range(5):
        resp = requests.post(BASELINKER_URL, data={
            "token": BASELINKER_TOKEN,
            "method": method,
            "parameters": json.dumps(params or {}),
        })
        data = resp.json()
        if data.get("status") == "ERROR":
            msg = data.get("error_message", "")
            if "limit exceeded" in msg.lower() or "blocked until" in msg.lower():
                match = re.search(r"blocked until (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", msg)
                if match:
                    blocked_until = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
                    wait = max((blocked_until - datetime.now()).total_seconds() + 5, 30)
                else:
                    wait = 60 * (attempt + 1)
                print(f"  [BL Rate limit] Waiting {wait:.0f}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
                continue
            raise Exception(f"Baselinker {method}: {msg}")
        return data
    raise Exception(f"Baselinker {method}: rate limit exceeded after 5 retries")


def supa_get(table: str, params: dict | None = None) -> list[dict]:
    """GET from Supabase REST API."""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=SUPA_HEADERS,
        params=params or {},
    )
    if r.status_code != 200:
        raise Exception(f"Supabase GET {table}: {r.status_code} {r.text[:200]}")
    return r.json()


def supa_post(table: str, data: list[dict] | dict, on_conflict: str | None = None) -> list[dict]:
    """POST (upsert) to Supabase REST API."""
    headers = dict(SUPA_HEADERS)
    params = {}
    if on_conflict:
        headers["Prefer"] = "return=representation,resolution=merge-duplicates"
        params["on_conflict"] = on_conflict
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=headers,
        json=data,
        params=params,
    )
    if r.status_code not in (200, 201):
        raise Exception(f"Supabase POST {table}: {r.status_code} {r.text[:200]}")
    return r.json()


def supa_patch(table: str, match_params: dict, data: dict) -> list[dict]:
    """PATCH rows matching filter in Supabase."""
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=SUPA_HEADERS,
        json=data,
        params=match_params,
    )
    if r.status_code not in (200, 204):
        raise Exception(f"Supabase PATCH {table}: {r.status_code} {r.text[:200]}")
    return r.json() if r.text else []


def _get_amazon_token() -> str:
    """Get fresh Amazon access token via LWA."""
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": AMZ_CREDS.get("refresh_token", ""),
        "client_id": AMZ_CREDS.get("client_id", ""),
        "client_secret": AMZ_CREDS.get("client_secret", ""),
    })
    return r.json()["access_token"]


def amz_post(path: str, payload: dict) -> dict | None:
    """POST to Amazon SP-API with retry."""
    for attempt in range(5):
        token = _get_amazon_token()
        headers = {
            "x-amz-access-token": token,
            "Content-Type": "application/json",
        }
        try:
            r = requests.post(
                f"{AMZ_API_BASE}{path}",
                headers=headers,
                json=payload,
                timeout=30,
            )
        except requests.exceptions.ConnectionError:
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"  [Amazon] Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code in (200, 202):
            return r.json()
        if r.status_code == 403:
            time.sleep(3)
            continue
        print(f"  [Amazon] POST {path} -> {r.status_code}: {r.text[:300]}")
        return None
    return None


# --- Carrier mapping ---
# Maps Printful carrier names to Baselinker courier module codes and Amazon carrier codes.
CARRIER_MAP = {
    "DPD":          {"bl_courier": "dpd",         "amazon_carrier": "DPD"},
    "DHL":          {"bl_courier": "dhl",         "amazon_carrier": "DHL"},
    "DHL EXPRESS":  {"bl_courier": "dhl",         "amazon_carrier": "DHL"},
    "FEDEX":        {"bl_courier": "fedex",       "amazon_carrier": "FedEx"},
    "UPS":          {"bl_courier": "ups",         "amazon_carrier": "UPS"},
    "USPS":         {"bl_courier": "usps",        "amazon_carrier": "USPS"},
    "TNT":          {"bl_courier": "tnt",         "amazon_carrier": "TNT"},
    "GLS":          {"bl_courier": "gls",         "amazon_carrier": "GLS"},
    "HERMES":       {"bl_courier": "hermes",      "amazon_carrier": "Hermes"},
    "ROYAL MAIL":   {"bl_courier": "royal_mail",  "amazon_carrier": "Royal Mail"},
    "POSTI":        {"bl_courier": "posti",       "amazon_carrier": "Posti"},
    "POSTNL":       {"bl_courier": "postnl",      "amazon_carrier": "PostNL"},
    "COLISSIMO":    {"bl_courier": "colissimo",   "amazon_carrier": "Colissimo"},
    "CORREOS":      {"bl_courier": "correos",     "amazon_carrier": "Correos"},
    "BPOST":        {"bl_courier": "bpost",       "amazon_carrier": "bpost"},
    "POSTEN":       {"bl_courier": "posten",      "amazon_carrier": "Posten"},
    "LATVIJAS PASTS": {"bl_courier": "latvijas_pasts", "amazon_carrier": "Other"},
}


def _normalize_carrier(carrier_name: str) -> dict:
    """Normalize Printful carrier name to BL/Amazon codes."""
    if not carrier_name:
        return {"bl_courier": "other", "amazon_carrier": "Other"}
    upper = carrier_name.upper().strip()
    if upper in CARRIER_MAP:
        return CARRIER_MAP[upper]
    for key, val in CARRIER_MAP.items():
        if key in upper or upper in key:
            return val
    return {"bl_courier": "other", "amazon_carrier": "Other"}


# ============================================================
# Core functions
# ============================================================


def get_pending_printful_orders() -> list[dict]:
    """Query printful_order_mappings for orders needing tracking sync.

    Fetches rows where:
    - status NOT IN ('delivered', 'cancelled', 'error')
    - Either no tracking yet OR tracking exists but not synced to BL

    Returns list of dicts from the printful_order_mappings table.
    """
    # Two-step query: first get orders missing tracking, then those with
    # tracking but not yet synced to BL/Amazon.
    #
    # PostgREST OR filter:
    #   tracking_number is null  => needs Printful check
    #   bl_tracking_synced is false AND tracking_number is not null  => needs BL sync
    rows = supa_get(TABLE, {
        "select": "*",
        "or": "(tracking_number.is.null,bl_tracking_synced.is.false)",
        "status": "not.in.(delivered,cancelled)",
        "order": "created_at.asc",
        "limit": "500",
    })

    print(f"  Found {len(rows)} pending orders in {TABLE}")
    return rows


def check_tracking_updates(printful_order_ids: list[int]) -> list[dict]:
    """Check Printful API for tracking updates on given orders.

    For each Printful order, fetches order detail and extracts shipment info.

    Args:
        printful_order_ids: List of Printful order IDs to check.

    Returns:
        List of dicts with tracking info per order:
        {printful_order_id, tracking_number, tracking_url, carrier,
         ship_date, printful_status}
    """
    results = []

    for i, pf_order_id in enumerate(printful_order_ids):
        data = pf_get(f"/orders/{pf_order_id}")

        if not data or not data.get("result"):
            print(f"  WARNING: Could not fetch Printful order {pf_order_id}")
            results.append({
                "printful_order_id": pf_order_id,
                "tracking_number": None,
                "tracking_url": None,
                "carrier": None,
                "ship_date": None,
                "printful_status": "unknown",
            })
            time.sleep(0.3)
            continue

        order = data["result"]
        pf_status = order.get("status", "unknown")

        # Printful stores shipments in the "shipments" array
        shipments = order.get("shipments", [])
        tracking_number = None
        tracking_url = None
        carrier = None
        ship_date = None

        if shipments:
            # Use the most recent shipment (last in array)
            latest = shipments[-1]
            tracking_number = latest.get("tracking_number") or None
            tracking_url = latest.get("tracking_url") or None
            carrier = latest.get("carrier") or latest.get("service") or None
            ship_date = latest.get("ship_date") or latest.get("created")

        # Fallback: some orders have tracking at the order level
        if not tracking_number:
            tracking_number = order.get("tracking_number") or None
            tracking_url = order.get("tracking_url") or None
            carrier = order.get("carrier") or carrier

        results.append({
            "printful_order_id": pf_order_id,
            "tracking_number": tracking_number,
            "tracking_url": tracking_url,
            "carrier": carrier,
            "ship_date": ship_date,
            "printful_status": pf_status,
        })

        if (i + 1) % 20 == 0:
            print(f"  Checked {i+1}/{len(printful_order_ids)} Printful orders...")

        time.sleep(0.3)  # respect rate limits

    return results


def update_baselinker_tracking(
    order_id: int,
    tracking_number: str,
    carrier: str,
    tracking_url: str | None = None,
) -> bool:
    """Update Baselinker order with tracking number and carrier.

    Uses setOrderFields to set:
    - delivery_package_nr  (tracking number)
    - delivery_package_module  (courier code, if recognized)

    Args:
        order_id: Baselinker order ID (numeric).
        tracking_number: Shipment tracking number.
        carrier: Carrier name from Printful.
        tracking_url: Optional tracking URL (logged, not sent to BL).

    Returns:
        True if update succeeded, False otherwise.
    """
    carrier_info = _normalize_carrier(carrier)

    fields: dict = {
        "order_id": order_id,
        "delivery_package_nr": tracking_number,
    }

    # Only set courier module if we have a recognized mapping
    if carrier_info["bl_courier"] != "other":
        fields["delivery_package_module"] = carrier_info["bl_courier"]

    try:
        result = bl_api("setOrderFields", fields)
        if result.get("status") == "SUCCESS":
            print(f"  [BL] Updated order {order_id}: tracking={tracking_number}, carrier={carrier}")
            return True
        else:
            print(f"  [BL] Unexpected response for order {order_id}: {result}")
            return False
    except Exception as e:
        print(f"  [BL] Error updating order {order_id}: {e}")
        return False


def confirm_amazon_shipment(
    amazon_order_id: str,
    tracking_number: str,
    carrier: str,
    ship_date: str | None = None,
) -> bool:
    """Confirm shipment on Amazon via SP-API Feeds API.

    Submits a POST_ORDER_FULFILLMENT_DATA feed that tells Amazon the order
    has been shipped with the given tracking number and carrier.

    The feed process is 3 steps:
    1. Create a feed document (get presigned upload URL)
    2. Upload the XML feed content to the presigned URL
    3. Create the feed referencing the document

    Args:
        amazon_order_id: Amazon order ID (e.g., "305-1234567-8901234").
        tracking_number: Shipment tracking number.
        carrier: Carrier name (will be mapped to Amazon-recognized name).
        ship_date: ISO format ship date. Defaults to now.

    Returns:
        True if feed was created successfully, False otherwise.
    """
    if not amazon_order_id:
        return False

    carrier_info = _normalize_carrier(carrier)
    amazon_carrier = carrier_info["amazon_carrier"]

    if not ship_date:
        ship_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    elif "T" not in ship_date:
        ship_date = f"{ship_date}T00:00:00Z"

    feed_content = _build_fulfillment_feed(
        amazon_order_id, tracking_number, amazon_carrier, ship_date
    )

    try:
        # Step 1: Create feed document
        doc_resp = amz_post("/feeds/2021-06-30/documents", {
            "contentType": "text/xml; charset=UTF-8",
        })
        if not doc_resp:
            print(f"  [Amazon] Failed to create feed document for {amazon_order_id}")
            return False

        feed_doc_id = doc_resp.get("feedDocumentId")
        upload_url = doc_resp.get("url")

        if not feed_doc_id or not upload_url:
            print(f"  [Amazon] Missing feed document info for {amazon_order_id}")
            return False

        # Step 2: Upload feed content to the presigned URL
        upload_resp = requests.put(
            upload_url,
            data=feed_content.encode("utf-8"),
            headers={"Content-Type": "text/xml; charset=UTF-8"},
            timeout=30,
        )
        if upload_resp.status_code not in (200, 204):
            print(f"  [Amazon] Feed upload failed ({upload_resp.status_code}) for {amazon_order_id}")
            return False

        # Step 3: Create the feed
        feed_resp = amz_post("/feeds/2021-06-30/feeds", {
            "feedType": "POST_ORDER_FULFILLMENT_DATA",
            "marketplaceIds": EU_MARKETPLACE_IDS,
            "inputFeedDocumentId": feed_doc_id,
        })

        if feed_resp:
            feed_id = feed_resp.get("feedId")
            print(f"  [Amazon] Fulfillment feed created: {feed_id} for order {amazon_order_id}")
            return True
        else:
            print(f"  [Amazon] Failed to create feed for {amazon_order_id}")
            return False

    except Exception as e:
        print(f"  [Amazon] Error confirming shipment for {amazon_order_id}: {e}")
        return False


def _build_fulfillment_feed(
    order_id: str,
    tracking_number: str,
    carrier: str,
    ship_date: str,
) -> str:
    """Build XML feed content for POST_ORDER_FULFILLMENT_DATA.

    Amazon expects a specific XML envelope format for order fulfillment.
    See: https://developer-docs.amazon.com/sp-api/docs/feeds-api-v2021-06-30-use-case-guide
    """
    fulfillment_date = ship_date if "T" in ship_date else f"{ship_date}T00:00:00Z"

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="amzn-envelope.xsd">
  <Header>
    <DocumentVersion>1.01</DocumentVersion>
    <MerchantIdentifier>{AMZ_CREDS.get('seller_id', '')}</MerchantIdentifier>
  </Header>
  <MessageType>OrderFulfillment</MessageType>
  <Message>
    <MessageID>1</MessageID>
    <OrderFulfillment>
      <AmazonOrderID>{order_id}</AmazonOrderID>
      <FulfillmentDate>{fulfillment_date}</FulfillmentDate>
      <FulfillmentData>
        <CarrierName>{carrier}</CarrierName>
        <ShippingMethod>Standard</ShippingMethod>
        <ShipperTrackingNumber>{tracking_number}</ShipperTrackingNumber>
      </FulfillmentData>
    </OrderFulfillment>
  </Message>
</AmazonEnvelope>"""
    return xml


def _update_mapping_record(
    record_id: int,
    tracking_number: str | None = None,
    tracking_url: str | None = None,
    carrier: str | None = None,
    printful_status: str | None = None,
    bl_synced: bool = False,
    amz_synced: bool = False,
    error_msg: str | None = None,
) -> None:
    """Update a printful_order_mappings record in Supabase.

    Only sets fields that are provided (non-None). The updated_at trigger
    handles timestamp automatically.
    """
    update_data: dict = {}

    if tracking_number is not None:
        update_data["tracking_number"] = tracking_number
    if tracking_url is not None:
        update_data["tracking_url"] = tracking_url
    if carrier is not None:
        update_data["carrier"] = carrier
    if error_msg is not None:
        update_data["error_message"] = error_msg

    # Sync status flags (added by tracking_sync migration)
    if bl_synced:
        update_data["bl_tracking_synced"] = True
        update_data["bl_synced_at"] = datetime.utcnow().isoformat()
    if amz_synced:
        update_data["amz_tracking_synced"] = True
        update_data["amz_synced_at"] = datetime.utcnow().isoformat()

    # Determine status progression
    if tracking_number and bl_synced:
        update_data["status"] = "shipped"
    elif tracking_number:
        # Has tracking but not yet synced to BL
        update_data["status"] = "shipped"
    elif printful_status and printful_status != "unknown":
        update_data["status"] = printful_status

    if update_data:
        supa_patch(TABLE, {"id": f"eq.{record_id}"}, update_data)


def sync_all_tracking(dry_run: bool = False) -> dict:
    """Main function: check all pending orders, update tracking where available.

    Steps:
    1. Get pending orders from printful_order_mappings
    2. For orders without tracking: check Printful API
    3. For orders with tracking but not synced:
       a. Update Baselinker order with tracking via setOrderFields
       b. If Amazon order, confirm shipment via Feeds API
       c. Update DB record
    4. Return summary

    Args:
        dry_run: If True, check Printful for tracking but don't push
                 updates to Baselinker/Amazon.

    Returns:
        Summary dict with counts: checked, updated, bl_updated,
        amz_confirmed, still_pending, errors.
    """
    summary: dict = {
        "checked": 0,
        "updated": 0,
        "bl_updated": 0,
        "amz_confirmed": 0,
        "still_pending": 0,
        "errors": [],
    }

    # Step 1: Get pending orders
    print("\n[1/3] Fetching pending Printful orders from DB...")
    pending = get_pending_printful_orders()

    if not pending:
        print("  No pending orders found.")
        return summary

    summary["checked"] = len(pending)

    # Step 2: Separate into two groups
    need_printful_check: list[dict] = []  # no tracking yet
    need_bl_sync: list[dict] = []         # has tracking, needs BL/AMZ sync

    for row in pending:
        if row.get("tracking_number"):
            need_bl_sync.append(row)
        else:
            need_printful_check.append(row)

    print(f"  {len(need_printful_check)} need Printful tracking check")
    print(f"  {len(need_bl_sync)} have tracking, need BL/Amazon sync")

    # Check Printful API for orders without tracking
    if need_printful_check:
        print(f"\n[2/3] Checking Printful API for {len(need_printful_check)} orders...")
        pf_ids = [
            row["printful_order_id"]
            for row in need_printful_check
            if row.get("printful_order_id")
        ]
        tracking_updates = check_tracking_updates(pf_ids)

        tracking_by_pf_id = {t["printful_order_id"]: t for t in tracking_updates}

        for row in need_printful_check:
            pf_id = row.get("printful_order_id")
            update = tracking_by_pf_id.get(pf_id, {})

            if update.get("tracking_number"):
                # Got tracking from Printful -- enrich row and queue for BL sync
                row["tracking_number"] = update["tracking_number"]
                row["tracking_url"] = update.get("tracking_url")
                row["carrier"] = update.get("carrier")
                row["_printful_status"] = update.get("printful_status")
                need_bl_sync.append(row)
            else:
                # Still no tracking -- update Printful status in DB
                summary["still_pending"] += 1
                if not dry_run:
                    try:
                        _update_mapping_record(
                            record_id=row["id"],
                            printful_status=update.get("printful_status", row.get("status", "pending")),
                        )
                    except Exception as e:
                        summary["errors"].append(
                            f"DB update error for mapping {row['id']}: {e}"
                        )
    else:
        print("\n[2/3] No Printful API checks needed (all already have tracking).")

    # Step 3: Sync tracking to Baselinker and Amazon
    if need_bl_sync:
        print(f"\n[3/3] Syncing tracking to Baselinker/Amazon for {len(need_bl_sync)} orders...")

        for row in need_bl_sync:
            tracking = row["tracking_number"]
            carrier = row.get("carrier") or ""
            tracking_url = row.get("tracking_url")
            bl_order_id = row.get("baselinker_order_id")
            amz_order_id = row.get("amazon_order_id")
            record_id = row["id"]

            if dry_run:
                print(
                    f"  [DRY RUN] Would sync: BL#{bl_order_id} / "
                    f"AMZ#{amz_order_id} <- {tracking} ({carrier})"
                )
                summary["updated"] += 1
                continue

            bl_ok = False
            amz_ok = False
            error_msg = None

            # Update Baselinker
            if bl_order_id:
                try:
                    bl_ok = update_baselinker_tracking(
                        order_id=int(bl_order_id),
                        tracking_number=tracking,
                        carrier=carrier or "Other",
                        tracking_url=tracking_url,
                    )
                    if bl_ok:
                        summary["bl_updated"] += 1
                except Exception as e:
                    error_msg = f"BL error: {e}"
                    summary["errors"].append(f"BL order {bl_order_id}: {e}")

            # Confirm Amazon shipment
            if amz_order_id:
                try:
                    amz_ok = confirm_amazon_shipment(
                        amazon_order_id=str(amz_order_id),
                        tracking_number=tracking,
                        carrier=carrier or "Other",
                    )
                    if amz_ok:
                        summary["amz_confirmed"] += 1
                except Exception as e:
                    amz_error = f"AMZ error: {e}"
                    error_msg = f"{error_msg}; {amz_error}" if error_msg else amz_error
                    summary["errors"].append(f"AMZ order {amz_order_id}: {e}")

            # Update DB record
            try:
                _update_mapping_record(
                    record_id=record_id,
                    tracking_number=tracking,
                    tracking_url=tracking_url,
                    carrier=carrier or None,
                    printful_status=row.get("_printful_status", "shipped"),
                    bl_synced=bl_ok,
                    amz_synced=amz_ok,
                    error_msg=error_msg,
                )
            except Exception as e:
                summary["errors"].append(f"DB update error for mapping {record_id}: {e}")

            if bl_ok or amz_ok:
                summary["updated"] += 1

            time.sleep(0.5)
    else:
        print("\n[3/3] No orders ready for BL/Amazon sync.")

    return summary


def register_printful_order(
    printful_order_id: int,
    baselinker_order_id: int | None = None,
    amazon_order_id: str | None = None,
    sku: str | None = None,
    status: str = "created",
) -> dict:
    """Register a new Printful order for tracking sync.

    Inserts into printful_order_mappings. If printful_order_id already exists,
    upserts (merges). The sync_all_tracking() function will pick it up.

    Args:
        printful_order_id: Printful order ID.
        baselinker_order_id: Corresponding Baselinker order ID.
        amazon_order_id: Corresponding Amazon order ID (if applicable).
        sku: Primary SKU of the order.
        status: Initial status (default: 'created').

    Returns:
        The created/updated record from Supabase.
    """
    record: dict = {
        "printful_order_id": printful_order_id,
        "status": status,
        "bl_tracking_synced": False,
        "amz_tracking_synced": False,
    }
    if baselinker_order_id is not None:
        record["baselinker_order_id"] = baselinker_order_id
    if amazon_order_id is not None:
        record["amazon_order_id"] = amazon_order_id
    if sku is not None:
        record["items"] = json.dumps([{"sku": sku}])

    result = supa_post(TABLE, [record], on_conflict="printful_order_id")
    print(f"  Registered Printful order {printful_order_id} for tracking sync")
    return result[0] if result else record


# ============================================================
# CLI entry point
# ============================================================


def main():
    parser = argparse.ArgumentParser(
        description="Sync Printful tracking numbers to Baselinker/Amazon"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Check Printful for tracking but don't push updates",
    )
    parser.add_argument(
        "--register", type=int, metavar="PF_ORDER_ID",
        help="Register a Printful order for tracking sync",
    )
    parser.add_argument("--bl-order", type=int, help="Baselinker order ID (with --register)")
    parser.add_argument("--amz-order", type=str, help="Amazon order ID (with --register)")
    parser.add_argument("--sku", type=str, help="SKU (with --register)")
    args = parser.parse_args()

    print(f"{'='*60}")
    print(f"Printful Tracking Sync — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    if args.register:
        register_printful_order(
            printful_order_id=args.register,
            baselinker_order_id=args.bl_order,
            amazon_order_id=args.amz_order,
            sku=args.sku,
        )
        return

    start = time.time()
    summary = sync_all_tracking(dry_run=args.dry_run)
    elapsed = time.time() - start

    print(f"\n{'='*60}")
    print("Summary:")
    print(f"  Checked:        {summary['checked']}")
    print(f"  Updated:        {summary['updated']}")
    print(f"  BL updated:     {summary['bl_updated']}")
    print(f"  AMZ confirmed:  {summary['amz_confirmed']}")
    print(f"  Still pending:  {summary['still_pending']}")
    if summary["errors"]:
        print(f"  Errors ({len(summary['errors'])}):")
        for err in summary["errors"][:10]:
            print(f"    - {err}")
    print(f"  Time:           {elapsed:.1f}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
