"""Fetch real Allegro fees from Billing API and update orders."""
import requests
import time
import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from . import config, db


# ── Token management ────────────────────────────────────────────────

_ENV_PATH = Path.home() / ".keys" / "allegro.env"


def _load_allegro_token():
    """Load Allegro access token, refresh if expired."""
    token = None
    expires_at = 0

    if _ENV_PATH.exists():
        for line in _ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line.startswith("ALLEGRO_ACCESS_TOKEN="):
                token = line.split("=", 1)[1].strip().strip('"')
            elif line.startswith("ALLEGRO_TOKEN_EXPIRES_AT="):
                try:
                    expires_at = int(line.split("=", 1)[1].strip().strip('"'))
                except ValueError:
                    expires_at = 0

    if not token:
        raise RuntimeError("No Allegro token found in ~/.keys/allegro.env")

    # Check if token is expired (with 60s buffer)
    if time.time() >= expires_at - 60:
        print("  [allegro] Token expired, refreshing...")
        try:
            subprocess.run(
                ["python3.11", str(Path.home() / "allegro-mcp" / "auth.py"), "refresh"],
                check=True,
                capture_output=True,
                text=True,
            )
            # Re-read token after refresh
            return _load_allegro_token.__wrapped__()
        except subprocess.CalledProcessError as e:
            print(f"  [allegro] Token refresh failed: {e.stderr}")
            raise RuntimeError(f"Allegro token refresh failed: {e.stderr}")

    return token


# Prevent infinite recursion on refresh
_load_allegro_token.__wrapped__ = lambda: _load_allegro_token_no_refresh()


def _load_allegro_token_no_refresh():
    """Load token without attempting refresh (used after refresh)."""
    token = None
    if _ENV_PATH.exists():
        for line in _ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line.startswith("ALLEGRO_ACCESS_TOKEN="):
                token = line.split("=", 1)[1].strip().strip('"')
    if not token:
        raise RuntimeError("No Allegro token after refresh")
    return token


# ── API helpers ─────────────────────────────────────────────────────

BASE = "https://api.allegro.pl"


def _headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.allegro.public.v1+json",
    }


def _api_get(token, path, params=None):
    """GET with retry for rate limits."""
    url = f"{BASE}{path}"
    for attempt in range(5):
        try:
            resp = requests.get(url, headers=_headers(token), params=params, timeout=30)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"    [ConnectionError] retry in {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [Rate limit] retry in {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 401:
            raise RuntimeError(f"Allegro API 401 Unauthorized: token may be invalid")
        if resp.status_code >= 500:
            time.sleep(5 * (attempt + 1))
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError(f"Allegro API {path}: failed after 5 retries")


# ── Data fetching ───────────────────────────────────────────────────

def _get_billing_entries(token, since_date, limit=100):
    """Fetch all billing entries since a date using offset pagination."""
    entries = []
    offset = 0
    while True:
        data = _api_get(token, "/billing/billing-entries", params={
            "occurredAt.gte": since_date.strftime("%Y-%m-%dT00:00:00.000Z"),
            "limit": limit,
            "offset": offset,
        })
        batch = data.get("billingEntries", [])
        entries.extend(batch)

        if len(batch) < limit:
            break
        offset += limit

        if offset % 500 == 0:
            print(f"    Billing entries page {offset // limit}: {len(entries)} total")

    return entries


def _get_payment_operations(token, since_date, limit=100):
    """Fetch payment operations (multi-currency: PLN, CZK, HUF, EUR).

    Uses cursor-based pagination via 'group' and 'occurredAt' filters.
    """
    operations = []
    offset = 0
    while True:
        data = _api_get(token, "/payments/payment-operations", params={
            "occurredAt.gte": since_date.strftime("%Y-%m-%dT00:00:00.000Z"),
            "group": "OUTCOME",
            "limit": limit,
            "offset": offset,
        })
        batch = data.get("paymentOperations", [])
        operations.extend(batch)

        if len(batch) < limit:
            break
        offset += limit

        if offset % 500 == 0:
            print(f"    Payment operations page {offset // limit}: {len(operations)} total")

    return operations


# ── Fee type classification ─────────────────────────────────────────

# Billing entry type IDs (from GET /billing/billing-types):
# SUC = Prowizja od sprzedazy (sales commission)
# FSF = Prowizja od sprzedazy oferty wyrooznionej (featured listing commission)
# BC1-BC5 = Prowizja od sprzedazy w kampanii (campaign commission)
# HB4 = Oplata za dostawe InPost
# DPB = Oplata za dostawe DPD Allegro Delivery
# HLB = Oplata za dostawe DHL Allegro Delivery
# ORB = Oplata za dostawe ORLEN Paczka Allegro Delivery
# DHR = Oplata dodatkowa za dostawe DHL
# REF = Zwrot kosztow (refund)
# ADS, PRO = promoted offers / ads
# PAD = Pobranie oplat z wplywow (fee collection from proceeds - accounting entry, skip)
# PS1 = Wyrownanie w programie Allegro Ceny (price match adjustment)
# SUM = Podsumowanie miesiaca (monthly summary, skip)

COMMISSION_TYPES = {"SUC", "FSF", "BC1", "BC2", "BC3", "BC4", "BC5", "ASC"}
SHIPPING_TYPES = {"HB4", "DPB", "HLB", "ORB", "DHR"}
REFUND_TYPES = {"REF"}
AD_TYPES = {"ADS", "PRO"}
PRICE_ADJUSTMENT_TYPES = {"PS1"}
# These are accounting/summary entries, not per-order fees - skip them
SKIP_TYPES = {"PAD", "SUM"}


def _classify_entry(entry):
    """Classify a billing entry by its type ID.
    Returns None for entries that should be skipped (accounting/summary entries)."""
    type_id = entry.get("type", {}).get("id", "")
    if type_id in SKIP_TYPES:
        return None  # skip accounting entries
    if type_id in COMMISSION_TYPES:
        return "commission"
    elif type_id in SHIPPING_TYPES:
        return "shipping"
    elif type_id in REFUND_TYPES:
        return "refund"
    elif type_id in AD_TYPES:
        return "ads"
    elif type_id in PRICE_ADJUSTMENT_TYPES:
        return "price_adjustment"
    else:
        return "other"


# ── Core logic ──────────────────────────────────────────────────────

def _aggregate_fees_by_order(entries):
    """Group billing entries by order ID and sum fees.

    Returns dict: {allegro_order_id: {commission, shipping, refund, ads, price_adjustment, other, total, currency}}
    Skips accounting/summary entries (PAD, SUM) that have no order association or are not real fees.
    """
    order_fees = {}
    skipped = 0

    for entry in entries:
        category = _classify_entry(entry)
        if category is None:
            skipped += 1
            continue

        order_info = entry.get("order", {})
        order_id = order_info.get("id") if order_info else None
        if not order_id:
            continue

        value = entry.get("value", {})
        amount = abs(float(value.get("amount", 0)))
        currency = value.get("currency", "PLN")

        if order_id not in order_fees:
            order_fees[order_id] = {
                "commission": 0.0,
                "shipping": 0.0,
                "refund": 0.0,
                "ads": 0.0,
                "price_adjustment": 0.0,
                "other": 0.0,
                "total": 0.0,
                "currency": currency,
            }

        order_fees[order_id][category] += amount
        # Total = commission + shipping + other (the actual fees the seller pays)
        if category in ("commission", "shipping", "other"):
            order_fees[order_id]["total"] += amount

    if skipped:
        print(f"    Skipped {skipped} accounting/summary entries (PAD, SUM)")

    return order_fees


def _aggregate_payment_ops_by_order(operations):
    """Group payment operations by order ID for multi-currency support.

    Returns dict: {allegro_order_id: {amount, currency}}
    """
    order_ops = {}

    for op in operations:
        order_info = op.get("order", {})
        order_id = order_info.get("id") if order_info else None
        if not order_id:
            continue

        value = op.get("value", {})
        amount = abs(float(value.get("amount", 0)))
        currency = value.get("currency", "PLN")

        if order_id not in order_ops:
            order_ops[order_id] = {"amount": 0.0, "currency": currency}

        order_ops[order_id]["amount"] += amount

    return order_ops


def _build_uuid_to_email_map(token, order_uuids, since_date):
    """Build mapping from Allegro checkout-form UUID to buyer email.

    Fetches checkout-forms from Allegro API and matches by UUID.
    Baselinker uses anonymized @allegromail.pl emails which are the same
    as Allegro's buyer.email field -- this is our matching key.
    """
    uuid_to_email = {}

    # Fetch recent checkout-forms in bulk (paginated)
    offset = 0
    limit = 100
    while True:
        try:
            data = _api_get(token, "/order/checkout-forms", params={
                "updatedAt.gte": since_date.strftime("%Y-%m-%dT00:00:00.000Z"),
                "limit": limit,
                "offset": offset,
                "sort": "-updatedAt",
            })
        except Exception as e:
            print(f"    [WARN] checkout-forms fetch failed at offset {offset}: {e}")
            break

        forms = data.get("checkoutForms", [])
        for cf in forms:
            cf_id = cf.get("id")
            buyer_email = cf.get("buyer", {}).get("email", "")
            if cf_id and buyer_email:
                uuid_to_email[cf_id] = buyer_email

        if len(forms) < limit:
            break
        offset += limit
        time.sleep(0.5)  # be nice to API

    print(f"    Fetched {len(uuid_to_email)} checkout-forms for UUID->email mapping")

    # For any UUIDs still missing, try individual lookup
    missing = [u for u in order_uuids if u not in uuid_to_email]
    if missing:
        print(f"    Looking up {len(missing)} individual checkout-forms...")
        for uuid in missing:
            try:
                data = _api_get(token, f"/order/checkout-forms/{uuid}")
                buyer_email = data.get("buyer", {}).get("email", "")
                if buyer_email:
                    uuid_to_email[uuid] = buyer_email
            except Exception:
                pass
            time.sleep(0.3)

    return uuid_to_email


def _update_orders_with_fees(token, order_fees, since_date):
    """Match Allegro fees to orders in DB and update platform_fee.

    Strategy:
    1. Build UUID->email mapping via Allegro checkout-forms API
    2. Match buyer_email in our orders table (Baselinker stores same @allegromail.pl email)
    3. Also store the Allegro UUID in platform_order_id for future direct matching
    """
    _H = {
        "apikey": config.SUPABASE_KEY,
        "Authorization": f"Bearer {config.SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    # Get allegro platform ID
    platform_map = db.get_platform_map(None)
    allegro_platform_id = platform_map.get("allegro")
    if not allegro_platform_id:
        print("  [WARN] No 'allegro' platform found in DB")
        return 0, 0

    # Step 1: Build UUID->email mapping
    print("    Building Allegro UUID -> buyer email mapping...")
    uuid_to_email = _build_uuid_to_email_map(token, list(order_fees.keys()), since_date)

    # Step 2: Load all Allegro orders from DB for matching
    print("    Loading Allegro orders from DB for matching...")
    db_orders = []
    offset = 0
    while True:
        resp = requests.get(
            f"{config.SUPABASE_URL}/rest/v1/orders",
            headers=_H,
            params={
                "select": "id,external_id,buyer_email,platform_order_id",
                "platform_id": f"eq.{allegro_platform_id}",
                "limit": "1000",
                "offset": str(offset),
            },
        )
        if resp.status_code != 200:
            break
        batch = resp.json()
        db_orders.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000

    # Build email->DB order lookup
    email_to_db = {}
    uuid_to_db = {}
    for row in db_orders:
        email = (row.get("buyer_email") or "").strip().lower()
        if email:
            email_to_db[email] = row
        # Also index by platform_order_id (for orders already matched in previous runs)
        po_id = (row.get("platform_order_id") or "").strip()
        if po_id:
            uuid_to_db[po_id] = row

    print(f"    DB has {len(db_orders)} Allegro orders, {len(email_to_db)} with email")

    # Step 3: Match and update
    updated = 0
    not_found = 0

    for allegro_uuid, fees in order_fees.items():
        db_row = None

        # Try direct UUID match first (from previous runs)
        if allegro_uuid in uuid_to_db:
            db_row = uuid_to_db[allegro_uuid]
        else:
            # Match via buyer email
            buyer_email = uuid_to_email.get(allegro_uuid, "").strip().lower()
            if buyer_email and buyer_email in email_to_db:
                db_row = email_to_db[buyer_email]

        if not db_row:
            not_found += 1
            continue

        # Build fee breakdown for notes
        notes_data = {
            "commission": round(fees["commission"], 2),
            "shipping_fee": round(fees["shipping"], 2),
            "refund": round(fees.get("refund", 0), 2),
            "ads": round(fees.get("ads", 0), 2),
            "price_adjustment": round(fees.get("price_adjustment", 0), 2),
            "other": round(fees.get("other", 0), 2),
            "source": "allegro_billing_api",
        }

        # PATCH: update platform_fee + store Allegro UUID in platform_order_id for future direct matching
        patch_data = {
            "platform_fee": round(fees["total"], 2),
            "platform_order_id": allegro_uuid,
            "notes": json.dumps(notes_data),
        }

        patch_resp = requests.patch(
            f"{config.SUPABASE_URL}/rest/v1/orders",
            headers=_H,
            params={"id": f"eq.{db_row['id']}"},
            json=patch_data,
        )
        if patch_resp.status_code in (200, 204):
            updated += 1

        if updated % 50 == 0 and updated > 0:
            print(f"    Updated {updated} orders...")

    return updated, not_found


# ── Public entry point ──────────────────────────────────────────────

def sync_allegro_fees(conn=None, days_back=90):
    """Main entry: fetch Allegro billing entries and update orders with real fees."""
    print("  Loading Allegro token...")
    token = _load_allegro_token()
    print("  Token loaded OK")

    since = datetime.utcnow() - timedelta(days=days_back)

    # Step 1: Billing entries (PLN, main fee source)
    print(f"  Fetching billing entries (since {since.strftime('%Y-%m-%d')})...")
    entries = _get_billing_entries(token, since)
    print(f"  Got {len(entries)} billing entries")

    # Step 2: Payment operations (multi-currency)
    print(f"  Fetching payment operations (since {since.strftime('%Y-%m-%d')})...")
    try:
        operations = _get_payment_operations(token, since)
        print(f"  Got {len(operations)} payment operations")
    except Exception as e:
        print(f"  [WARN] Payment operations fetch failed: {e}")
        print("  Continuing with billing entries only...")
        operations = []

    # Step 3: Aggregate fees per order
    order_fees = _aggregate_fees_by_order(entries)
    print(f"  Fees aggregated for {len(order_fees)} unique orders")

    # If payment ops returned multi-currency data, merge for orders not covered by billing
    if operations:
        payment_ops = _aggregate_payment_ops_by_order(operations)
        new_from_ops = 0
        for oid, op_data in payment_ops.items():
            if oid not in order_fees:
                order_fees[oid] = {
                    "commission": op_data["amount"],
                    "shipping": 0.0,
                    "refund": 0.0,
                    "ads": 0.0,
                    "other": 0.0,
                    "total": op_data["amount"],
                    "currency": op_data["currency"],
                }
                new_from_ops += 1
        if new_from_ops:
            print(f"  Added {new_from_ops} orders from payment operations (multi-currency)")

    # Stats
    total_commission = sum(f["commission"] for f in order_fees.values())
    total_shipping = sum(f["shipping"] for f in order_fees.values())
    total_refund = sum(f["refund"] for f in order_fees.values())
    total_ads = sum(f["ads"] for f in order_fees.values())
    total_price_adj = sum(f.get("price_adjustment", 0) for f in order_fees.values())
    total_other = sum(f["other"] for f in order_fees.values())
    total_all = sum(f["total"] for f in order_fees.values())

    print(f"\n  Fee breakdown (all orders, {days_back}d):")
    print(f"    Commission:     {total_commission:>10,.2f} PLN")
    print(f"    Shipping fees:  {total_shipping:>10,.2f} PLN")
    print(f"    Ads/promoted:   {total_ads:>10,.2f} PLN")
    print(f"    Price adj (PS1):{total_price_adj:>10,.2f} PLN")
    print(f"    Refunds:        {total_refund:>10,.2f} PLN")
    print(f"    Other:          {total_other:>10,.2f} PLN")
    print(f"    Total fees:     {total_all:>10,.2f} PLN")

    # Step 4: Update orders in DB
    print(f"\n  Updating orders in DB...")
    updated, not_found = _update_orders_with_fees(token, order_fees, since)
    print(f"  Updated: {updated} orders with real Allegro fees")
    if not_found:
        print(f"  Not found in DB: {not_found} orders (may be older or not synced)")

    return {
        "entries": len(entries),
        "operations": len(operations),
        "orders_with_fees": len(order_fees),
        "updated": updated,
        "not_found": not_found,
    }


if __name__ == "__main__":
    sync_allegro_fees(days_back=120)
