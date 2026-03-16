#!/usr/bin/env python3
"""POD Auto-fulfillment Daemon — Amazon orders → Printful → Tracking sync.

Flow:
  1. Poll Baselinker for new Amazon orders containing PFT-* SKUs
  2. Auto-create + confirm Printful order (confirmed = sent to production immediately)
  3. Poll Printful orders for tracking numbers
  4. Register tracking in Baselinker via createPackageManual
     (Baselinker automatically pushes tracking to Amazon within seconds)
  5. Alert Telegram on errors and successful syncs

State file: data/pod_fulfillment_state.json
Log file:   data/pod_fulfillment.log

Usage:
  python scripts/pod_fulfillment.py             # run one cycle and exit
  python scripts/pod_fulfillment.py --daemon    # poll every 30 min
  python scripts/pod_fulfillment.py --status    # show current state summary
  python scripts/pod_fulfillment.py --retry     # retry all errored orders
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import requests as _requests

from etl import config
from etl.baselinker import bl_api
from etl.printful_orders import (
    PrintfulAPIError,
    PrintfulConflictError,
    PrintfulValidationError,
    _build_headers,
    _raise_for_status,
    _request,
    confirm_printful_order,
    get_printful_order_status,
    parse_pft_sku,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
STATE_FILE = ROOT / "data" / "pod_fulfillment_state.json"
LOG_FILE = ROOT / "data" / "pod_fulfillment.log"
SKU_MAP_FILE = ROOT / "data" / "pod_sku_map.json"

POLL_INTERVAL_MINUTES = 30
INITIAL_LOOKBACK_DAYS = 7

# Printful carrier name → Baselinker courier_code mapping
# Baselinker accepts lowercase courier codes; unknown carriers use "other"
CARRIER_TO_BL_CODE: dict[str, str] = {
    "DHL": "dhl",
    "DPD": "dpd",
    "UPS": "ups",
    "GLS": "gls",
    "HERMES": "hermes",
    "FEDEX": "fedex",
    "TNT": "tnt",
    "INPOST": "inpost",
    "POCZTA POLSKA": "poczta_polska",
    "USPS": "usps",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pod_fulfillment")


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------
def _load_telegram_creds() -> tuple[str, str]:
    """Load Telegram bot token and chat ID from ~/.keys/telegram.env."""
    _tg_path = Path.home() / ".keys" / "telegram.env"
    token = ""
    chat_id = ""
    if _tg_path.exists():
        for line in _tg_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() == "TELEGRAM_BOT_TOKEN":
                    token = v.strip()
                elif k.strip() == "OWNER_CHAT_ID":
                    chat_id = v.strip()
    return token, chat_id


_TG_TOKEN, _TG_CHAT_ID = _load_telegram_creds()


def _telegram_send(text: str) -> None:
    """Send Telegram message. Fails silently."""
    if not _TG_TOKEN or not _TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage",
            json={"chat_id": _TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as exc:
        logger.warning("Telegram send failed: %s", exc)


def _alert_error(subject: str, detail: str = "") -> None:
    logger.error("ALERT: %s — %s", subject, detail)
    msg = f"🚨 <b>POD Fulfillment ERROR</b>\n{subject}"
    if detail:
        msg += f"\n\n<code>{detail[:600]}</code>"
    _telegram_send(msg)


def _alert_info(text: str) -> None:
    logger.info("ALERT: %s", text)
    _telegram_send(f"✅ <b>POD Fulfillment</b>\n{text}")


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------
def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            logger.warning("State file corrupt, starting fresh")
    return {
        "last_polled_timestamp": None,
        "orders": {},  # bl_order_id → OrderState
    }


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


# ---------------------------------------------------------------------------
# SKU mapping
# ---------------------------------------------------------------------------
def _load_sku_map() -> dict[str, int]:
    """Load SKU → Printful sync_variant_id mapping from file.

    The mapping file is built by scripts/pod_sku_mapping.py.
    Maps Amazon SKU (PFT-*) → sync_variant_id (store-specific, has design attached).
    """
    if SKU_MAP_FILE.exists():
        try:
            data = json.loads(SKU_MAP_FILE.read_text())
            if data:
                return data
        except Exception:
            pass
    # If no map file, warn — orders may fail without it
    logger.warning(
        "SKU map file not found at %s. "
        "Run 'python scripts/pod_sku_mapping.py' to build it. "
        "Orders may fail without sync_variant_id mapping.",
        SKU_MAP_FILE,
    )
    return {}


def _create_printful_order_with_sync_variants(
    token: str,
    store_id: int | str,
    bl_order: dict,
    sku_to_sync_variant: dict[str, int],
) -> dict:
    """Create a Printful order using sync_variant_id (includes saved design files).

    sync_variant_id is the store-specific variant ID that has the embroidery/print
    design already attached. This is required for orders without inline print files.

    For SKUs not found in sku_to_sync_variant, falls back to catalog variant_id
    extracted from PFT-{template}-{catalog_variant_id} SKU format.
    """
    recipient = {
        "name": bl_order.get("delivery_fullname", ""),
        "address1": bl_order.get("delivery_address", ""),
        "city": bl_order.get("delivery_city", ""),
        "country_code": bl_order.get("delivery_country_code", ""),
        "zip": bl_order.get("delivery_postcode", ""),
    }
    for opt in ("delivery_company", "phone", "email"):
        field = opt.replace("delivery_", "") if opt.startswith("delivery_") else opt
        val = bl_order.get(opt) or bl_order.get(field)
        if val:
            key = "company" if "company" in opt else opt
            recipient[key] = val

    items = []
    for product in bl_order.get("products", []):
        sku = product.get("sku", "")
        quantity = int(product.get("quantity", 1))

        sync_vid = sku_to_sync_variant.get(sku)
        if sync_vid:
            items.append({"sync_variant_id": sync_vid, "quantity": quantity})
        else:
            # Fallback: parse catalog variant_id from PFT-* SKU
            _, cat_vid = parse_pft_sku(sku)
            if cat_vid:
                logger.warning(
                    "SKU %s not in sync_variant map — using catalog variant_id=%d (may fail)",
                    sku, cat_vid,
                )
                items.append({"variant_id": cat_vid, "quantity": quantity})
            else:
                raise PrintfulValidationError(
                    f"Cannot resolve Printful variant for SKU '{sku}'. "
                    "Run 'python scripts/pod_sku_mapping.py' to rebuild the mapping.",
                    status_code=None,
                )

    payload = {
        "external_id": str(bl_order.get("order_id", "")),
        "recipient": recipient,
        "items": items,
    }

    headers = _build_headers(token, store_id)
    url = "https://api.printful.com/orders"
    resp = _requests.post(url, headers=headers, json=payload, timeout=30)
    _raise_for_status(resp)
    data = resp.json()
    return data.get("result", data)


# ---------------------------------------------------------------------------
# Baselinker helpers
# ---------------------------------------------------------------------------
def _is_pod_order(order: dict) -> bool:
    """Return True if any product in the order has a PFT-* SKU."""
    return any(
        p.get("sku", "").startswith("PFT-")
        for p in order.get("products", [])
    )


def _is_amazon_order(order: dict) -> bool:
    """Return True if the order came from Amazon."""
    return "amazon" in (order.get("order_source") or "").lower()


def _get_amazon_pod_orders(since_timestamp: int) -> list[dict]:
    """Fetch Amazon POD orders from Baselinker since a given Unix timestamp."""
    all_orders: list[dict] = []
    cursor = since_timestamp

    while True:
        data = bl_api("getOrders", {
            "date_confirmed_from": cursor,
            "get_unconfirmed_orders": False,
            "include_custom_extra_fields": False,
        })
        orders = data.get("orders", [])
        if not orders:
            break

        for o in orders:
            if _is_amazon_order(o) and _is_pod_order(o):
                all_orders.append(o)

        # Advance cursor past the latest order in this batch
        last_date = max(o.get("date_confirmed", 0) for o in orders)
        if last_date <= cursor:
            break
        cursor = last_date + 1
        time.sleep(0.4)

    return all_orders


def _carrier_to_bl_code(carrier: str | None) -> str:
    """Map Printful carrier name to Baselinker courier code."""
    if not carrier:
        return "other"
    upper = carrier.upper()
    for key, code in CARRIER_TO_BL_CODE.items():
        if key in upper:
            return code
    # Use lowercased carrier name as fallback — Baselinker accepts unknown codes
    return carrier.lower().replace(" ", "_")[:20]


def _register_tracking_in_baselinker(
    bl_order_id: str,
    tracking_number: str,
    carrier: str | None,
) -> bool:
    """Register external tracking number in Baselinker.

    Uses createPackageManual — Baselinker automatically syncs tracking
    to Amazon/marketplaces within seconds.
    """
    courier_code = _carrier_to_bl_code(carrier)
    try:
        bl_api("createPackageManual", {
            "order_id": int(bl_order_id),
            "courier_code": courier_code,
            "package_number": tracking_number,
            "pickup_date": int(datetime.now().timestamp()),
            "return_shipment": False,
        })
        logger.info(
            "BL order %s: tracking registered — %s (%s → code=%s)",
            bl_order_id, tracking_number, carrier, courier_code,
        )
        return True
    except Exception as exc:
        logger.error(
            "BL order %s: failed to register tracking %s: %s",
            bl_order_id, tracking_number, exc,
        )
        return False


# ---------------------------------------------------------------------------
# Core fulfillment logic
# ---------------------------------------------------------------------------
def process_new_orders(state: dict) -> int:
    """Poll Baselinker and create confirmed Printful orders for new POD orders.

    Returns count of orders newly submitted to Printful.
    """
    since = state.get("last_polled_timestamp")
    if since is None:
        since = int((datetime.now() - timedelta(days=INITIAL_LOOKBACK_DAYS)).timestamp())

    logger.info(
        "Polling Baselinker for Amazon POD orders since %s",
        datetime.fromtimestamp(since).isoformat(),
    )

    orders = _get_amazon_pod_orders(since)
    logger.info("Found %d Amazon POD order(s) in this window", len(orders))

    sku_map = _load_sku_map()
    new_count = 0
    now_ts = int(datetime.now().timestamp())

    for bl_order in orders:
        bl_order_id = str(bl_order["order_id"])

        # Skip already processed (unless previously errored — those can retry)
        existing = state["orders"].get(bl_order_id)
        if existing and existing.get("status") not in ("error",):
            logger.debug("BL order %s already processed (status=%s), skipping",
                         bl_order_id, existing.get("status"))
            continue

        amz_id = bl_order.get("order_source_external_id", "?")
        logger.info("Processing BL order %s (Amazon: %s)", bl_order_id, amz_id)

        # Create + confirm Printful order using sync_variant_id (includes saved design)
        try:
            pf_order = _create_printful_order_with_sync_variants(
                config.PRINTFUL_V1_TOKEN,
                int(config.PRINTFUL_STORE_ID),
                bl_order,
                sku_map,
            )
            pf_order_id = pf_order["id"]
            ship_country = bl_order.get("delivery_country_code", "?")
            num_items = len([p for p in bl_order.get("products", []) if p.get("sku", "").startswith("PFT-")])

            confirm_printful_order(
                config.PRINTFUL_V1_TOKEN,
                int(config.PRINTFUL_STORE_ID),
                pf_order_id,
            )

            state["orders"][bl_order_id] = {
                "printful_order_id": pf_order_id,
                "amazon_order_id": amz_id,
                "status": "confirmed",
                "tracking_number": None,
                "carrier": None,
                "error": None,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }
            new_count += 1

            logger.info(
                "Printful order #%d created+confirmed for BL #%s (Amazon: %s) — %d item(s)",
                pf_order_id, bl_order_id, amz_id, num_items,
            )
            _alert_info(
                f"New POD order → Printful\n"
                f"BL #{bl_order_id} | Amazon #{amz_id} | Printful #{pf_order_id}\n"
                f"Items: {num_items} | Ship to: {ship_country}"
            )

        except PrintfulValidationError as exc:
            logger.warning("BL order %s: validation error — %s", bl_order_id, exc)
            state["orders"][bl_order_id] = _error_record(str(exc))
            _alert_error(
                f"BL #{bl_order_id} (Amazon {amz_id}): Printful validation error",
                str(exc),
            )

        except PrintfulConflictError as exc:
            # 409 = order already exists in Printful (duplicate external_id)
            logger.warning("BL order %s: Printful conflict (already exists?) — %s", bl_order_id, exc)
            state["orders"][bl_order_id] = _error_record(f"conflict: {exc}")

        except PrintfulAPIError as exc:
            logger.error("BL order %s: Printful API error — %s", bl_order_id, exc)
            state["orders"][bl_order_id] = _error_record(str(exc))
            _alert_error(
                f"BL #{bl_order_id} (Amazon {amz_id}): Printful API error",
                str(exc),
            )

    state["last_polled_timestamp"] = now_ts
    return new_count


def sync_tracking(state: dict) -> int:
    """Check Printful for tracking numbers and sync back to Baselinker/Amazon.

    Returns count of orders where tracking was synced.
    """
    pending = {
        bl_id: info
        for bl_id, info in state["orders"].items()
        if info.get("status") == "confirmed" and info.get("printful_order_id")
    }

    if not pending:
        return 0

    logger.info("Checking tracking for %d confirmed Printful order(s)", len(pending))
    synced = 0

    for bl_order_id, info in pending.items():
        pf_order_id = info["printful_order_id"]

        try:
            result = get_printful_order_status(
                config.PRINTFUL_V1_TOKEN,
                int(config.PRINTFUL_STORE_ID),
                pf_order_id,
            )
        except PrintfulAPIError as exc:
            logger.error(
                "Cannot fetch status for Printful order #%d (BL #%s): %s",
                pf_order_id, bl_order_id, exc,
            )
            time.sleep(0.5)
            continue

        pf_status = result["status"]
        tracking = result.get("tracking_number")
        carrier = result.get("carrier") or result.get("tracking_url", "")
        info["updated_at"] = datetime.now().isoformat()

        if pf_status == "fulfilled" and tracking:
            ok = _register_tracking_in_baselinker(bl_order_id, tracking, carrier)
            if ok:
                info["status"] = "tracking_synced"
                info["tracking_number"] = tracking
                info["carrier"] = carrier
                synced += 1
                _alert_info(
                    f"Tracking synced to Amazon!\n"
                    f"BL #{bl_order_id} | Printful #{pf_order_id}\n"
                    f"Tracking: {tracking} ({carrier or 'unknown carrier'})"
                )
            else:
                # Failed to register — will retry next cycle
                _alert_error(
                    f"BL #{bl_order_id}: tracking {tracking} obtained but failed to register in Baselinker",
                    "Will retry next cycle.",
                )

        elif pf_status in ("canceled", "failed"):
            info["status"] = f"printful_{pf_status}"
            _alert_error(
                f"Printful order #{pf_order_id} (BL #{bl_order_id}) is {pf_status}",
                "Manual intervention required.",
            )

        else:
            # Still in progress: pending, inprocess, partial
            info["printful_status"] = pf_status
            logger.debug(
                "Printful order #%d (BL #%s): status=%s, no tracking yet",
                pf_order_id, bl_order_id, pf_status,
            )

        time.sleep(0.5)

    return synced


def retry_errors(state: dict) -> int:
    """Reset error status on failed orders so they get reprocessed next cycle."""
    errors = [
        bl_id for bl_id, info in state["orders"].items()
        if info.get("status") == "error"
    ]
    for bl_id in errors:
        del state["orders"][bl_id]
    logger.info("Cleared %d errored order(s) for retry", len(errors))
    return len(errors)


def _error_record(error: str) -> dict:
    now = datetime.now().isoformat()
    return {
        "status": "error",
        "error": error,
        "created_at": now,
        "updated_at": now,
    }


# ---------------------------------------------------------------------------
# High-level cycle
# ---------------------------------------------------------------------------
def run_once(retry: bool = False) -> dict[str, int]:
    """Execute one complete fulfillment cycle.

    Returns dict with counts: new_orders, tracking_synced, retried.
    """
    state = _load_state()
    retried = 0

    if retry:
        retried = retry_errors(state)
        _save_state(state)

    try:
        new_orders = process_new_orders(state)
    except Exception as exc:
        logger.exception("Unhandled error in process_new_orders")
        _alert_error("process_new_orders crashed", str(exc))
        new_orders = 0
    finally:
        _save_state(state)

    try:
        synced = sync_tracking(state)
    except Exception as exc:
        logger.exception("Unhandled error in sync_tracking")
        _alert_error("sync_tracking crashed", str(exc))
        synced = 0
    finally:
        _save_state(state)

    return {"new_orders": new_orders, "tracking_synced": synced, "retried": retried}


def run_daemon() -> None:
    """Run as a polling daemon with POLL_INTERVAL_MINUTES interval."""
    logger.info("Starting POD fulfillment daemon (poll every %d min)", POLL_INTERVAL_MINUTES)
    _alert_info(f"POD fulfillment daemon started. Poll interval: {POLL_INTERVAL_MINUTES} min.")

    while True:
        try:
            result = run_once()
            logger.info(
                "Cycle complete — new: %d, tracking synced: %d",
                result["new_orders"], result["tracking_synced"],
            )
        except KeyboardInterrupt:
            logger.info("Daemon stopped by user")
            _alert_info("POD fulfillment daemon stopped.")
            break
        except Exception as exc:
            logger.exception("Unexpected error in daemon cycle")
            _alert_error("Daemon cycle unhandled error", str(exc))

        logger.info("Sleeping %d minutes...", POLL_INTERVAL_MINUTES)
        time.sleep(POLL_INTERVAL_MINUTES * 60)


def show_status() -> None:
    """Print current state summary to stdout."""
    state = _load_state()
    orders = state.get("orders", {})
    last_ts = state.get("last_polled_timestamp")

    print("=" * 60)
    print("POD Fulfillment State")
    print("=" * 60)
    print(f"Last polled: {datetime.fromtimestamp(last_ts).isoformat() if last_ts else 'never'}")
    print(f"Total tracked orders: {len(orders)}")

    if not orders:
        print("\nNo orders tracked yet.")
        return

    counts = Counter(v.get("status") for v in orders.values())
    print("\nBy status:")
    for status, count in sorted(counts.items()):
        print(f"  {status:25s} {count}")

    # Recent tracking synced
    synced = [
        (bl_id, info) for bl_id, info in orders.items()
        if info.get("status") == "tracking_synced"
    ]
    if synced:
        print(f"\nRecently synced ({len(synced)} total, showing last 5):")
        for bl_id, info in list(synced)[-5:]:
            print(f"  BL #{bl_id}: {info.get('tracking_number')} ({info.get('carrier', '?')})")

    # Errors
    errors = [
        (bl_id, info) for bl_id, info in orders.items()
        if info.get("status") == "error"
    ]
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for bl_id, info in errors[-10:]:
            print(f"  BL #{bl_id}: {str(info.get('error', ''))[:80]}")

    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="POD Auto-fulfillment: Amazon → Printful → Tracking sync"
    )
    parser.add_argument("--daemon", action="store_true",
                        help=f"Run as polling daemon (every {POLL_INTERVAL_MINUTES} min)")
    parser.add_argument("--status", action="store_true",
                        help="Show current state summary")
    parser.add_argument("--retry", action="store_true",
                        help="Retry all errored orders before running cycle")
    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.daemon:
        run_daemon()
    else:
        result = run_once(retry=args.retry)
        print(
            f"Done — new orders: {result['new_orders']}, "
            f"tracking synced: {result['tracking_synced']}, "
            f"retried: {result['retried']}"
        )
