"""
Amazon-Printful auto-fulfillment orchestrator.

Ties together order detection, SKU mapping, Printful order creation,
and tracking sync into a single pipeline.

Usage:
    python3.11 -m etl.run --printful-orders   # process new orders
    python3.11 -m etl.run --tracking-sync      # sync tracking info
"""
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from etl import config, db
from etl.sku_mapping import is_printful_sku, parse_printful_sku, validate_variant_availability
from etl.order_detection import (
    get_new_amazon_orders,
    filter_printful_items,
    extract_shipping_address,
    mark_order_processing,
)
from etl.printful_orders import create_printful_order, confirm_printful_order, calculate_shipping
from etl.tracking_sync import sync_all_tracking

logger = logging.getLogger(__name__)

# ── Supabase REST helpers (reuse db module internals) ────────────────

def _get_pending_mappings() -> list[dict]:
    """Fetch all printful_order_mappings that need tracking updates."""
    return db._get("printful_order_mappings", {
        "select": "*",
        "status": "in.(created,in_production)",
        "order": "created_at.asc",
    })


def _get_mapping_by_bl_order(baselinker_order_id: int) -> dict | None:
    """Check if a mapping already exists for this Baselinker order."""
    rows = db._get("printful_order_mappings", {
        "select": "id,status",
        "baselinker_order_id": f"eq.{baselinker_order_id}",
        "limit": "1",
    })
    return rows[0] if rows else None


def _insert_mapping(mapping: dict) -> dict:
    """Insert a new order mapping row."""
    return db._post("printful_order_mappings", [mapping])[0]


def _update_mapping(mapping_id: int, data: dict) -> None:
    """Update an existing mapping row."""
    db._patch("printful_order_mappings", {"id": f"eq.{mapping_id}"}, data)


# ── Config loader ────────────────────────────────────────────────────

def load_config() -> dict[str, Any]:
    """Build config dict from ~/.keys/ credential files."""
    return {
        "baselinker_token": config.BASELINKER_TOKEN,
        "baselinker_url": config.BASELINKER_URL,
        "printful_token": config.PRINTFUL_V1_TOKEN,
        "printful_store_id": config.PRINTFUL_STORE_ID,
        "supabase_url": config.SUPABASE_URL,
        "supabase_key": config.SUPABASE_KEY,
    }


# ── Main pipeline: process new orders ────────────────────────────────

def process_new_orders(cfg: dict) -> dict[str, Any]:
    """
    Main fulfillment loop:
    1. Fetch new Amazon orders from Baselinker (PFT-* SKU only)
    2. For each order: validate variant availability, create Printful order, confirm
    3. Store order mapping in Supabase
    4. Mark Baselinker order as "processing"
    5. Return summary
    """
    summary: dict[str, Any] = {"processed": 0, "errors": [], "skipped": []}
    start_time = time.time()

    # Step 1: Get new Amazon orders that have Printful items
    logger.info("Fetching new Amazon orders from Baselinker...")
    try:
        raw_orders = get_new_amazon_orders(cfg)
    except Exception as e:
        logger.error("Failed to fetch orders from Baselinker: %s", e)
        summary["errors"].append({"step": "fetch_orders", "error": str(e)})
        return summary

    if not raw_orders:
        logger.info("No new orders to process.")
        return summary

    logger.info("Found %d new Amazon orders to check.", len(raw_orders))

    for order in raw_orders:
        bl_order_id: int = order["order_id"]
        amazon_order_id: str = order.get("extra_field_1", "") or order.get("shop_order_id", "")
        order_label = f"BL#{bl_order_id}"

        try:
            # Skip if already mapped
            existing = _get_mapping_by_bl_order(bl_order_id)
            if existing:
                logger.debug("%s already mapped (status=%s), skipping.", order_label, existing["status"])
                summary["skipped"].append({
                    "baselinker_order_id": bl_order_id,
                    "reason": f"already_mapped ({existing['status']})",
                })
                continue

            # Step 2a: Filter to PFT-* items only
            pf_items = filter_printful_items(order)
            if not pf_items:
                logger.debug("%s has no Printful items, skipping.", order_label)
                summary["skipped"].append({
                    "baselinker_order_id": bl_order_id,
                    "reason": "no_printful_items",
                })
                continue

            logger.info("%s — %d Printful item(s) found.", order_label, len(pf_items))

            # Step 2b: Parse SKUs and validate variant availability
            order_items_for_printful: list[dict] = []
            all_available = True

            for item in pf_items:
                sku = item["sku"]
                parsed = parse_printful_sku(sku)
                if not parsed:
                    logger.warning("%s — could not parse SKU: %s", order_label, sku)
                    all_available = False
                    break

                variant_id = parsed["variant_id"]
                quantity = item.get("quantity", 1)

                if not validate_variant_availability(cfg, variant_id):
                    logger.warning(
                        "%s — variant %s not available for SKU %s.",
                        order_label, variant_id, sku,
                    )
                    all_available = False
                    break

                order_items_for_printful.append({
                    "sku": sku,
                    "variant_id": variant_id,
                    "quantity": quantity,
                    "name": item.get("name", ""),
                    "retail_price": item.get("price_brutto", "0"),
                    "files": parsed.get("files", []),
                })

            if not all_available:
                summary["skipped"].append({
                    "baselinker_order_id": bl_order_id,
                    "reason": "variant_unavailable",
                })
                continue

            # Step 2c: Extract shipping address
            shipping = extract_shipping_address(order)

            # Step 2d: Create Printful order (draft first)
            external_id = f"BL-{bl_order_id}"
            logger.info("%s — creating Printful order (external_id=%s)...", order_label, external_id)

            pf_result = create_printful_order(
                cfg=cfg,
                external_id=external_id,
                items=order_items_for_printful,
                shipping_address=shipping,
            )

            if not pf_result or "id" not in pf_result:
                error_msg = pf_result.get("error", "Unknown error") if pf_result else "Empty response"
                logger.error("%s — Printful order creation failed: %s", order_label, error_msg)
                _insert_mapping({
                    "baselinker_order_id": bl_order_id,
                    "amazon_order_id": amazon_order_id,
                    "printful_external_id": external_id,
                    "status": "error",
                    "error_message": str(error_msg)[:500],
                    "items": json.dumps([{
                        "sku": i["sku"],
                        "variant_id": i["variant_id"],
                        "quantity": i["quantity"],
                    } for i in order_items_for_printful]),
                    "shipping_address": json.dumps(shipping),
                })
                summary["errors"].append({
                    "baselinker_order_id": bl_order_id,
                    "error": str(error_msg),
                })
                continue

            printful_order_id: int = pf_result["id"]
            printful_cost = pf_result.get("costs", {}).get("total", None)
            logger.info(
                "%s — Printful draft created: PF#%d (cost: %s).",
                order_label, printful_order_id, printful_cost,
            )

            # Step 2e: Confirm the Printful order
            logger.info("%s — confirming Printful order PF#%d...", order_label, printful_order_id)
            confirm_result = confirm_printful_order(cfg, printful_order_id)

            if not confirm_result:
                logger.error("%s — Printful confirm failed for PF#%d.", order_label, printful_order_id)
                _insert_mapping({
                    "baselinker_order_id": bl_order_id,
                    "amazon_order_id": amazon_order_id,
                    "printful_order_id": printful_order_id,
                    "printful_external_id": external_id,
                    "status": "error",
                    "error_message": "Confirmation failed",
                    "items": json.dumps([{
                        "sku": i["sku"],
                        "variant_id": i["variant_id"],
                        "quantity": i["quantity"],
                    } for i in order_items_for_printful]),
                    "shipping_address": json.dumps(shipping),
                    "printful_cost": printful_cost,
                })
                summary["errors"].append({
                    "baselinker_order_id": bl_order_id,
                    "error": "confirm_failed",
                })
                continue

            confirmed_status = confirm_result.get("status", "created")
            logger.info("%s — Printful order confirmed (status=%s).", order_label, confirmed_status)

            # Step 3: Store mapping in Supabase
            _insert_mapping({
                "baselinker_order_id": bl_order_id,
                "amazon_order_id": amazon_order_id,
                "printful_order_id": printful_order_id,
                "printful_external_id": external_id,
                "status": confirmed_status,
                "items": json.dumps([{
                    "sku": i["sku"],
                    "variant_id": i["variant_id"],
                    "quantity": i["quantity"],
                } for i in order_items_for_printful]),
                "shipping_address": json.dumps(shipping),
                "printful_cost": printful_cost,
            })

            # Step 4: Mark Baselinker order as "processing"
            try:
                mark_order_processing(cfg, bl_order_id)
                logger.info("%s — Baselinker order marked as processing.", order_label)
            except Exception as e:
                logger.warning("%s — could not mark BL order: %s", order_label, e)

            summary["processed"] += 1

        except Exception as e:
            logger.error("%s — unexpected error: %s", order_label, e, exc_info=True)
            summary["errors"].append({
                "baselinker_order_id": bl_order_id,
                "error": str(e),
            })

    elapsed = time.time() - start_time
    logger.info(
        "process_new_orders done in %.1fs — processed: %d, errors: %d, skipped: %d",
        elapsed, summary["processed"], len(summary["errors"]), len(summary["skipped"]),
    )
    return summary


# ── Tracking sync ────────────────────────────────────────────────────

def sync_tracking(cfg: dict) -> dict[str, Any]:
    """
    Tracking sync loop:
    1. Get all pending Printful orders from DB (status: created, in_production)
    2. Check for tracking updates via Printful API
    3. Update Baselinker with tracking info
    4. Return summary
    """
    summary: dict[str, Any] = {"updated": 0, "errors": [], "still_pending": 0}
    start_time = time.time()

    # Step 1: Get pending mappings
    logger.info("Fetching pending Printful order mappings...")
    pending = _get_pending_mappings()

    if not pending:
        logger.info("No pending orders to track.")
        return summary

    logger.info("Found %d pending orders to check tracking.", len(pending))

    # Step 2-3: Delegate to tracking_sync module for bulk processing
    try:
        tracking_results = sync_all_tracking(cfg, pending)
    except Exception as e:
        logger.error("Tracking sync failed: %s", e, exc_info=True)
        summary["errors"].append({"step": "sync_all_tracking", "error": str(e)})
        return summary

    # Step 4: Process results and update DB
    for result in tracking_results:
        mapping_id = result["mapping_id"]
        try:
            if result.get("error"):
                logger.warning("Mapping #%d — tracking error: %s", mapping_id, result["error"])
                summary["errors"].append({
                    "mapping_id": mapping_id,
                    "error": result["error"],
                })
                continue

            new_status = result.get("status")
            tracking_number = result.get("tracking_number")
            tracking_url = result.get("tracking_url")
            carrier = result.get("carrier")

            # Only update if something changed
            update_data: dict[str, Any] = {}
            if new_status:
                update_data["status"] = new_status
            if tracking_number:
                update_data["tracking_number"] = tracking_number
            if tracking_url:
                update_data["tracking_url"] = tracking_url
            if carrier:
                update_data["carrier"] = carrier

            if update_data:
                _update_mapping(mapping_id, update_data)
                logger.info(
                    "Mapping #%d — updated: status=%s, tracking=%s",
                    mapping_id, new_status, tracking_number,
                )
                summary["updated"] += 1

                # If shipped with tracking, update Baselinker
                if new_status == "shipped" and tracking_number:
                    bl_order_id = result.get("baselinker_order_id")
                    if bl_order_id:
                        logger.info(
                            "Mapping #%d — updating Baselinker BL#%d with tracking %s",
                            mapping_id, bl_order_id, tracking_number,
                        )
            else:
                summary["still_pending"] += 1

        except Exception as e:
            logger.error("Mapping #%d — update error: %s", mapping_id, e, exc_info=True)
            summary["errors"].append({"mapping_id": mapping_id, "error": str(e)})

    elapsed = time.time() - start_time
    logger.info(
        "sync_tracking done in %.1fs — updated: %d, errors: %d, still_pending: %d",
        elapsed, summary["updated"], len(summary["errors"]), summary["still_pending"],
    )
    return summary


# ── Full cycle ───────────────────────────────────────────────────────

def full_cycle(cfg: dict) -> dict[str, Any]:
    """Run both process_new_orders and sync_tracking in sequence."""
    logger.info("=" * 60)
    logger.info("Printful auto-fulfillment — full cycle start")
    logger.info("=" * 60)

    cfg = cfg or load_config()

    orders_result = process_new_orders(cfg)
    tracking_result = sync_tracking(cfg)

    return {
        "orders": orders_result,
        "tracking": tracking_result,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── CLI entry point (standalone) ─────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    cfg = load_config()
    result = full_cycle(cfg)

    print(f"\n{'='*60}")
    print("Printful auto-fulfillment summary:")
    print(f"  Orders processed: {result['orders']['processed']}")
    print(f"  Orders skipped:   {len(result['orders']['skipped'])}")
    print(f"  Order errors:     {len(result['orders']['errors'])}")
    print(f"  Tracking updated: {result['tracking']['updated']}")
    print(f"  Still pending:    {result['tracking']['still_pending']}")
    print(f"  Tracking errors:  {len(result['tracking']['errors'])}")
    print(f"{'='*60}")
