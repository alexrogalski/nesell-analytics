#!/usr/bin/env python3.11
"""
nesell-analytics ETL runner.

Usage:
    python3.11 -m etl.run                # full sync (all steps)
    python3.11 -m etl.run --fx           # only FX rates
    python3.11 -m etl.run --orders       # only Baselinker orders
    python3.11 -m etl.run --fba          # only Amazon FBA orders
    python3.11 -m etl.run --products     # only product catalog
    python3.11 -m etl.run --fees         # real Amazon fees (Finances API)
    python3.11 -m etl.run --allegro-fees # real Allegro fees (Billing API)
    python3.11 -m etl.run --reports      # Amazon reports (traffic, inventory, etc.)
    python3.11 -m etl.run --amzdata      # Amazon live API (BSR, pricing, inventory)
    python3.11 -m etl.run --aggregate    # re-aggregate daily metrics
    python3.11 -m etl.run --images       # fetch missing product images
    python3.11 -m etl.run --cogs              # fill missing COGS from all sources
    python3.11 -m etl.run --shipping          # estimate DPD shipping costs
    python3.11 -m etl.run --dpd-csv file.csv  # import actual costs from DPD invoice CSV
    python3.11 -m etl.run --printful-orders   # process new Printful orders
    python3.11 -m etl.run --tracking-sync    # sync Printful tracking info
    python3.11 -m etl.run --days 30      # lookback period (default 90)
"""
import argparse, sys, time, traceback
from datetime import datetime
from . import db, fx_rates, baselinker, amazon, amazon_fees, allegro_fees, amazon_reports, amazon_data, aggregator


def _run_step(step_num: int, total: int, label: str, func, *args, **kwargs):
    """Run a single ETL step with isolated error handling. Returns True on success."""
    print(f"\n[{step_num}/{total}] {label}...")
    try:
        func(*args, **kwargs)
        return True
    except Exception as e:
        print(f"  [FAILED] {label}: {e}")
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(description="nesell-analytics ETL")
    parser.add_argument("--fx", action="store_true", help="Sync FX rates")
    parser.add_argument("--orders", action="store_true", help="Sync Baselinker orders")
    parser.add_argument("--fba", action="store_true", help="Sync Amazon FBA orders")
    parser.add_argument("--products", action="store_true", help="Sync product catalog")
    parser.add_argument("--fees", action="store_true", help="Fetch real Amazon fees (Finances API)")
    parser.add_argument("--allegro-fees", action="store_true", help="Fetch real Allegro fees (Billing API)")
    parser.add_argument("--reports", action="store_true", help="Amazon reports (traffic, inventory, fees, returns)")
    parser.add_argument("--amzdata", action="store_true", help="Amazon live APIs (BSR, pricing, inventory)")
    parser.add_argument("--aggregate", action="store_true", help="Aggregate daily metrics")
    parser.add_argument("--printful-orders", action="store_true", help="Process new Printful auto-fulfillment orders")
    parser.add_argument("--tracking-sync", action="store_true", help="Sync Printful tracking info to Baselinker")
    parser.add_argument("--images", action="store_true", help="Fetch missing product images from Baselinker")
    parser.add_argument("--cogs", action="store_true", help="Fill missing COGS from all available sources")
    parser.add_argument("--shipping", action="store_true", help="Estimate DPD shipping costs for FBM orders")
    parser.add_argument("--dpd-csv", type=str, default=None, help="Import actual DPD costs from invoice CSV file")
    parser.add_argument("--ads-csv", type=str, default=None, help="Import Amazon advertising report CSV")
    parser.add_argument("--ads-marketplace", type=str, default=None, help="Marketplace ID for ads CSV (default: DE)")
    parser.add_argument("--days", type=int, default=90, help="Days to look back")
    args = parser.parse_args()

    all_flags = [args.fx, args.orders, args.fba, args.products, args.fees,
                 args.allegro_fees, args.reports, args.amzdata, args.aggregate, args.images,
                 args.cogs, args.shipping]
    # Printful automation flags are opt-in only (never run in "run_all" mode)
    run_all = not any(all_flags) and not args.printful_orders and not args.tracking_sync and not args.dpd_csv

    print(f"{'='*60}")
    print(f"nesell-analytics ETL — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    conn = db.get_conn()
    start = time.time()
    step = 0
    total_steps = 9
    failures = []

    if run_all or args.fx:
        step += 1
        if not _run_step(step, total_steps, "Syncing FX rates",
                         fx_rates.sync_fx_rates, conn, days_back=args.days):
            failures.append("FX rates")

    if run_all or args.orders:
        step += 1
        if not _run_step(step, total_steps, "Syncing Baselinker orders",
                         baselinker.sync_orders, conn, days_back=args.days):
            failures.append("Baselinker orders")

    if run_all or args.fba:
        step += 1
        if not _run_step(step, total_steps, "Syncing Amazon FBA orders",
                         amazon.sync_orders, conn, days_back=args.days):
            failures.append("Amazon FBA orders")

    if run_all or args.products:
        step += 1
        if not _run_step(step, total_steps, "Syncing product catalog",
                         baselinker.sync_products, conn):
            failures.append("Product catalog")

    if run_all or args.fees:
        step += 1
        if not _run_step(step, total_steps, "Fetching real Amazon fees",
                         amazon_fees.sync_fees, conn, days_back=args.days):
            failures.append("Amazon fees")

    if run_all or args.allegro_fees:
        step += 1
        if not _run_step(step, total_steps, "Fetching real Allegro fees",
                         allegro_fees.sync_allegro_fees, conn, days_back=args.days):
            failures.append("Allegro fees")

    if run_all or args.reports:
        step += 1
        if not _run_step(step, total_steps, "Fetching Amazon reports",
                         amazon_reports.sync_all_reports, conn, days_back=args.days):
            failures.append("Amazon reports")

    if run_all or args.amzdata:
        step += 1
        if not _run_step(step, total_steps, "Fetching Amazon live data (BSR, pricing, inventory)",
                         amazon_data.sync_all_data, conn, days_back=min(args.days, 30)):
            failures.append("Amazon live data")

    if run_all or args.aggregate:
        step += 1
        if not _run_step(step, total_steps, "Aggregating daily metrics",
                         aggregator.aggregate_daily, conn, days_back=args.days):
            failures.append("Aggregation")

    if args.images:
        step += 1
        from . import fetch_images
        if not _run_step(step, total_steps, "Fetching missing product images",
                         fetch_images.run):
            failures.append("Product images")

    if args.cogs:
        step += 1
        from . import cogs_filler
        if not _run_step(step, total_steps, "Filling missing COGS",
                         cogs_filler.fill_cogs, conn):
            failures.append("COGS filler")

    if run_all or args.shipping:
        step += 1
        from . import shipping_costs
        if not _run_step(step, total_steps, "Estimating DPD shipping costs",
                         shipping_costs.sync_shipping_costs, conn, days_back=args.days):
            failures.append("Shipping costs")

    if args.dpd_csv:
        step += 1
        from . import shipping_costs
        if not _run_step(step, total_steps, f"Importing DPD costs from CSV: {args.dpd_csv}",
                         shipping_costs.import_dpd_csv, conn, args.dpd_csv):
            failures.append("DPD CSV import")

    if args.ads_csv:
        step += 1
        from . import amazon_ads
        if not _run_step(step, total_steps, f"Importing Amazon ads CSV: {args.ads_csv}",
                         amazon_ads.import_ads_csv, conn, args.ads_csv, args.ads_marketplace):
            failures.append("Amazon ads CSV import")

    # ── Printful auto-fulfillment (opt-in only, never in run_all) ──
    if args.printful_orders or args.tracking_sync:
        import logging
        from .order_automation import process_new_orders, sync_tracking, load_config
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        pf_cfg = load_config()

    if args.printful_orders:
        step += 1
        print(f"\n[{step}] Processing new Printful auto-fulfillment orders...")
        try:
            result = process_new_orders(pf_cfg)
            print(f"  Processed: {result['processed']}, Errors: {len(result['errors'])}, Skipped: {len(result['skipped'])}")
        except Exception as e:
            print(f"  [FAILED] Printful orders: {e}")
            traceback.print_exc()
            failures.append("Printful orders")

    if args.tracking_sync:
        step += 1
        print(f"\n[{step}] Syncing Printful tracking info...")
        try:
            result = sync_tracking(pf_cfg)
            print(f"  Updated: {result['updated']}, Errors: {len(result['errors'])}, Still pending: {result['still_pending']}")
        except Exception as e:
            print(f"  [FAILED] Printful tracking: {e}")
            traceback.print_exc()
            failures.append("Printful tracking")

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    if failures:
        print(f"Done in {elapsed:.1f}s with {len(failures)} FAILED step(s): {', '.join(failures)}")
        sys.exit(1)
    else:
        print(f"Done in {elapsed:.1f}s (all steps OK)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
