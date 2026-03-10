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
    python3.11 -m etl.run --reports      # Amazon reports (traffic, inventory, etc.)
    python3.11 -m etl.run --amzdata      # Amazon live API (BSR, pricing, inventory)
    python3.11 -m etl.run --aggregate    # re-aggregate daily metrics
    python3.11 -m etl.run --printful-orders  # process new Printful orders
    python3.11 -m etl.run --tracking-sync    # sync Printful tracking info
    python3.11 -m etl.run --days 30      # lookback period (default 90)
"""
import argparse, sys, time
from datetime import datetime
from . import db, fx_rates, baselinker, amazon, amazon_fees, amazon_reports, amazon_data, aggregator


def main():
    parser = argparse.ArgumentParser(description="nesell-analytics ETL")
    parser.add_argument("--fx", action="store_true", help="Sync FX rates")
    parser.add_argument("--orders", action="store_true", help="Sync Baselinker orders")
    parser.add_argument("--fba", action="store_true", help="Sync Amazon FBA orders")
    parser.add_argument("--products", action="store_true", help="Sync product catalog")
    parser.add_argument("--fees", action="store_true", help="Fetch real Amazon fees (Finances API)")
    parser.add_argument("--reports", action="store_true", help="Amazon reports (traffic, inventory, fees, returns)")
    parser.add_argument("--amzdata", action="store_true", help="Amazon live APIs (BSR, pricing, inventory)")
    parser.add_argument("--aggregate", action="store_true", help="Aggregate daily metrics")
    parser.add_argument("--printful-orders", action="store_true", help="Process new Printful auto-fulfillment orders")
    parser.add_argument("--tracking-sync", action="store_true", help="Sync Printful tracking info to Baselinker")
    parser.add_argument("--days", type=int, default=90, help="Days to look back")
    args = parser.parse_args()

    all_flags = [args.fx, args.orders, args.fba, args.products, args.fees,
                 args.reports, args.amzdata, args.aggregate]
    # Printful automation flags are opt-in only (never run in "run_all" mode)
    run_all = not any(all_flags) and not args.printful_orders and not args.tracking_sync

    print(f"{'='*60}")
    print(f"nesell-analytics ETL — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    conn = db.get_conn()
    start = time.time()
    step = 0
    total_steps = 8

    try:
        if run_all or args.fx:
            step += 1
            print(f"\n[{step}/{total_steps}] Syncing FX rates...")
            fx_rates.sync_fx_rates(conn, days_back=args.days)

        if run_all or args.orders:
            step += 1
            print(f"\n[{step}/{total_steps}] Syncing Baselinker orders...")
            baselinker.sync_orders(conn, days_back=args.days)

        if run_all or args.fba:
            step += 1
            print(f"\n[{step}/{total_steps}] Syncing Amazon FBA orders...")
            amazon.sync_orders(conn, days_back=args.days)

        if run_all or args.products:
            step += 1
            print(f"\n[{step}/{total_steps}] Syncing product catalog...")
            baselinker.sync_products(conn)

        if run_all or args.fees:
            step += 1
            print(f"\n[{step}/{total_steps}] Fetching real Amazon fees...")
            amazon_fees.sync_fees(conn, days_back=args.days)

        if run_all or args.reports:
            step += 1
            print(f"\n[{step}/{total_steps}] Fetching Amazon reports...")
            amazon_reports.sync_all_reports(conn, days_back=args.days)

        if run_all or args.amzdata:
            step += 1
            print(f"\n[{step}/{total_steps}] Fetching Amazon live data (BSR, pricing, inventory)...")
            amazon_data.sync_all_data(conn, days_back=min(args.days, 30))

        if run_all or args.aggregate:
            step += 1
            print(f"\n[{step}/{total_steps}] Aggregating daily metrics...")
            aggregator.aggregate_daily(conn, days_back=args.days)

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
            result = process_new_orders(pf_cfg)
            print(f"  Processed: {result['processed']}, Errors: {len(result['errors'])}, Skipped: {len(result['skipped'])}")

        if args.tracking_sync:
            step += 1
            print(f"\n[{step}] Syncing Printful tracking info...")
            result = sync_tracking(pf_cfg)
            print(f"  Updated: {result['updated']}, Errors: {len(result['errors'])}, Still pending: {result['still_pending']}")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"Done in {elapsed:.1f}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
