#!/usr/bin/env python3.11
"""
nesell-analytics ETL runner.

Usage:
    python3.11 -m etl.run              # full sync (FX + orders + products + aggregate)
    python3.11 -m etl.run --fx         # only FX rates
    python3.11 -m etl.run --orders     # only Baselinker orders
    python3.11 -m etl.run --fba        # only Amazon FBA orders
    python3.11 -m etl.run --products   # only product catalog
    python3.11 -m etl.run --aggregate  # only re-aggregate daily metrics
    python3.11 -m etl.run --days 30    # lookback period (default 90)
"""
import argparse, sys, time
from datetime import datetime
from . import db, fx_rates, baselinker, amazon, amazon_fees, aggregator


def main():
    parser = argparse.ArgumentParser(description="nesell-analytics ETL")
    parser.add_argument("--fx", action="store_true", help="Sync FX rates")
    parser.add_argument("--orders", action="store_true", help="Sync Baselinker orders")
    parser.add_argument("--fba", action="store_true", help="Sync Amazon FBA orders")
    parser.add_argument("--products", action="store_true", help="Sync product catalog")
    parser.add_argument("--fees", action="store_true", help="Fetch real Amazon fees from Finances API")
    parser.add_argument("--aggregate", action="store_true", help="Aggregate daily metrics")
    parser.add_argument("--days", type=int, default=90, help="Days to look back")
    args = parser.parse_args()

    # If no flags → run everything
    run_all = not any([args.fx, args.orders, args.fba, args.products, args.fees, args.aggregate])

    print(f"{'='*60}")
    print(f"nesell-analytics ETL — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    conn = db.get_conn()
    start = time.time()

    try:
        if run_all or args.fx:
            print("\n[1/5] Syncing FX rates...")
            fx_rates.sync_fx_rates(conn, days_back=args.days)

        if run_all or args.orders:
            print("\n[2/5] Syncing Baselinker orders...")
            baselinker.sync_orders(conn, days_back=args.days)

        if run_all or args.fba:
            print("\n[3/5] Syncing Amazon FBA orders...")
            amazon.sync_orders(conn, days_back=args.days)

        if run_all or args.products:
            print("\n[4/6] Syncing product catalog...")
            baselinker.sync_products(conn)

        if run_all or args.fees:
            print("\n[5/6] Fetching real Amazon fees...")
            amazon_fees.sync_fees(conn, days_back=args.days)

        if run_all or args.aggregate:
            print("\n[6/6] Aggregating daily metrics...")
            aggregator.aggregate_daily(conn, days_back=args.days)

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
