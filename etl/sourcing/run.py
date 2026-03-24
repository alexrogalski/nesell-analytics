"""CLI runner for the product-sourcing profitability analyser.

Analyses supplier price lists (CSV/XLSX) or individual EANs against
Amazon EU and Allegro, calculates fees, estimates velocity, recommends
purchase quantities, and generates reports.

Examples::

    # Analyse a supplier CSV against all 8 Amazon EU markets + Allegro
    python3.11 -m etl.sourcing.run --file supplier.csv

    # Single EAN with explicit purchase price
    python3.11 -m etl.sourcing.run --ean 5904066095280 --purchase-price 15.50

    # Restrict to DE and FR, skip Allegro, require 15 % min margin
    python3.11 -m etl.sourcing.run --file data.xlsx --markets DE,FR \\
        --min-margin 15 --skip-allegro --output report.xlsx

    # Quick check, no cache
    python3.11 -m etl.sourcing.run --file data.csv --no-cache

    # Use Qogita wholesale prices instead of manual purchase price
    python3.11 -m etl.sourcing.run --ean 4006381333931 --qogita

    # Qogita mode: auto-fetch wholesale price, compare with Amazon/Allegro
    python3.11 -m etl.sourcing.run --file eans.csv --qogita
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from .config import SourcingConfig
from .parser import parse_file, SupplierProduct
from .fee_calculator import calculate_amazon_fees, calculate_allegro_fees
from .profit_calculator import analyze_profitability, ProfitAnalysis
from .velocity_estimator import (
    estimate_monthly_sales_amazon,
    estimate_monthly_sales_allegro,
)
from .quantity_recommender import recommend, PurchaseRecommendation
from .report import generate_excel_report, generate_csv, print_terminal_summary
from .cache import get_cached, set_cached

# ---------------------------------------------------------------------------
# Lazy imports for lookup modules (they may pull heavy dependencies)
# ---------------------------------------------------------------------------

_amazon_lookup = None
_allegro_lookup = None
_qogita_client = None


def _get_amazon_lookup():
    global _amazon_lookup
    if _amazon_lookup is None:
        from . import amazon_lookup
        _amazon_lookup = amazon_lookup
    return _amazon_lookup


def _get_allegro_lookup():
    global _allegro_lookup
    if _allegro_lookup is None:
        from . import allegro_lookup
        _allegro_lookup = allegro_lookup
    return _allegro_lookup


def _get_qogita_client():
    global _qogita_client
    if _qogita_client is None:
        try:
            from .qogita_client import QogitaClient
            _qogita_client = QogitaClient()
        except (FileNotFoundError, ValueError):
            _qogita_client = False  # Mark as unavailable
    return _qogita_client if _qogita_client is not False else None


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python3.11 -m etl.sourcing.run",
        description="Analyse product sourcing profitability across Amazon EU and Allegro.",
    )

    # Input: file or single EAN.
    input_group = p.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--file", "-f",
        type=str,
        help="Path to supplier CSV or XLSX price list.",
    )
    input_group.add_argument(
        "--ean",
        type=str,
        help="Single EAN-13 to analyse.",
    )

    # Single-EAN helpers.
    p.add_argument(
        "--purchase-price", "--price", "-p",
        type=float,
        default=None,
        help="Purchase price for --ean mode (required with --ean).",
    )
    p.add_argument(
        "--purchase-currency",
        type=str,
        default="PLN",
        help="Currency of the purchase price (default: PLN).",
    )
    p.add_argument(
        "--weight",
        type=float,
        default=None,
        help="Product weight in kg (overrides config default).",
    )

    # Market filters.
    p.add_argument(
        "--markets",
        type=str,
        default=None,
        help="Comma-separated Amazon market codes to check (e.g. DE,FR,IT).",
    )
    p.add_argument(
        "--skip-allegro",
        action="store_true",
        help="Do not look up prices on Allegro.",
    )

    # Thresholds.
    p.add_argument(
        "--min-margin",
        type=float,
        default=10.0,
        help="Minimum margin %% to consider profitable (default: 10).",
    )
    p.add_argument(
        "--max-investment",
        type=float,
        default=5000.0,
        help="Max investment per product in PLN (default: 5000).",
    )
    p.add_argument(
        "--target-months",
        type=float,
        default=2.0,
        help="Target months of stock to recommend (default: 2).",
    )

    # Output.
    p.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output file path (.xlsx or .csv). Default: sourcing_report.xlsx",
    )
    p.add_argument(
        "--csv",
        action="store_true",
        help="Also generate a CSV alongside the Excel report.",
    )
    p.add_argument(
        "--no-report",
        action="store_true",
        help="Skip file report generation (terminal output only).",
    )

    # Cache and rate-limit.
    p.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass the lookup cache (always fetch fresh data).",
    )
    p.add_argument(
        "--cache-ttl",
        type=int,
        default=24,
        help="Cache TTL in hours (default: 24).",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=None,
        help="Delay in seconds between API lookups (overrides config).",
    )

    # Qogita wholesale source.
    p.add_argument(
        "--qogita",
        action="store_true",
        help="Use Qogita wholesale prices as purchase price source. "
             "Overrides --purchase-price. Requires ~/.keys/qogita.env.",
    )

    # Misc.
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N products from the file.",
    )
    p.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress progress output.",
    )

    return p


# ---------------------------------------------------------------------------
# Config builder
# ---------------------------------------------------------------------------

def _build_config(args: argparse.Namespace) -> SourcingConfig:
    cfg = SourcingConfig(
        min_margin_pct=args.min_margin,
        max_investment_per_product_pln=args.max_investment,
        target_months_stock=args.target_months,
        include_allegro=not args.skip_allegro,
        cache_ttl_hours=args.cache_ttl,
    )
    if args.markets:
        cfg.amazon_markets = [m.strip().upper() for m in args.markets.split(",")]
    if args.delay is not None:
        cfg.amazon_delay_sec = args.delay
        cfg.allegro_delay_sec = args.delay
    return cfg


# ---------------------------------------------------------------------------
# Single-product pipeline
# ---------------------------------------------------------------------------

def _process_product(
    product: SupplierProduct,
    cfg: SourcingConfig,
    use_cache: bool,
    quiet: bool,
) -> tuple[
    list[ProfitAnalysis],
    dict[str, int],
    PurchaseRecommendation | None,
]:
    """Run the full analysis pipeline for one product.

    Returns ``(analyses, monthly_sales_map, recommendation)``.
    """
    ean = product.ean
    weight = product.weight_kg or cfg.default_weight_kg
    # Will be overridden by Amazon-discovered weight if available
    amazon_weight_override = None

    if not quiet:
        label = product.name[:40] if product.name else ean
        print(f"  [{ean}] {label} @ {product.purchase_price} {product.purchase_currency}")

    # ------------------------------------------------------------------
    # Amazon lookup
    # ------------------------------------------------------------------
    amazon_data: dict[str, dict] = {}

    # Check cache per market first.
    markets_to_fetch: list[str] = []
    for market in cfg.amazon_markets:
        cache_key_platform = f"amazon_{market.lower()}"
        if use_cache:
            cached = get_cached(ean, cache_key_platform, ttl_hours=cfg.cache_ttl_hours)
            if cached:
                amazon_data[market] = cached
                if not quiet:
                    print(f"    {market}: cached")
                continue
        markets_to_fetch.append(market)

    # Fetch missing markets in one call (EAN search once, then iterate markets).
    if markets_to_fetch:
        try:
            amz_mod = _get_amazon_lookup()
            results = amz_mod.lookup_ean(ean, markets=markets_to_fetch, delay_sec=cfg.amazon_delay_sec)
            for market, product_data in results.items():
                # Convert dataclass to dict for storage / downstream.
                from dataclasses import asdict
                mdata = asdict(product_data)
                amazon_data[market] = mdata
                if use_cache:
                    set_cached(ean, f"amazon_{market.lower()}", mdata)
                if not quiet:
                    price = mdata.get("buy_box_price") or mdata.get("lowest_fba_price") or "?"
                    print(f"    {market}: {price} {mdata.get('currency', '')}")
        except Exception as exc:
            if not quiet:
                print(f"    Amazon: ERROR {exc}")

            # Use Amazon-discovered weight if supplier didn't provide one
            if amazon_weight_override is None and not product.weight_kg:
                for mdata in amazon_data.values():
                    w = mdata.get("item_weight_kg") or mdata.get("package_weight_kg")
                    if w and w > 0:
                        amazon_weight_override = w
                        weight = w
                        if not quiet:
                            print(f"    Weight from Amazon: {w} kg")
                        break

        # Report markets with no data.
        for market in markets_to_fetch:
            if market not in amazon_data and not quiet:
                print(f"    {market}: not found")

    # ------------------------------------------------------------------
    # Allegro lookup
    # ------------------------------------------------------------------
    allegro_data: dict | None = None

    if cfg.include_allegro:
        if use_cache:
            cached = get_cached(ean, "allegro", ttl_hours=cfg.cache_ttl_hours)
            if cached:
                allegro_data = cached
                if not quiet:
                    print(f"    Allegro: cached")

        if allegro_data is None:
            try:
                alg_mod = _get_allegro_lookup()
                alg_result = alg_mod.lookup_ean(ean)
                if alg_result:
                    from dataclasses import asdict
                    allegro_data = asdict(alg_result)
                    if use_cache:
                        set_cached(ean, "allegro", allegro_data)
                    if not quiet:
                        price = allegro_data.get("lowest_price", "?")
                        print(f"    Allegro: {price} PLN")
                else:
                    if not quiet:
                        print(f"    Allegro: not found")
            except Exception as exc:
                if not quiet:
                    print(f"    Allegro: ERROR {exc}")
                allegro_data = None

            time.sleep(cfg.allegro_delay_sec)

    # ------------------------------------------------------------------
    # Profitability analysis
    # ------------------------------------------------------------------
    analyses = analyze_profitability(
        ean=ean,
        purchase_price=product.purchase_price,
        purchase_currency=product.purchase_currency,
        amazon_data=amazon_data if amazon_data else None,
        allegro_data=allegro_data,
        config=cfg,
        weight_kg=weight,
    )

    # ------------------------------------------------------------------
    # Velocity estimation
    # ------------------------------------------------------------------
    monthly_sales: dict[str, int] = {}

    for a in analyses:
        if a.platform.startswith("amazon_"):
            market_code = a.platform.replace("amazon_", "").upper()
            # Get enriched data for this market from amazon_data
            mdata = amazon_data.get(market_code, {})
            est = estimate_monthly_sales_amazon(
                bsr_rank=a.bsr_rank,
                marketplace=market_code,
                category=mdata.get("category", ""),
                num_sellers=mdata.get("num_total_offers", 0),
                has_fba=bool(mdata.get("num_fba_sellers", 0)),
                subcategory_bsr=mdata.get("bsr_subcategory_rank"),
            )
            monthly_sales[a.platform] = int(est)
            a.estimated_monthly_sales = int(est)
        elif a.platform == "allegro" and allegro_data:
            est = estimate_monthly_sales_allegro(
                offer_count=allegro_data.get("offer_count", 0),
                avg_price=allegro_data.get("avg_price", 100.0),
            )
            monthly_sales["allegro"] = int(est)
            a.estimated_monthly_sales = int(est)

    # ------------------------------------------------------------------
    # Purchase recommendation
    # ------------------------------------------------------------------
    from ..fx_rates import fetch_nbp_rate
    from datetime import date

    if product.purchase_currency == "PLN":
        purchase_pln = product.purchase_price
    else:
        rate = fetch_nbp_rate(product.purchase_currency, date.today())
        purchase_pln = round(product.purchase_price * (rate or 4.27), 2)

    rec = recommend(
        ean=ean,
        purchase_price_pln=purchase_pln,
        analyses=analyses,
        monthly_sales=monthly_sales,
        config=cfg,
        name=product.name,
    )

    return analyses, monthly_sales, rec


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns 0 on success, 1 on error."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Validate single-EAN mode.
    use_qogita = getattr(args, "qogita", False)
    if args.ean and args.purchase_price is None and not use_qogita:
        parser.error("--purchase-price is required when using --ean (or use --qogita)")

    cfg = _build_config(args)
    use_cache = not args.no_cache
    quiet = args.quiet

    # Qogita client (lazy init)
    qogita = None
    if use_qogita:
        qogita = _get_qogita_client()
        if qogita is None:
            print("WARNING: Qogita credentials not found (~/.keys/qogita.env). "
                  "Falling back to manual prices.", file=sys.stderr)

    # ------------------------------------------------------------------
    # Build product list
    # ------------------------------------------------------------------
    if args.file:
        file_path = Path(args.file).expanduser().resolve()
        if not file_path.exists():
            print(f"ERROR: File not found: {file_path}", file=sys.stderr)
            return 1

        if not quiet:
            print(f"Parsing {file_path.name} ...")

        products = parse_file(str(file_path))
        if not products:
            print("No valid products found in the file.", file=sys.stderr)
            return 1

        if not quiet:
            print(f"Found {len(products)} valid products\n")
    else:
        purchase_price = args.purchase_price or 0.0
        purchase_currency = args.purchase_currency
        ean_name = ""

        # Qogita mode: fetch wholesale price for single EAN
        if use_qogita and qogita and (not purchase_price or purchase_price <= 0):
            if not quiet:
                print(f"Fetching Qogita wholesale price for {args.ean} ...")
            qprod = qogita.get_variant(args.ean)
            if qprod and qprod.lowest_price and qprod.lowest_price > 0:
                purchase_price = qprod.lowest_price
                purchase_currency = qprod.currency
                ean_name = qprod.name
                if not quiet:
                    print(f"  Qogita: {qprod.name[:50]}")
                    print(f"  Brand: {qprod.brand}")
                    print(f"  Price: {purchase_price} {purchase_currency}")
                    print(f"  Available: {qprod.total_available_qty} units")
                    print()
            else:
                if not quiet:
                    print(f"  Qogita: product not found or no price\n")

        if purchase_price <= 0:
            print("ERROR: No purchase price available. Use --purchase-price or --qogita.",
                  file=sys.stderr)
            return 1

        products = [
            SupplierProduct(
                ean=args.ean,
                purchase_price=purchase_price,
                purchase_currency=purchase_currency,
                name=ean_name,
                weight_kg=args.weight,
            )
        ]

    # ------------------------------------------------------------------
    # Qogita enrichment for file mode: fetch wholesale prices for
    # products that don't have a purchase price
    # ------------------------------------------------------------------
    if use_qogita and qogita and args.file:
        enriched = 0
        for prod in products:
            if prod.purchase_price <= 0:
                qprod = qogita.get_variant(prod.ean)
                if qprod and qprod.lowest_price and qprod.lowest_price > 0:
                    prod.purchase_price = qprod.lowest_price
                    prod.purchase_currency = qprod.currency
                    if not prod.name:
                        prod.name = qprod.name
                    enriched += 1
                time.sleep(0.3)
        if enriched and not quiet:
            print(f"Enriched {enriched} products with Qogita prices\n")

    # Apply --limit.
    if args.limit and args.limit < len(products):
        products = products[: args.limit]
        if not quiet:
            print(f"Limited to first {args.limit} products\n")

    # ------------------------------------------------------------------
    # Run analysis
    # ------------------------------------------------------------------
    all_results: dict[str, list[ProfitAnalysis]] = {}
    all_recs: dict[str, PurchaseRecommendation] = {}
    errors = 0
    start_time = time.time()

    for idx, product in enumerate(products, 1):
        if not quiet:
            print(f"\n[{idx}/{len(products)}]", end=" ")

        try:
            analyses, _, rec = _process_product(product, cfg, use_cache, quiet)
            all_results[product.ean] = analyses
            if rec:
                all_recs[product.ean] = rec
        except Exception as exc:
            errors += 1
            print(f"  FAILED: {exc}", file=sys.stderr)

    elapsed = time.time() - start_time

    # ------------------------------------------------------------------
    # Terminal output
    # ------------------------------------------------------------------
    print_terminal_summary(all_results, all_recs, min_margin=cfg.min_margin_pct)

    # Stats line.
    total = len(all_results)
    profitable = sum(
        1 for ean, analyses in all_results.items()
        if any(a.margin_pct >= cfg.min_margin_pct and not a.errors for a in analyses)
    )
    best_roi = 0.0
    best_ean = ""
    for ean, analyses in all_results.items():
        for a in analyses:
            if a.roi_pct > best_roi and not a.errors:
                best_roi = a.roi_pct
                best_ean = ean

    print(f"  {total} products analysed, {profitable} profitable, {errors} errors")
    if best_ean:
        print(f"  Best ROI: {best_roi:.1f}% (EAN {best_ean})")
    print(f"  Completed in {elapsed:.1f}s\n")

    # ------------------------------------------------------------------
    # File reports
    # ------------------------------------------------------------------
    if not args.no_report and all_results:
        output = args.output
        if output is None:
            output = "sourcing_report.xlsx"

        out_path = Path(output).expanduser().resolve()

        if out_path.suffix.lower() == ".csv":
            csv_path = generate_csv(all_results, path=str(out_path))
            print(f"  CSV report: {csv_path}")
        else:
            xlsx_path = generate_excel_report(
                all_results, all_recs,
                path=str(out_path),
                min_margin=cfg.min_margin_pct,
            )
            print(f"  Excel report: {xlsx_path}")

            if args.csv:
                csv_name = out_path.with_suffix(".csv")
                csv_path = generate_csv(all_results, path=str(csv_name))
                print(f"  CSV report:   {csv_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
