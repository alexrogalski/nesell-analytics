"""Competitor Price Monitor: fetch buy box + top 3 competitors for our ASINs.

For each ASIN on Amazon DE (primary marketplace):
  1. Get our listing price via Listings Items API
  2. Get buy box price + top 3 competitor offers via getItemOffers (pricing v0)
  3. Flag products where our price > buybox * 1.10 (more than 10% above buy box)
  4. Save snapshot to Supabase amazon_pricing table

Usage:
    cd ~/nesell-analytics
    python3.11 -m etl.competitor_prices --limit 20
    python3.11 -m etl.competitor_prices --marketplace DE --limit 50
    python3.11 -m etl.competitor_prices --all-marketplaces
"""
import argparse
import json
import time
from datetime import date
from typing import Optional

from . import config, db
from .amazon_api import api_get, api_post, ALL_EU_MARKETPLACE_IDS

# ── Constants ─────────────────────────────────────────────────────────

SELLER_ID = config.AMZ_SELLER_ID

MARKETPLACE_IDS = {
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS",
    "NL": "A1805IZSGTT6HS",
    "PL": "A1C3SOZRARQ6R3",
    "SE": "A2NODRKZP88ZB9",
    "BE": "AMEN7PMS3EDWL",
}

OVERPRICE_THRESHOLD = 0.10  # flag if our price > buy_box * (1 + this)


# ── Data fetching ─────────────────────────────────────────────────────

def get_our_asins(limit: Optional[int] = None) -> list[dict]:
    """Collect our ASIN→SKU pairs from FBA inventory + products + order_items."""
    seen: dict[str, str] = {}

    # 1. FBA inventory (most reliable — these are active FBA listings)
    inv = db._get("amazon_inventory", {
        "select": "sku,asin",
        "asin": "not.is.null",
        "order": "fulfillable_qty.desc",
        "limit": "500",
    })
    for it in inv:
        asin = it.get("asin", "").strip()
        sku = it.get("sku", "").strip()
        if asin and asin not in seen:
            seen[asin] = sku

    # 2. Products table
    products = db._get("products", {
        "select": "sku,asin",
        "asin": "not.is.null",
        "limit": "500",
    })
    for p in products:
        asin = p.get("asin", "").strip()
        sku = p.get("sku", "").strip()
        if asin and asin not in seen:
            seen[asin] = sku

    # 3. Order items (recent 90 days — catches anything not yet in products)
    items = db._get("order_items", {
        "select": "asin,sku",
        "asin": "not.is.null",
        "order": "created_at.desc",
        "limit": "1000",
    })
    for it in items:
        asin = it.get("asin", "").strip()
        sku = it.get("sku", "").strip()
        if asin and asin not in seen:
            seen[asin] = sku

    result = [{"asin": asin, "sku": sku} for asin, sku in seen.items()]

    if limit:
        result = result[:limit]

    return result


def get_our_price(sku: str, marketplace_id: str) -> Optional[float]:
    """Fetch our listing price from the Listings Items API.

    Returns the price (incl. VAT / our_price) or None if not found.
    Rate limit: 5 req/sec.
    """
    data = api_get(
        f"/listings/2021-08-01/items/{SELLER_ID}/{sku}",
        params={
            "marketplaceIds": marketplace_id,
            "includedData": "attributes",
        },
    )

    # Parse purchasable_offer -> our_price -> schedule -> value_with_tax
    for offer in data.get("attributes", {}).get("purchasable_offer", []):
        for price_obj in offer.get("our_price", []):
            for schedule in price_obj.get("schedule", []):
                val = schedule.get("value_with_tax")
                if val is not None:
                    return float(val)
    return None


def get_item_offers(asin: str, marketplace_id: str) -> dict:
    """Fetch buy box + all competitor offers via getItemOffers (pricing v0).

    Returns dict with:
      buy_box_price, buy_box_seller, buy_box_fba,
      lowest_fba_price, lowest_fbm_price,
      num_offers_new, num_offers_used,
      competitors: [{seller_id, price, fba, prime, buy_box_winner}] (top 3)
      currency
    """
    data = api_get(
        f"/products/pricing/v0/items/{asin}/offers",
        params={
            "MarketplaceId": marketplace_id,
            "ItemCondition": "New",
        },
    )

    payload = data.get("payload", {})
    summary = payload.get("Summary", {})
    offers_raw = payload.get("Offers", [])

    # Extract currency
    currency = "EUR"
    for lp in summary.get("LowestPrices", []):
        currency = lp.get("LandedPrice", {}).get("CurrencyCode", "EUR")
        break

    # Number of offers
    num_new = 0
    num_used = 0
    for offer_count in summary.get("NumberOfOffers", []):
        cond = offer_count.get("condition", "")
        cnt = offer_count.get("OfferCount", 0) or 0
        if cond == "new":
            num_new += cnt
        elif cond == "used":
            num_used += cnt

    # Lowest FBA / FBM from summary
    lowest_fba: Optional[float] = None
    lowest_fbm: Optional[float] = None
    for lp in summary.get("LowestPrices", []):
        if lp.get("condition") != "new":
            continue
        channel = lp.get("fulfillmentChannel", "")
        price_val = float(lp.get("LandedPrice", {}).get("Amount", 0) or 0)
        if channel == "Amazon":
            if lowest_fba is None or price_val < lowest_fba:
                lowest_fba = price_val
        elif channel == "Merchant":
            if lowest_fbm is None or price_val < lowest_fbm:
                lowest_fbm = price_val

    # Parse individual offers — find buy box winner, build competitor list
    buy_box_price: Optional[float] = None
    buy_box_seller: Optional[str] = None
    buy_box_fba: Optional[bool] = None

    # Sort offers by listing price ascending for "top 3"
    all_offers = []
    for o in offers_raw:
        price_val = float(o.get("ListingPrice", {}).get("Amount", 0) or 0)
        shipping_val = float(o.get("Shipping", {}).get("Amount", 0) or 0)
        landed = price_val + shipping_val
        seller_id = o.get("SellerId", "")
        is_fba = bool(o.get("IsFulfilledByAmazon", False))
        is_prime = bool(o.get("PrimeInformation", {}).get("IsPrime", False))
        is_winner = bool(o.get("IsBuyBoxWinner", False))

        if is_winner:
            buy_box_price = price_val
            buy_box_seller = seller_id
            buy_box_fba = is_fba

        all_offers.append({
            "seller_id": seller_id,
            "price": price_val,
            "shipping": shipping_val,
            "landed": landed,
            "fba": is_fba,
            "prime": is_prime,
            "buy_box_winner": is_winner,
        })

    # If no explicit buy box winner, use lowest landed price
    if buy_box_price is None and all_offers:
        cheapest = min(all_offers, key=lambda x: x["landed"])
        buy_box_price = cheapest["price"]
        buy_box_seller = cheapest["seller_id"]
        buy_box_fba = cheapest["fba"]

    # Sort competitors by landed price, exclude ourselves
    competitors_sorted = sorted(
        [o for o in all_offers if o["seller_id"] != SELLER_ID],
        key=lambda x: x["landed"],
    )
    top3 = competitors_sorted[:3]

    return {
        "buy_box_price": buy_box_price,
        "buy_box_seller": buy_box_seller,
        "buy_box_fba": buy_box_fba,
        "lowest_fba_price": lowest_fba,
        "lowest_fbm_price": lowest_fbm,
        "num_offers_new": num_new,
        "num_offers_used": num_used,
        "competitors": top3,
        "currency": currency,
        "all_offers_count": len(all_offers),
    }


# ── Core logic ────────────────────────────────────────────────────────

def analyze_asin(asin: str, sku: str, marketplace_id: str) -> Optional[dict]:
    """Fetch pricing data for one ASIN and return a result dict."""
    # Our price
    our_price = get_our_price(sku, marketplace_id)
    time.sleep(0.2)  # listings API: 5 req/sec

    # Buy box + competitor offers
    offers_data = get_item_offers(asin, marketplace_id)
    time.sleep(1.0)  # pricing v0: 1 req/sec burst

    buy_box_price = offers_data.get("buy_box_price")
    currency = offers_data.get("currency", "EUR")

    # Flag: our price > buybox * 1.10
    price_flag = False
    price_delta_pct = None
    if our_price and buy_box_price and buy_box_price > 0:
        price_delta_pct = round((our_price - buy_box_price) / buy_box_price * 100, 2)
        price_flag = price_delta_pct > OVERPRICE_THRESHOLD * 100

    return {
        "asin": asin,
        "sku": sku,
        "marketplace_id": marketplace_id,
        "our_price": our_price,
        "buy_box_price": buy_box_price,
        "buy_box_seller": offers_data.get("buy_box_seller"),
        "buy_box_fba": offers_data.get("buy_box_fba"),
        "buy_box_shipping": 0.0,  # getItemOffers returns landed price
        "buy_box_landed_price": buy_box_price,
        "lowest_fba_price": offers_data.get("lowest_fba_price"),
        "lowest_fbm_price": offers_data.get("lowest_fbm_price"),
        "num_offers_new": offers_data.get("num_offers_new", 0),
        "num_offers_used": offers_data.get("num_offers_used", 0),
        "list_price": None,
        "currency": currency,
        "price_delta_pct": price_delta_pct,
        "price_flag": price_flag,
        "competitors": offers_data.get("competitors", []),
        "all_offers_count": offers_data.get("all_offers_count", 0),
    }


def save_to_db(results: list[dict]) -> int:
    """Upsert pricing records to amazon_pricing table (without extended fields)."""
    if not results:
        return 0

    today = str(date.today())
    records = []
    for r in results:
        records.append({
            "snapshot_date": today,
            "asin": r["asin"],
            "marketplace_id": r["marketplace_id"],
            "buy_box_price": r.get("buy_box_price"),
            "buy_box_shipping": r.get("buy_box_shipping"),
            "buy_box_landed_price": r.get("buy_box_landed_price"),
            "lowest_fba_price": r.get("lowest_fba_price"),
            "lowest_fbm_price": r.get("lowest_fbm_price"),
            "num_offers_new": r.get("num_offers_new", 0),
            "num_offers_used": r.get("num_offers_used", 0),
            "list_price": r.get("list_price"),
            "our_price": r.get("our_price"),
            "currency": r.get("currency", "EUR"),
        })

    return db.upsert_amazon_pricing(None, records)


# ── Reporting ─────────────────────────────────────────────────────────

def print_results(results: list[dict], marketplace_code: str) -> None:
    """Pretty-print results table with flagged products highlighted."""
    flagged = [r for r in results if r.get("price_flag")]
    no_bb = [r for r in results if r.get("buy_box_price") is None]
    no_our = [r for r in results if r.get("our_price") is None]
    normal = [r for r in results if not r.get("price_flag") and r.get("buy_box_price") and r.get("our_price")]

    print(f"\n{'='*72}")
    print(f"  COMPETITOR PRICE REPORT — Amazon {marketplace_code} — {date.today()}")
    print(f"  Total ASINs analyzed: {len(results)}")
    print(f"  Flagged (our price >10% above buy box): {len(flagged)}")
    print(f"  No buy box found: {len(no_bb)}")
    print(f"  No our price found: {len(no_our)}")
    print(f"{'='*72}")

    if flagged:
        print(f"\n  *** FLAGGED — PRICE TOO HIGH ***")
        print(f"  {'ASIN':<14} {'SKU':<25} {'Ours':>8} {'BuyBox':>8} {'Delta':>8}  {'#Sellers':>8}  {'Top Competitor'}")
        print(f"  {'-'*110}")
        for r in sorted(flagged, key=lambda x: x.get("price_delta_pct", 0), reverse=True):
            our = f"{r['our_price']:.2f}" if r['our_price'] else "N/A"
            bb = f"{r['buy_box_price']:.2f}" if r['buy_box_price'] else "N/A"
            delta = f"+{r['price_delta_pct']:.1f}%" if r['price_delta_pct'] else ""
            comps = r.get("competitors", [])
            top_comp = ""
            if comps:
                c = comps[0]
                top_comp = f"Seller {c['seller_id'][:10]} @ {c['price']:.2f} {r['currency']} {'(FBA)' if c['fba'] else '(FBM)'}"
            sku_short = r["sku"][:24]
            print(f"  {r['asin']:<14} {sku_short:<25} {our:>8} {bb:>8} {delta:>8}  {r['num_offers_new']:>8}  {top_comp}")

    if normal:
        print(f"\n  OK — Price competitive or no issue")
        print(f"  {'ASIN':<14} {'SKU':<25} {'Ours':>8} {'BuyBox':>8} {'Delta':>8}  {'#Sellers':>8}")
        print(f"  {'-'*80}")
        for r in sorted(normal, key=lambda x: x.get("price_delta_pct") or 0, reverse=True):
            our = f"{r['our_price']:.2f}" if r['our_price'] else "N/A"
            bb = f"{r['buy_box_price']:.2f}" if r['buy_box_price'] else "N/A"
            delta_val = r.get("price_delta_pct")
            delta = (f"+{delta_val:.1f}%" if delta_val and delta_val > 0
                     else f"{delta_val:.1f}%" if delta_val else "N/A")
            sku_short = r["sku"][:24]
            print(f"  {r['asin']:<14} {sku_short:<25} {our:>8} {bb:>8} {delta:>8}  {r['num_offers_new']:>8}")

    if no_bb or no_our:
        print(f"\n  SKIPPED (no buy box or no our price):")
        for r in no_bb + no_our:
            reason = "no_buybox" if r.get("buy_box_price") is None else "no_our_price"
            print(f"    {r['asin']} ({r['sku'][:30]}) — {reason}")

    print(f"\n  Saved {len(results)} records to amazon_pricing table.")
    print(f"{'='*72}\n")


# ── Main ──────────────────────────────────────────────────────────────

def run(marketplace_code: str = "DE", limit: Optional[int] = None) -> list[dict]:
    """Main entry point: analyze pricing for our ASINs on given marketplace."""
    marketplace_id = MARKETPLACE_IDS.get(marketplace_code.upper())
    if not marketplace_id:
        raise ValueError(f"Unknown marketplace: {marketplace_code}. Valid: {list(MARKETPLACE_IDS)}")

    print(f"\n[CompetitorPrices] Starting analysis — Amazon {marketplace_code} ({marketplace_id})")

    # 1. Collect our ASINs
    print(f"  Fetching our ASIN list...")
    asin_list = get_our_asins(limit=limit)
    print(f"  Found {len(asin_list)} ASINs to analyze")

    if not asin_list:
        print("  No ASINs found — make sure inventory/products/orders are synced first.")
        return []

    # 2. Analyze each ASIN
    results = []
    for idx, entry in enumerate(asin_list):
        asin = entry["asin"]
        sku = entry["sku"]
        print(f"  [{idx+1}/{len(asin_list)}] {asin} (SKU: {sku[:30]})", end="", flush=True)

        try:
            result = analyze_asin(asin, sku, marketplace_id)
            if result:
                results.append(result)
                bb = f"{result['buy_box_price']:.2f}" if result['buy_box_price'] else "N/A"
                our = f"{result['our_price']:.2f}" if result['our_price'] else "N/A"
                flag = " *** FLAGGED ***" if result.get("price_flag") else ""
                print(f" — ours={our} buybox={bb}{flag}")
            else:
                print(" — no data")
        except Exception as e:
            print(f" — ERROR: {e}")

    # 3. Save to DB
    if results:
        saved = save_to_db(results)
        print(f"\n  Saved {saved} records to amazon_pricing")

    # 4. Print report
    print_results(results, marketplace_code)

    return results


def main():
    parser = argparse.ArgumentParser(description="Competitor price monitor for Amazon.")
    parser.add_argument("--marketplace", default="DE",
                        help="Amazon marketplace code: DE, FR, IT, ES, NL, PL, SE, BE (default: DE)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of ASINs to analyze (default: all)")
    parser.add_argument("--all-marketplaces", action="store_true",
                        help="Run for all EU marketplaces")
    args = parser.parse_args()

    if args.all_marketplaces:
        all_results = []
        for code in MARKETPLACE_IDS:
            try:
                r = run(marketplace_code=code, limit=args.limit)
                all_results.extend(r)
                time.sleep(5)
            except Exception as e:
                print(f"  [ERROR] {code}: {e}")
        print(f"\nTotal records across all marketplaces: {len(all_results)}")
    else:
        run(marketplace_code=args.marketplace, limit=args.limit)


if __name__ == "__main__":
    main()
