"""Amazon SP-API product lookup by EAN for sourcing analysis.

Searches across all 8 EU marketplaces, retrieves competitive offers,
and estimates referral + FBA fees for margin calculation.

Usage:
    from etl.sourcing.amazon_lookup import lookup_ean
    results = lookup_ean("5903111111111")  # dict[market_code, AmazonProductData]
"""
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from ..amazon_api import api_get, api_post
from .. import config
from .config import SourcingConfig

# ── Marketplace mappings ─────────────────────────────────────────────

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

MARKETPLACE_CURRENCIES = {
    "DE": "EUR", "FR": "EUR", "IT": "EUR", "ES": "EUR",
    "NL": "EUR", "BE": "EUR", "PL": "PLN", "SE": "SEK",
}

_cfg = SourcingConfig()


# ── Data model ───────────────────────────────────────────────────────

@dataclass
class AmazonProductData:
    """Aggregated product data for a single ASIN on a single marketplace."""
    asin: str
    marketplace: str           # DE, FR, etc.
    title: str = ""
    category: str = ""
    bsr_rank: int | None = None
    buy_box_price: float | None = None
    buy_box_is_fba: bool | None = None
    lowest_fba_price: float | None = None
    lowest_fbm_price: float | None = None
    num_fba_sellers: int = 0
    num_fbm_sellers: int = 0
    num_total_offers: int = 0
    currency: str = "EUR"
    referral_fee: float = 0.0
    fba_fee: float = 0.0
    total_fee: float = 0.0
    image_url: str = ""
    errors: list[str] = field(default_factory=list)


# ── Catalog search ───────────────────────────────────────────────────

def search_by_ean(ean: str, marketplace: str = "DE") -> list[str]:
    """Search Amazon catalog by EAN and return list of matching ASINs.

    Uses Catalog Items API 2022-04-01 with EAN identifier lookup.
    Rate limit: ~2 req/sec, we use 0.6s delay after the call.
    """
    marketplace_id = MARKETPLACE_IDS.get(marketplace)
    if not marketplace_id:
        print(f"    [WARN] Unknown marketplace: {marketplace}")
        return []

    params = {
        "identifiers": ean,
        "identifiersType": "EAN",
        "marketplaceIds": marketplace_id,
        "includedData": "identifiers,salesRanks,summaries,dimensions,images",
    }

    try:
        data = api_get("/catalog/2022-04-01/items", params=params)
    except Exception as e:
        print(f"    [ERROR] Catalog search failed for EAN {ean} on {marketplace}: {e}")
        return []

    time.sleep(0.6)

    if not data or "items" not in data:
        return []

    asins = []
    for item in data.get("items", []):
        asin = item.get("asin", "")
        if asin:
            asins.append(asin)
    return asins


def _extract_catalog_info(ean: str, marketplace: str = "DE") -> dict:
    """Search by EAN and extract catalog metadata (title, category, BSR, image).

    Returns dict with keys: asin, title, category, bsr_rank, image_url.
    Returns empty dict if nothing found.
    """
    marketplace_id = MARKETPLACE_IDS.get(marketplace)
    if not marketplace_id:
        return {}

    params = {
        "identifiers": ean,
        "identifiersType": "EAN",
        "marketplaceIds": marketplace_id,
        "includedData": "identifiers,salesRanks,summaries,dimensions,images",
    }

    try:
        data = api_get("/catalog/2022-04-01/items", params=params)
    except Exception as e:
        print(f"    [ERROR] Catalog search for EAN {ean} on {marketplace}: {e}")
        return {}

    time.sleep(0.6)

    items = data.get("items", []) if data else []
    if not items:
        return {}

    item = items[0]
    asin = item.get("asin", "")

    # Extract title and category from summaries
    title = ""
    category = ""
    for summary in item.get("summaries", []):
        if summary.get("marketplaceId") == marketplace_id:
            title = summary.get("itemName", "")
            category = summary.get("browseClassification", {}).get("displayName", "")
            break
    # Fallback: use first summary if marketplace-specific not found
    if not title and item.get("summaries"):
        first = item["summaries"][0]
        title = first.get("itemName", "")
        category = first.get("browseClassification", {}).get("displayName", "")

    # Extract BSR from salesRanks
    bsr_rank = None
    for rank_set in item.get("salesRanks", []):
        if rank_set.get("marketplaceId") == marketplace_id:
            for rank in rank_set.get("ranks", []):
                if rank.get("link", "").count("/") <= 4:
                    # Top-level category rank (fewer path segments)
                    bsr_rank = rank.get("rank")
                    break
            if bsr_rank is None and rank_set.get("ranks"):
                bsr_rank = rank_set["ranks"][0].get("rank")
            break

    # Extract main image
    image_url = ""
    for img_set in item.get("images", []):
        if img_set.get("marketplaceId") == marketplace_id:
            images = img_set.get("images", [])
            for img in images:
                if img.get("variant", "") == "MAIN":
                    image_url = img.get("link", "")
                    break
            if not image_url and images:
                image_url = images[0].get("link", "")
            break

    return {
        "asin": asin,
        "title": title,
        "category": category,
        "bsr_rank": bsr_rank,
        "image_url": image_url,
    }


# ── Competitive offers ───────────────────────────────────────────────

def get_item_offers(asin: str, marketplace: str = "DE") -> dict:
    """Fetch competitive offers for an ASIN on a given marketplace.

    Uses Product Pricing API v0 getItemOffers endpoint.
    Rate limit: ~1 req/sec, we use 1.2s delay after the call.

    Returns dict with keys:
        buy_box_price, buy_box_is_fba, lowest_fba_price, lowest_fbm_price,
        num_fba_sellers, num_fbm_sellers, num_total_offers, currency
    """
    marketplace_id = MARKETPLACE_IDS.get(marketplace)
    if not marketplace_id:
        return {"error": f"Unknown marketplace: {marketplace}"}

    params = {
        "MarketplaceId": marketplace_id,
        "ItemCondition": "New",
    }

    try:
        data = api_get(f"/products/pricing/v0/items/{asin}/offers", params=params)
    except Exception as e:
        return {"error": str(e)}

    time.sleep(1.2)

    if not data:
        return {"error": "Empty response from offers API"}

    result = {
        "buy_box_price": None,
        "buy_box_is_fba": None,
        "lowest_fba_price": None,
        "lowest_fbm_price": None,
        "num_fba_sellers": 0,
        "num_fbm_sellers": 0,
        "num_total_offers": 0,
        "currency": MARKETPLACE_CURRENCIES.get(marketplace, "EUR"),
    }

    # The response wraps data in payload.Offers or similar structure
    payload = data.get("payload", data)

    # Summary from numberOfOffers
    summary = payload.get("Summary", {})
    for offer_count in summary.get("NumberOfOffers", []):
        condition = offer_count.get("condition", "")
        channel = offer_count.get("fulfillmentChannel", "")
        count = offer_count.get("OfferCount", 0)
        if condition == "New":
            if channel == "Amazon":
                result["num_fba_sellers"] = count
            elif channel == "Merchant":
                result["num_fbm_sellers"] = count

    result["num_total_offers"] = result["num_fba_sellers"] + result["num_fbm_sellers"]

    # Lowest prices from Summary
    for lowest in summary.get("LowestPrices", []):
        condition = lowest.get("condition", "")
        channel = lowest.get("fulfillmentChannel", "")
        if condition != "New":
            continue
        landed = lowest.get("LandedPrice", {})
        price = _parse_amount(landed)
        if price is not None:
            if channel == "Amazon":
                result["lowest_fba_price"] = price
            elif channel == "Merchant":
                result["lowest_fbm_price"] = price

    # Buy box from BuyBoxPrices
    for bb in summary.get("BuyBoxPrices", []):
        if bb.get("condition") == "New":
            landed = bb.get("LandedPrice", {})
            result["buy_box_price"] = _parse_amount(landed)
            # Determine if buy box winner is FBA
            # BuyBoxPrices doesn't directly say, but we can infer from Offers list
            break

    # Parse individual offers for buy box winner details
    offers = payload.get("Offers", [])
    for offer in offers:
        if offer.get("IsBuyBoxWinner"):
            channel = offer.get("IsFulfilledByAmazon", False)
            result["buy_box_is_fba"] = channel
            # Also grab buy box price from the winner if not set
            if result["buy_box_price"] is None:
                landed = offer.get("ListingPrice", {})
                shipping = offer.get("Shipping", {})
                lp = _parse_amount(landed)
                sp = _parse_amount(shipping)
                if lp is not None:
                    result["buy_box_price"] = lp + (sp or 0.0)
            break

    return result


def _parse_amount(price_obj: dict) -> float | None:
    """Safely extract Amount as float from a price object."""
    if not price_obj:
        return None
    try:
        return float(price_obj.get("Amount", 0))
    except (TypeError, ValueError):
        return None


# ── Fee estimation ───────────────────────────────────────────────────

def estimate_fees(asin: str, price: float, marketplace: str = "DE",
                  is_fba: bool = True) -> dict:
    """Estimate referral + FBA fees for an ASIN at a given price.

    Uses My Fees Estimate API (Product Fees v0).
    Returns dict with keys: referral_fee, fba_fee, total_fee, currency.
    """
    marketplace_id = MARKETPLACE_IDS.get(marketplace)
    if not marketplace_id:
        return {"error": f"Unknown marketplace: {marketplace}"}

    currency = MARKETPLACE_CURRENCIES.get(marketplace, "EUR")

    body = {
        "FeesEstimateRequest": {
            "MarketplaceId": marketplace_id,
            "Identifier": "sourcing-lookup",
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": currency,
                    "Amount": price,
                },
            },
            "IsAmazonFulfilled": is_fba,
        }
    }

    try:
        data = api_post(f"/products/fees/v0/items/{asin}/feesEstimate", body=body)
    except Exception as e:
        return {"error": str(e)}

    time.sleep(0.5)

    result = {
        "referral_fee": 0.0,
        "fba_fee": 0.0,
        "total_fee": 0.0,
        "currency": currency,
    }

    if not data:
        result["error"] = "Empty response from fees API"
        return result

    payload = data.get("payload", data)
    estimate = payload.get("FeesEstimateResult", {}).get("FeesEstimate", {})

    if not estimate:
        # Check for error in response
        err = payload.get("FeesEstimateResult", {}).get("Error", {})
        if err:
            result["error"] = err.get("Message", str(err))
        return result

    total_obj = estimate.get("TotalFeesEstimate", {})
    result["total_fee"] = _parse_amount(total_obj) or 0.0

    for fee_detail in estimate.get("FeeDetailList", []):
        fee_type = fee_detail.get("FeeType", "")
        fee_amount = _parse_amount(fee_detail.get("FinalFee", {})) or 0.0

        if fee_type == "ReferralFee":
            result["referral_fee"] = fee_amount
        elif fee_type in ("FBAFees", "FBAPerUnitFulfillmentFee"):
            result["fba_fee"] += fee_amount

    return result


# ── Main pipeline ────────────────────────────────────────────────────

def _find_asin_for_ean(ean: str) -> tuple[str, str, dict]:
    """Try to find an ASIN for the given EAN, searching DE first, then FR, IT.

    Returns (asin, found_marketplace, catalog_info) or ("", "", {}).
    """
    for market in ["DE", "FR", "IT"]:
        info = _extract_catalog_info(ean, marketplace=market)
        if info and info.get("asin"):
            print(f"    Found ASIN {info['asin']} for EAN {ean} on {market}")
            return info["asin"], market, info
    return "", "", {}


def _lookup_single_market(
    asin: str,
    market: str,
    catalog_info: dict,
    delay_sec: float,
) -> tuple[str, AmazonProductData]:
    """Fetch offers + fees for one ASIN on one marketplace. Thread-safe."""
    product = AmazonProductData(
        asin=asin,
        marketplace=market,
        currency=MARKETPLACE_CURRENCIES.get(market, "EUR"),
    )

    if catalog_info:
        product.title = catalog_info.get("title", "")
        product.category = catalog_info.get("category", "")
        product.bsr_rank = catalog_info.get("bsr_rank")
        product.image_url = catalog_info.get("image_url", "")

    # Get competitive offers
    try:
        offers = get_item_offers(asin, marketplace=market)
        if "error" in offers:
            product.errors.append(f"offers: {offers['error']}")
        else:
            product.buy_box_price = offers.get("buy_box_price")
            product.buy_box_is_fba = offers.get("buy_box_is_fba")
            product.lowest_fba_price = offers.get("lowest_fba_price")
            product.lowest_fbm_price = offers.get("lowest_fbm_price")
            product.num_fba_sellers = offers.get("num_fba_sellers", 0)
            product.num_fbm_sellers = offers.get("num_fbm_sellers", 0)
            product.num_total_offers = offers.get("num_total_offers", 0)
    except Exception as e:
        product.errors.append(f"offers: {e}")

    time.sleep(delay_sec)

    # Estimate fees using buy box price (or lowest FBA, or lowest FBM)
    sell_price = (
        product.buy_box_price
        or product.lowest_fba_price
        or product.lowest_fbm_price
    )

    if sell_price and sell_price > 0:
        try:
            fees = estimate_fees(asin, sell_price, marketplace=market, is_fba=True)
            if "error" in fees:
                product.errors.append(f"fees: {fees['error']}")
            else:
                product.referral_fee = fees.get("referral_fee", 0.0)
                product.fba_fee = fees.get("fba_fee", 0.0)
                product.total_fee = fees.get("total_fee", 0.0)
        except Exception as e:
            product.errors.append(f"fees: {e}")
    else:
        product.errors.append("no sell price available for fee estimation")

    return market, product


def lookup_ean(ean: str,
               markets: list[str] | None = None,
               delay_sec: float | None = None,
               max_workers: int = 4) -> dict[str, AmazonProductData]:
    """Full lookup pipeline: search EAN across EU marketplaces, get offers + fees.

    Uses a thread pool to query multiple marketplaces in parallel.

    Args:
        ean: European Article Number (barcode).
        markets: List of marketplace codes to check. Defaults to all 8 EU markets.
        delay_sec: Delay between API call groups. Defaults to SourcingConfig.amazon_delay_sec.
        max_workers: Max parallel threads for marketplace lookups.

    Returns:
        dict mapping marketplace code (e.g. "DE") to AmazonProductData.
        Empty dict if EAN not found on Amazon at all.
    """
    if markets is None:
        markets = list(MARKETPLACE_IDS.keys())
    if delay_sec is None:
        delay_sec = _cfg.amazon_delay_sec

    results: dict[str, AmazonProductData] = {}

    # Step 1: Find ASIN via catalog search (DE -> FR -> IT)
    asin, found_market, catalog_info = _find_asin_for_ean(ean)
    if not asin:
        print(f"    EAN {ean} not found on Amazon (searched DE, FR, IT)")
        return results

    # Step 2: Parallel lookup across all target marketplaces
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _lookup_single_market, asin, market, catalog_info, delay_sec
            ): market
            for market in markets
        }

        for future in as_completed(futures):
            market = futures[future]
            try:
                mkt, product = future.result()
                results[mkt] = product
            except Exception as e:
                results[market] = AmazonProductData(
                    asin=asin,
                    marketplace=market,
                    errors=[str(e)],
                )

    return results
