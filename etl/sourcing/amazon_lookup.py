"""Amazon SP-API product lookup by EAN for sourcing analysis.

Searches across all 8 EU marketplaces, retrieves competitive offers,
estimates fees, and extracts rich product data (brand, weight, dimensions,
MSRP, BSR per market, variation info, seller details).

Data extracted is comparable to Keepa for current-state analysis.
Historical tracking (price/BSR trends) requires daily polling.

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

# Reverse: marketplace_id -> country code
_ID_TO_MARKET = {v: k for k, v in MARKETPLACE_IDS.items()}

_cfg = SourcingConfig()


# ── Data model ───────────────────────────────────────────────────────

@dataclass
class AmazonProductData:
    """Rich product data for a single ASIN on a single marketplace."""
    asin: str
    marketplace: str           # DE, FR, etc.
    title: str = ""
    category: str = ""
    category_path: str = ""    # full breadcrumb
    bsr_rank: int | None = None
    bsr_subcategory_rank: int | None = None
    bsr_subcategory_name: str = ""
    buy_box_price: float | None = None
    buy_box_is_fba: bool | None = None
    lowest_fba_price: float | None = None
    lowest_fbm_price: float | None = None
    msrp: float | None = None
    num_fba_sellers: int = 0
    num_fbm_sellers: int = 0
    num_total_offers: int = 0
    currency: str = "EUR"
    referral_fee: float = 0.0
    fba_fee: float = 0.0
    total_fee: float = 0.0
    image_url: str = ""
    # Rich product attributes
    brand: str = ""
    manufacturer: str = ""
    item_weight_kg: float | None = None
    package_weight_kg: float | None = None
    item_length_cm: float | None = None
    item_width_cm: float | None = None
    item_height_cm: float | None = None
    package_length_cm: float | None = None
    package_width_cm: float | None = None
    package_height_cm: float | None = None
    launch_date: str = ""
    variation_theme: str = ""
    variation_count: int = 0
    parent_asin: str = ""
    is_prime: bool = False
    buy_box_seller_feedback_count: int = 0
    buy_box_seller_rating: float = 0.0
    buy_box_ships_from: str = ""
    top_sellers: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ── Helpers ──────────────────────────────────────────────────────────

def _parse_amount(price_obj: dict) -> float | None:
    """Safely extract Amount as float from a price object."""
    if not price_obj:
        return None
    try:
        return float(price_obj.get("Amount", 0))
    except (TypeError, ValueError):
        return None


def _attr_value(attrs: dict, key: str) -> str:
    """Extract first value from an attributes dict entry."""
    entries = attrs.get(key, [])
    if not entries:
        return ""
    entry = entries[0]
    if isinstance(entry, dict):
        return str(entry.get("value", ""))
    return str(entry)


def _attr_dimension_cm(attrs: dict, key: str) -> float | None:
    """Extract a dimension in cm from attributes (may be in mm, inches, etc.)."""
    entries = attrs.get(key, [])
    if not entries:
        return None
    entry = entries[0]
    if not isinstance(entry, dict):
        return None

    # Could be nested: {value: {value: 10, unit: "centimeters"}}
    val = entry.get("value", entry)
    if isinstance(val, dict):
        num = val.get("value")
        unit = val.get("unit", "")
    else:
        num = val
        unit = entry.get("unit", "")

    if num is None:
        return None
    try:
        num = float(num)
    except (TypeError, ValueError):
        return None

    # Convert to cm
    unit_lower = str(unit).lower()
    if "milli" in unit_lower or unit_lower == "mm":
        return round(num / 10, 2)
    if "inch" in unit_lower or unit_lower == "in":
        return round(num * 2.54, 2)
    if "meter" in unit_lower and "centi" not in unit_lower:
        return round(num * 100, 2)
    # Assume cm
    return round(num, 2)


def _attr_weight_kg(attrs: dict, key: str) -> float | None:
    """Extract weight in kg from attributes."""
    entries = attrs.get(key, [])
    if not entries:
        return None
    entry = entries[0]
    if not isinstance(entry, dict):
        return None

    val = entry.get("value", entry)
    if isinstance(val, dict):
        num = val.get("value")
        unit = val.get("unit", "")
    else:
        num = val
        unit = entry.get("unit", "")

    if num is None:
        return None
    try:
        num = float(num)
    except (TypeError, ValueError):
        return None

    unit_lower = str(unit).lower()
    if "gram" in unit_lower and "kilo" not in unit_lower:
        return round(num / 1000, 4)
    if "pound" in unit_lower or unit_lower == "lb":
        return round(num * 0.4536, 4)
    if "ounce" in unit_lower or unit_lower == "oz":
        return round(num * 0.02835, 4)
    # Assume kg
    return round(num, 4)


# ── Catalog search (enriched) ───────────────────────────────────────

def _extract_catalog_info(ean: str, markets: list[str] | None = None) -> dict:
    """Search by EAN and extract rich catalog metadata.

    Uses a single API call with multiple marketplace IDs to get
    cross-market BSR data efficiently.

    Returns dict with keys: asin, title, category, category_path,
    bsr_per_market, image_url, brand, manufacturer, weight, dimensions,
    launch_date, variation_theme, parent_asin, variation_count.
    """
    if markets is None:
        markets = ["DE", "FR", "IT", "ES"]

    marketplace_ids = [MARKETPLACE_IDS[m] for m in markets if m in MARKETPLACE_IDS]
    if not marketplace_ids:
        return {}

    params = {
        "identifiers": ean,
        "identifiersType": "EAN",
        "marketplaceIds": ",".join(marketplace_ids),
        "includedData": (
            "attributes,classifications,dimensions,identifiers,"
            "images,relationships,salesRanks,summaries"
        ),
    }

    try:
        data = api_get("/catalog/2022-04-01/items", params=params)
    except Exception as e:
        print(f"    [ERROR] Catalog search for EAN {ean}: {e}")
        return {}

    time.sleep(0.6)

    items = data.get("items", []) if data else []
    if not items:
        return {}

    item = items[0]
    asin = item.get("asin", "")
    result = {"asin": asin}

    # --- Title and category from summaries ---
    title = ""
    category = ""
    primary_mid = marketplace_ids[0]  # prefer DE

    for summary in item.get("summaries", []):
        if summary.get("marketplaceId") == primary_mid:
            title = summary.get("itemName", "")
            category = summary.get("browseClassification", {}).get("displayName", "")
            break
    if not title and item.get("summaries"):
        first = item["summaries"][0]
        title = first.get("itemName", "")
        category = first.get("browseClassification", {}).get("displayName", "")

    result["title"] = title
    result["category"] = category

    # --- Full category path from classifications ---
    category_path = ""
    for cls in item.get("classifications", []):
        if cls.get("marketplaceId") == primary_mid:
            chain = cls.get("classifications", [])
            if chain:
                # Build path from leaf to root
                names = [c.get("displayName", "") for c in chain if c.get("displayName")]
                if names:
                    category_path = " > ".join(names)
            break
    result["category_path"] = category_path

    # --- BSR per marketplace ---
    bsr_per_market: dict[str, dict] = {}
    for rank_set in item.get("salesRanks", []):
        mid = rank_set.get("marketplaceId", "")
        market_code = _ID_TO_MARKET.get(mid, "")
        if not market_code:
            continue

        ranks = rank_set.get("ranks", [])
        main_rank = None
        sub_rank = None
        sub_name = ""

        for rank in ranks:
            r = rank.get("rank")
            link = rank.get("link", "")
            title_r = rank.get("title", "")
            # Fewer path segments = more top-level category
            if link.count("/") <= 4:
                if main_rank is None or r < main_rank:
                    main_rank = r
            else:
                if sub_rank is None or r < sub_rank:
                    sub_rank = r
                    sub_name = title_r

        if main_rank is None and ranks:
            main_rank = ranks[0].get("rank")

        bsr_per_market[market_code] = {
            "main": main_rank,
            "sub": sub_rank,
            "sub_name": sub_name,
        }

    result["bsr_per_market"] = bsr_per_market

    # --- Main image ---
    image_url = ""
    for img_set in item.get("images", []):
        if img_set.get("marketplaceId") == primary_mid:
            images = img_set.get("images", [])
            for img in images:
                if img.get("variant", "") == "MAIN":
                    image_url = img.get("link", "")
                    break
            if not image_url and images:
                image_url = images[0].get("link", "")
            break
    result["image_url"] = image_url

    # --- Product attributes ---
    attrs = item.get("attributes", {})
    result["brand"] = _attr_value(attrs, "brand")
    result["manufacturer"] = _attr_value(attrs, "manufacturer")
    result["launch_date"] = _attr_value(attrs, "product_site_launch_date")
    result["variation_theme"] = _attr_value(attrs, "variation_theme")

    # MSRP / list price
    msrp_str = _attr_value(attrs, "list_price") or _attr_value(attrs, "uvp_list_price")
    if msrp_str:
        # Could be nested dict or formatted string
        if isinstance(msrp_str, str):
            try:
                result["msrp"] = float(msrp_str)
            except ValueError:
                result["msrp"] = None
        else:
            result["msrp"] = None
    else:
        result["msrp"] = None

    # Weight
    result["item_weight_kg"] = _attr_weight_kg(attrs, "item_weight")
    result["package_weight_kg"] = _attr_weight_kg(attrs, "item_package_weight")

    # Dimensions (item)
    result["item_length_cm"] = _attr_dimension_cm(attrs, "item_dimensions_length")
    result["item_width_cm"] = _attr_dimension_cm(attrs, "item_dimensions_width")
    result["item_height_cm"] = _attr_dimension_cm(attrs, "item_dimensions_height")

    # Dimensions (package)
    result["package_length_cm"] = _attr_dimension_cm(attrs, "item_package_dimensions_length")
    result["package_width_cm"] = _attr_dimension_cm(attrs, "item_package_dimensions_width")
    result["package_height_cm"] = _attr_dimension_cm(attrs, "item_package_dimensions_height")

    # Fallback: dimensions from the structured 'dimensions' section
    for dim_set in item.get("dimensions", []):
        if dim_set.get("marketplaceId") == primary_mid:
            di = dim_set.get("item", {})
            dp = dim_set.get("package", {})

            if result["item_weight_kg"] is None and "weight" in di:
                w = di["weight"]
                try:
                    val = float(w.get("value", 0))
                    unit = w.get("unit", "pounds")
                    if "pound" in unit.lower():
                        result["item_weight_kg"] = round(val * 0.4536, 4)
                    elif "gram" in unit.lower() and "kilo" not in unit.lower():
                        result["item_weight_kg"] = round(val / 1000, 4)
                    else:
                        result["item_weight_kg"] = round(val, 4)
                except (TypeError, ValueError):
                    pass

            if result["package_weight_kg"] is None and "weight" in dp:
                w = dp["weight"]
                try:
                    val = float(w.get("value", 0))
                    unit = w.get("unit", "pounds")
                    if "pound" in unit.lower():
                        result["package_weight_kg"] = round(val * 0.4536, 4)
                    elif "gram" in unit.lower() and "kilo" not in unit.lower():
                        result["package_weight_kg"] = round(val / 1000, 4)
                    else:
                        result["package_weight_kg"] = round(val, 4)
                except (TypeError, ValueError):
                    pass
            break

    # --- Relationships (variation parent/child) ---
    parent_asin = ""
    variation_count = 0
    for rel_set in item.get("relationships", []):
        if rel_set.get("marketplaceId") == primary_mid:
            rels = rel_set.get("relationships", [])
            for rel in rels:
                if rel.get("type") == "VARIATION" and "parentAsins" in rel:
                    parents = rel["parentAsins"]
                    if parents:
                        parent_asin = parents[0]
                if rel.get("type") == "VARIATION" and "childAsins" in rel:
                    variation_count = len(rel["childAsins"])
            break
    result["parent_asin"] = parent_asin
    result["variation_count"] = variation_count

    return result


# ── Competitive offers (enriched) ───────────────────────────────────

def get_item_offers(asin: str, marketplace: str = "DE") -> dict:
    """Fetch competitive offers for an ASIN on a given marketplace.

    Extracts: buy box price, seller counts, Prime status, seller feedback,
    ships-from country, MSRP, top 5 seller details.
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
        "msrp": None,
        "is_prime": False,
        "buy_box_seller_feedback_count": 0,
        "buy_box_seller_rating": 0.0,
        "buy_box_ships_from": "",
        "top_sellers": [],
    }

    payload = data.get("payload", data)
    summary = payload.get("Summary", {})

    # MSRP from summary
    list_price = summary.get("ListPrice", {})
    result["msrp"] = _parse_amount(list_price)

    # Total offer count
    result["num_total_offers"] = summary.get("TotalOfferCount", 0)

    # Seller counts by channel
    for offer_count in summary.get("NumberOfOffers", []):
        condition = offer_count.get("condition", "")
        channel = offer_count.get("fulfillmentChannel", "")
        count = offer_count.get("OfferCount", 0)
        if condition == "New":
            if channel == "Amazon":
                result["num_fba_sellers"] = count
            elif channel == "Merchant":
                result["num_fbm_sellers"] = count

    if result["num_total_offers"] == 0:
        result["num_total_offers"] = result["num_fba_sellers"] + result["num_fbm_sellers"]

    # Lowest prices
    for lowest in summary.get("LowestPrices", []):
        if lowest.get("condition") != "New":
            continue
        channel = lowest.get("fulfillmentChannel", "")
        landed = lowest.get("LandedPrice", {})
        price = _parse_amount(landed)
        if price is not None:
            if channel == "Amazon":
                result["lowest_fba_price"] = price
            elif channel == "Merchant":
                result["lowest_fbm_price"] = price

    # Buy box price
    for bb in summary.get("BuyBoxPrices", []):
        if bb.get("condition") == "New":
            result["buy_box_price"] = _parse_amount(bb.get("LandedPrice", {}))
            break

    # Individual offers (up to 20)
    offers = payload.get("Offers", [])
    top_sellers = []

    for offer in offers:
        seller_info = {
            "seller_id": offer.get("SellerId", ""),
            "is_fba": offer.get("IsFulfilledByAmazon", False),
            "is_buy_box_winner": offer.get("IsBuyBoxWinner", False),
            "is_featured_merchant": offer.get("IsFeaturedMerchant", False),
        }

        # Price
        lp = _parse_amount(offer.get("ListingPrice", {}))
        sp = _parse_amount(offer.get("Shipping", {}))
        seller_info["price"] = (lp or 0) + (sp or 0)
        seller_info["listing_price"] = lp
        seller_info["shipping_price"] = sp

        # Prime
        prime = offer.get("PrimeInformation", {})
        seller_info["is_prime"] = prime.get("IsPrime", False)

        # Seller feedback
        feedback = offer.get("SellerFeedbackRating", {})
        seller_info["feedback_count"] = feedback.get("FeedbackCount", 0)
        seller_info["feedback_rating"] = feedback.get("SellerPositiveFeedbackRating", 0.0)

        # Ships from
        ships_from = offer.get("ShipsFrom", {})
        seller_info["ships_from"] = ships_from.get("Country", "")

        # Shipping time
        ship_time = offer.get("ShippingTime", {})
        seller_info["min_hours"] = ship_time.get("minimumHours")
        seller_info["max_hours"] = ship_time.get("maximumHours")

        top_sellers.append(seller_info)

        # Extract buy box winner details
        if offer.get("IsBuyBoxWinner"):
            result["buy_box_is_fba"] = offer.get("IsFulfilledByAmazon", False)
            result["is_prime"] = prime.get("IsPrime", False)
            result["buy_box_seller_feedback_count"] = feedback.get("FeedbackCount", 0)
            result["buy_box_seller_rating"] = feedback.get("SellerPositiveFeedbackRating", 0.0)
            result["buy_box_ships_from"] = ships_from.get("Country", "")

            if result["buy_box_price"] is None and lp is not None:
                result["buy_box_price"] = (lp or 0) + (sp or 0)

    result["top_sellers"] = top_sellers[:5]

    return result


# ── Competitive pricing (batch) ─────────────────────────────────────

def get_competitive_pricing(asins: list[str], marketplace: str = "DE") -> dict:
    """Batch competitive pricing for up to 20 ASINs in one call.

    Returns dict {asin: {competitive_price, offer_count, bsr, bsr_subcategory}}.
    """
    marketplace_id = MARKETPLACE_IDS.get(marketplace)
    if not marketplace_id:
        return {}

    params = {
        "MarketplaceId": marketplace_id,
        "Asins": ",".join(asins[:20]),
        "ItemType": "Asin",
    }

    try:
        data = api_get("/products/pricing/v0/competitivePrice", params=params)
    except Exception as e:
        print(f"    [WARN] getCompetitivePricing failed: {e}")
        return {}

    time.sleep(0.5)

    if not data:
        return {}

    results = {}
    payload_list = data.get("payload", data) if isinstance(data, dict) else data
    if not isinstance(payload_list, list):
        payload_list = [payload_list]

    for item in payload_list:
        asin = item.get("ASIN", "")
        if not asin:
            continue

        product = item.get("Product", {})
        comp_prices = product.get("CompetitivePricing", {})

        entry = {
            "competitive_price": None,
            "offer_count_new": 0,
            "bsr_main": None,
            "bsr_sub": None,
            "bsr_sub_name": "",
        }

        # Competitive prices
        for cp in comp_prices.get("CompetitivePrices", []):
            if cp.get("condition") == "New":
                landed = cp.get("Price", {}).get("LandedPrice", {})
                entry["competitive_price"] = _parse_amount(landed)
                break

        # Number of offer listings
        for nol in comp_prices.get("NumberOfOfferListings", []):
            if nol.get("condition") == "New":
                entry["offer_count_new"] = nol.get("Count", 0)

        # Sales rankings
        for sr in product.get("SalesRankings", []):
            cat_id = sr.get("ProductCategoryId", "")
            rank = sr.get("Rank")
            # Main category has shorter ID (no underscore prefix)
            if "_" not in cat_id:
                entry["bsr_main"] = rank
            else:
                if entry["bsr_sub"] is None or rank < entry["bsr_sub"]:
                    entry["bsr_sub"] = rank
                    entry["bsr_sub_name"] = cat_id

        results[asin] = entry

    return results


# ── Fee estimation ───────────────────────────────────────────────────

def estimate_fees(asin: str, price: float, marketplace: str = "DE",
                  is_fba: bool = True) -> dict:
    """Estimate referral + FBA fees for an ASIN at a given price."""
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
    """Find ASIN for EAN, searching DE first, then FR, IT.

    Returns (asin, found_marketplace, rich_catalog_info) or ("", "", {}).
    Now uses enriched catalog call with attributes, classifications, etc.
    """
    # Try DE first with full data across 4 main markets
    info = _extract_catalog_info(ean, markets=["DE", "FR", "IT", "ES"])
    if info and info.get("asin"):
        print(f"    Found ASIN {info['asin']} for EAN {ean} on DE")
        return info["asin"], "DE", info

    # Fallback: try individual markets
    for market in ["FR", "IT"]:
        info = _extract_catalog_info(ean, markets=[market])
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

    # Populate from catalog info
    if catalog_info:
        product.title = catalog_info.get("title", "")
        product.category = catalog_info.get("category", "")
        product.category_path = catalog_info.get("category_path", "")
        product.image_url = catalog_info.get("image_url", "")
        product.brand = catalog_info.get("brand", "")
        product.manufacturer = catalog_info.get("manufacturer", "")
        product.launch_date = catalog_info.get("launch_date", "")
        product.variation_theme = catalog_info.get("variation_theme", "")
        product.variation_count = catalog_info.get("variation_count", 0)
        product.parent_asin = catalog_info.get("parent_asin", "")
        product.item_weight_kg = catalog_info.get("item_weight_kg")
        product.package_weight_kg = catalog_info.get("package_weight_kg")
        product.item_length_cm = catalog_info.get("item_length_cm")
        product.item_width_cm = catalog_info.get("item_width_cm")
        product.item_height_cm = catalog_info.get("item_height_cm")
        product.package_length_cm = catalog_info.get("package_length_cm")
        product.package_width_cm = catalog_info.get("package_width_cm")
        product.package_height_cm = catalog_info.get("package_height_cm")

        # BSR per market from catalog
        bsr_map = catalog_info.get("bsr_per_market", {})
        if market in bsr_map:
            product.bsr_rank = bsr_map[market].get("main")
            product.bsr_subcategory_rank = bsr_map[market].get("sub")
            product.bsr_subcategory_name = bsr_map[market].get("sub_name", "")

    # Get competitive offers (enriched)
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
            product.is_prime = offers.get("is_prime", False)
            product.buy_box_seller_feedback_count = offers.get("buy_box_seller_feedback_count", 0)
            product.buy_box_seller_rating = offers.get("buy_box_seller_rating", 0.0)
            product.buy_box_ships_from = offers.get("buy_box_ships_from", "")
            product.top_sellers = offers.get("top_sellers", [])

            # MSRP from offers (if not in catalog)
            if product.msrp is None:
                product.msrp = offers.get("msrp")
    except Exception as e:
        product.errors.append(f"offers: {e}")

    time.sleep(delay_sec)

    # Estimate fees
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
    """Full lookup: EAN -> ASIN -> offers + fees across EU marketplaces.

    Now extracts rich data: brand, weight, dimensions, MSRP, BSR per market,
    variation info, seller feedback, Prime status, ships-from country.
    """
    if markets is None:
        markets = list(MARKETPLACE_IDS.keys())
    if delay_sec is None:
        delay_sec = _cfg.amazon_delay_sec

    results: dict[str, AmazonProductData] = {}

    # Step 1: Find ASIN with enriched catalog data
    asin, found_market, catalog_info = _find_asin_for_ean(ean)
    if not asin:
        print(f"    EAN {ean} not found on Amazon (searched DE, FR, IT)")
        return results

    # Step 2: Get BSR for remaining markets not covered by initial catalog call
    initial_markets = set(catalog_info.get("bsr_per_market", {}).keys())
    missing_bsr_markets = [m for m in markets if m not in initial_markets]
    if missing_bsr_markets:
        extra_info = _extract_catalog_info(ean, markets=missing_bsr_markets)
        if extra_info and extra_info.get("bsr_per_market"):
            existing = catalog_info.get("bsr_per_market", {})
            existing.update(extra_info["bsr_per_market"])
            catalog_info["bsr_per_market"] = existing

    # Step 3: Parallel lookup across all target marketplaces
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
