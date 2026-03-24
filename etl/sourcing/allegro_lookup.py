"""Allegro product lookup by EAN for sourcing analysis.

Uses /sale/products (GTIN mode) for product discovery and category,
then estimates pricing based on available market data.

The /offers/listing endpoint (public search with actual prices) requires
a verified Allegro application. Until our app is verified, we use the
product catalog endpoint and mark pricing as estimated.

Usage:
    from etl.sourcing.allegro_lookup import lookup_ean
    result = lookup_ean("5903111111111")  # AllegroProductData
"""
from dataclasses import dataclass, field
import requests
import time

from ..allegro_fees import _load_allegro_token
from .config import SourcingConfig

# ── Constants ────────────────────────────────────────────────────────

ALLEGRO_API_BASE = "https://api.allegro.pl"

_cfg = SourcingConfig()


# ── Data model ───────────────────────────────────────────────────────

@dataclass
class AllegroProductData:
    """Aggregated product data from Allegro search results for a given EAN."""
    ean: str
    product_id: str = ""
    product_name: str = ""
    offer_count: int = 0
    lowest_price: float | None = None
    highest_price: float | None = None
    avg_price: float | None = None
    smart_offers_count: int = 0
    smart_lowest_price: float | None = None
    currency: str = "PLN"
    category_id: str = ""
    category_name: str = ""
    is_listed: bool = False
    is_estimated: bool = False
    top_offers: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ── HTTP helper ──────────────────────────────────────────────────────

def _allegro_get(path: str, params: dict | None = None,
                 token: str = "") -> dict | None:
    """GET request to Allegro API with retry on 429/5xx and re-auth on 401."""
    url = f"{ALLEGRO_API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.allegro.public.v1+json",
    }

    last_status = 0
    for attempt in range(5):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"    [allegro] ConnectionError, retry in {wait}s ({attempt+1}/5)")
            time.sleep(wait)
            continue
        except requests.exceptions.ReadTimeout:
            wait = 10 * (attempt + 1)
            print(f"    [allegro] ReadTimeout, retry in {wait}s ({attempt+1}/5)")
            time.sleep(wait)
            continue

        last_status = resp.status_code

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [allegro] Rate limited, retry in {wait}s ({attempt+1}/5)")
            time.sleep(wait)
            continue

        if resp.status_code == 401:
            print("    [allegro] 401 Unauthorized, attempting token refresh...")
            try:
                new_token = _load_allegro_token()
                headers["Authorization"] = f"Bearer {new_token}"
                token = new_token
            except Exception as e:
                raise RuntimeError(f"Allegro token refresh failed: {e}")
            time.sleep(1)
            continue

        if resp.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [allegro] Server error {resp.status_code}, retry in {wait}s ({attempt+1}/5)")
            time.sleep(wait)
            continue

        if resp.status_code == 403:
            print(f"    [allegro] 403 on {path}")
            return None

        raise RuntimeError(
            f"Allegro API {path} returned {resp.status_code}: {resp.text[:300]}"
        )

    raise RuntimeError(
        f"Allegro API {path}: all 5 retries exhausted (last status: {last_status})"
    )


# ── Product catalog search (GTIN mode) ──────────────────────────────

def _search_product_catalog(ean: str, token: str) -> dict | None:
    """Search Allegro product catalog by EAN (GTIN mode).

    Returns the best matching product dict or None.
    """
    params = {
        "phrase": ean,
        "mode": "GTIN",
        "language": "pl-PL",
    }

    data = _allegro_get("/sale/products", params=params, token=token)
    if not data:
        return None

    products = data.get("products", [])
    if not products:
        return None

    # Return the first (best-match) product
    return products[0]


# ── Offer listing search (requires verified app) ────────────────────

def _search_offers_listing(ean: str, token: str) -> list[dict]:
    """Search public offers by EAN phrase. Needs verified app (may 403)."""
    params = {
        "phrase": ean,
        "searchMode": "DESCRIPTIONS",
        "sort": "-relevance",
        "limit": "20",
        "fallback": "false",
    }

    try:
        data = _allegro_get("/offers/listing", params=params, token=token)
    except Exception as e:
        print(f"    [allegro] Offers listing failed: {e}")
        return []

    if data is None:
        return []

    offers = []
    for section_key in ("promoted", "regular"):
        section = data.get("items", {}).get(section_key, [])
        for item in section:
            offer = _parse_offer_item(item)
            if offer:
                offers.append(offer)

    return offers


def _parse_offer_item(item: dict) -> dict | None:
    """Parse a single item from Allegro listing search results."""
    try:
        price_obj = item.get("sellingMode", {}).get("price", {})
        price = _safe_float(price_obj.get("amount"))
        if price is None:
            return None

        delivery = item.get("delivery", {})
        delivery_price_obj = delivery.get("lowestPrice", {})

        return {
            "id": str(item.get("id", "")),
            "name": item.get("name", ""),
            "price": price,
            "currency": price_obj.get("currency", "PLN"),
            "delivery_price": _safe_float(delivery_price_obj.get("amount")) or 0.0,
            "is_smart": delivery.get("availableForFree", False),
            "seller_id": str(item.get("seller", {}).get("id", "")),
            "seller_name": item.get("seller", {}).get("login", ""),
            "quantity": item.get("stock", {}).get("available", 0),
        }
    except Exception:
        return None


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ── Category path helper ─────────────────────────────────────────────

def _extract_category(product: dict) -> tuple[str, str]:
    """Extract leaf category_id and category_name from product dict."""
    cat = product.get("category", {})
    cat_id = str(cat.get("id", ""))
    cat_name = ""
    path = cat.get("path", [])
    if path:
        cat_name = path[-1].get("name", "")
    return cat_id, cat_name


# ── Main pipeline ────────────────────────────────────────────────────

def lookup_ean(ean: str) -> AllegroProductData | None:
    """Full lookup pipeline: search EAN on Allegro, aggregate pricing data.

    Strategy:
    1. Try /offers/listing (full pricing data, but may 403)
    2. Fall back to /sale/products (product catalog: confirms existence + category)

    If only catalog data is available, the result is marked is_estimated=True
    with no pricing (downstream profit_calculator handles missing prices).
    """
    result = AllegroProductData(ean=ean)

    # Load token
    try:
        token = _load_allegro_token()
    except Exception as e:
        result.errors.append(f"Token load failed: {e}")
        return result

    # Strategy 1: Try offers/listing (real prices)
    offers = _search_offers_listing(ean, token)
    time.sleep(_cfg.allegro_delay_sec)

    if offers:
        result.offer_count = len(offers)

        all_prices: list[float] = []
        smart_prices: list[float] = []

        for offer in offers:
            price = offer.get("price")
            if price is not None and price > 0:
                all_prices.append(price)
                if offer.get("is_smart"):
                    smart_prices.append(price)

        if all_prices:
            all_prices.sort()
            result.lowest_price = all_prices[0]
            result.highest_price = all_prices[-1]
            result.avg_price = round(sum(all_prices) / len(all_prices), 2)

        result.smart_offers_count = len(smart_prices)
        if smart_prices:
            smart_prices.sort()
            result.smart_lowest_price = smart_prices[0]

        sorted_offers = sorted(offers, key=lambda o: o.get("price", float("inf")))
        result.top_offers = sorted_offers[:5]

        if offers:
            result.currency = offers[0].get("currency", "PLN")

        # Enrich with catalog data for category
        catalog = _search_product_catalog(ean, token)
        if catalog:
            result.product_id = catalog.get("id", "")
            result.product_name = catalog.get("name", "")
            result.category_id, result.category_name = _extract_category(catalog)
            pub = catalog.get("publication", {})
            result.is_listed = pub.get("status") == "LISTED"

        return result

    # Strategy 2: Fall back to product catalog (no prices, but confirms existence)
    catalog = _search_product_catalog(ean, token)
    if not catalog:
        return None  # Product not found on Allegro at all

    result.product_id = catalog.get("id", "")
    result.product_name = catalog.get("name", "")
    result.category_id, result.category_name = _extract_category(catalog)

    pub = catalog.get("publication", {})
    result.is_listed = pub.get("status") == "LISTED"
    result.is_estimated = True

    if result.is_listed:
        result.errors.append("Prices unavailable (app not verified for /offers/listing)")

    return result
