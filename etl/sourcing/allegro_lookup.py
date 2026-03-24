"""Allegro product lookup by EAN for sourcing analysis.

Searches Allegro.pl marketplace by EAN phrase, extracts pricing data,
identifies Smart! (fulfilled by Allegro) offers, and calculates price stats.

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
    offer_count: int = 0
    lowest_price: float | None = None
    highest_price: float | None = None
    avg_price: float | None = None
    smart_offers_count: int = 0
    smart_lowest_price: float | None = None
    currency: str = "PLN"
    category_id: str = ""
    category_name: str = ""
    top_offers: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ── HTTP helper ──────────────────────────────────────────────────────

def _allegro_get(path: str, params: dict | None = None,
                 token: str = "") -> dict:
    """GET request to Allegro API with retry on 429/5xx and re-auth on 401.

    Args:
        path: API path (e.g. "/offers/listing").
        params: Query parameters.
        token: Bearer token for Authorization header.

    Returns:
        Parsed JSON response as dict.

    Raises:
        RuntimeError: If all retries exhausted or token is invalid.
    """
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
            # Token expired mid-session; try to reload
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

        # 4xx other than 401/429: don't retry
        raise RuntimeError(
            f"Allegro API {path} returned {resp.status_code}: {resp.text[:300]}"
        )

    raise RuntimeError(
        f"Allegro API {path}: all 5 retries exhausted (last status: {last_status})"
    )


# ── Search ───────────────────────────────────────────────────────────

def search_by_ean(ean: str, token: str) -> list[dict]:
    """Search Allegro offers listing by EAN phrase.

    Uses the public offers/listing endpoint which searches by phrase.
    EAN codes are indexed in product descriptions and parameters,
    so a phrase search usually finds exact matches.

    Returns list of parsed offer dicts with keys:
        id, name, price, currency, delivery_price, is_smart,
        seller_id, seller_name, quantity, condition.
    """
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
        print(f"    [allegro] Search failed for EAN {ean}: {e}")
        return []

    time.sleep(_cfg.allegro_delay_sec)

    offers = []

    # Items in "promoted" and "regular" sections
    for section_key in ("promoted", "regular"):
        section = data.get("items", {}).get(section_key, [])
        for item in section:
            offer = _parse_offer_item(item)
            if offer:
                offers.append(offer)

    return offers


def _parse_offer_item(item: dict) -> dict | None:
    """Parse a single item from Allegro listing search results.

    Returns a normalized offer dict or None if parsing fails.
    """
    try:
        offer_id = item.get("id", "")
        name = item.get("name", "")

        # Price
        selling_mode = item.get("sellingMode", {})
        price_obj = selling_mode.get("price", {})
        price = _safe_float(price_obj.get("amount"))
        currency = price_obj.get("currency", "PLN")

        if price is None:
            return None

        # Delivery
        delivery = item.get("delivery", {})
        delivery_price_obj = delivery.get("lowestPrice", {})
        delivery_price = _safe_float(delivery_price_obj.get("amount"))
        is_smart = delivery.get("availableForFree", False)

        # Seller
        seller = item.get("seller", {})
        seller_id = seller.get("id", "")
        seller_name = seller.get("login", "")

        # Stock / condition
        stock = item.get("stock", {})
        quantity = stock.get("available", 0)

        return {
            "id": str(offer_id),
            "name": name,
            "price": price,
            "currency": currency,
            "delivery_price": delivery_price or 0.0,
            "is_smart": is_smart,
            "seller_id": str(seller_id),
            "seller_name": seller_name,
            "quantity": quantity,
        }
    except Exception:
        return None


def _safe_float(value) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ── Main pipeline ────────────────────────────────────────────────────

def lookup_ean(ean: str) -> AllegroProductData:
    """Full lookup pipeline: search EAN on Allegro, aggregate pricing data.

    Args:
        ean: European Article Number (barcode).

    Returns:
        AllegroProductData with pricing stats, Smart! offer info, and top 5 offers.
    """
    result = AllegroProductData(ean=ean)

    # Step 1: Load token
    try:
        token = _load_allegro_token()
    except Exception as e:
        result.errors.append(f"Token load failed: {e}")
        return result

    # Step 2: Search by EAN
    try:
        offers = search_by_ean(ean, token)
    except Exception as e:
        result.errors.append(f"Search failed: {e}")
        return result

    if not offers:
        return result

    result.offer_count = len(offers)

    # Step 3: Extract prices and identify Smart! offers
    all_prices: list[float] = []
    smart_prices: list[float] = []

    for offer in offers:
        price = offer.get("price")
        if price is not None and price > 0:
            all_prices.append(price)
            if offer.get("is_smart"):
                smart_prices.append(price)

    # Price statistics
    if all_prices:
        all_prices.sort()
        result.lowest_price = all_prices[0]
        result.highest_price = all_prices[-1]
        result.avg_price = round(sum(all_prices) / len(all_prices), 2)

    # Smart! statistics
    result.smart_offers_count = len(smart_prices)
    if smart_prices:
        smart_prices.sort()
        result.smart_lowest_price = smart_prices[0]

    # Step 4: Top 5 offers (sorted by price ascending)
    sorted_offers = sorted(offers, key=lambda o: o.get("price", float("inf")))
    result.top_offers = sorted_offers[:5]

    # Step 5: Extract category from first offer if available
    # Note: The listing endpoint doesn't return category directly.
    # We can get it from the search result metadata.
    # For now, category enrichment would require a separate API call.

    # Currency from first offer
    if offers:
        result.currency = offers[0].get("currency", "PLN")

    return result
