"""Analyse per-EAN profitability across Amazon EU markets and Allegro.

For every marketplace where the product is listed the module computes a
full cost breakdown (purchase price, platform fees, shipping) and derives
profit, margin, and ROI.  Results are sorted best-first so the caller
can immediately see the most attractive opportunities.

Usage::

    from etl.sourcing.profit_calculator import analyze_profitability, ProfitAnalysis
    results: list[ProfitAnalysis] = analyze_profitability(
        ean="5904066095280",
        purchase_price=15.50,
        purchase_currency="PLN",
        amazon_data=amz,
        allegro_data=alg,
        config=cfg,
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from ..fx_rates import fetch_nbp_rate
from ..shipping_costs import (
    DPD_COUNTRY_RATES,
    DPD_SECURITY_FEE_EUR,
    DPD_FUEL_SURCHARGE_PCT,
    WEIGHT_BRACKETS,
)
from .config import SourcingConfig
from .fee_calculator import calculate_amazon_fees, calculate_allegro_fees

# ---------------------------------------------------------------------------
# Country-code mapping for Amazon marketplace IDs
# ---------------------------------------------------------------------------

_MARKETPLACE_TO_COUNTRY: dict[str, str] = {
    "DE": "DE",
    "FR": "FR",
    "IT": "IT",
    "ES": "ES",
    "NL": "NL",
    "PL": "PL",
    "SE": "SE",
    "BE": "BE",
}

# Currency used by each Amazon marketplace.
_MARKETPLACE_CURRENCY: dict[str, str] = {
    "DE": "EUR",
    "FR": "EUR",
    "IT": "EUR",
    "ES": "EUR",
    "NL": "EUR",
    "PL": "PLN",
    "SE": "SEK",
    "BE": "EUR",
}

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ProfitAnalysis:
    """Full profitability breakdown for a single EAN on a single platform."""

    ean: str
    platform: str                   # e.g. "amazon_de", "allegro"
    asin: str = ""
    title: str = ""
    sell_price: float = 0.0
    sell_currency: str = "EUR"
    sell_price_pln: float = 0.0
    purchase_price_pln: float = 0.0
    platform_fees_pln: float = 0.0
    shipping_cost_pln: float = 0.0
    total_costs_pln: float = 0.0
    profit_pln: float = 0.0
    margin_pct: float = 0.0
    roi_pct: float = 0.0
    verdict: str = "UNPROFITABLE"
    bsr_rank: int | None = None
    competition: int = 0
    estimated_monthly_sales: int = 0
    image_url: str = ""
    fee_breakdown: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Verdict thresholds
# ---------------------------------------------------------------------------

def _verdict(margin_pct: float, cfg: SourcingConfig) -> str:
    if margin_pct >= cfg.excellent_margin_pct:
        return "EXCELLENT"
    if margin_pct >= cfg.target_margin_pct:
        return "GOOD"
    if margin_pct >= cfg.min_margin_pct:
        return "MARGINAL"
    return "UNPROFITABLE"


# ---------------------------------------------------------------------------
# Shipping helpers
# ---------------------------------------------------------------------------

def _dpd_shipping_cost_eur(country: str, weight_kg: float) -> float:
    """Estimate DPD shipping cost in EUR for a given destination and weight."""
    country = country.upper()
    data = DPD_COUNTRY_RATES.get(country)
    if data is None:
        # Unknown country: use DE as mid-range fallback
        data = DPD_COUNTRY_RATES.get("DE", {"rates": [3.00]})

    rates = data["rates"]
    base = rates[-1]
    for i, bracket_max in enumerate(WEIGHT_BRACKETS):
        if weight_kg <= bracket_max:
            base = rates[i]
            break

    return round((base + DPD_SECURITY_FEE_EUR) * (1 + DPD_FUEL_SURCHARGE_PCT), 2)


def _to_pln(amount: float, currency: str) -> float:
    """Convert *amount* to PLN.  Returns 0.0 on failure."""
    if currency == "PLN":
        return amount
    rate = fetch_nbp_rate(currency, date.today())
    if rate is None:
        # Hardcoded fallbacks for the most common currencies.
        fallbacks = {"EUR": 4.27, "SEK": 0.39, "GBP": 5.30, "USD": 3.95}
        rate = fallbacks.get(currency)
    if rate is None:
        return 0.0
    return round(amount * rate, 2)


def _purchase_in_pln(price: float, currency: str) -> float:
    return _to_pln(price, currency)


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def _analyse_amazon_market(
    ean: str,
    market: str,
    market_data: dict,
    purchase_price_pln: float,
    weight_kg: float,
    cfg: SourcingConfig,
) -> ProfitAnalysis:
    """Build a ProfitAnalysis for a single Amazon marketplace.

    *market_data* is expected to carry at least::

        {
            "asin": str,
            "title": str,
            "buy_box_price": float | None,
            "lowest_fba_price": float | None,
            "referral_fee": float | None,
            "fba_fee": float | None,
            "bsr_rank": int | None,
            "offer_count": int,
            "image_url": str,
        }
    """
    result = ProfitAnalysis(
        ean=ean,
        platform=f"amazon_{market.lower()}",
        asin=market_data.get("asin", ""),
        title=market_data.get("title", ""),
        purchase_price_pln=purchase_price_pln,
    )

    currency = _MARKETPLACE_CURRENCY.get(market, "EUR")
    result.sell_currency = currency

    # Pick the best available sell price.
    sell = market_data.get("buy_box_price")
    if sell is None or sell <= 0:
        sell = market_data.get("lowest_fba_price")
    if sell is None or sell <= 0:
        sell = market_data.get("lowest_price")
    if sell is None or sell <= 0:
        result.errors.append(f"No sell price found for {market}")
        return result

    result.sell_price = sell
    result.sell_price_pln = _to_pln(sell, currency)
    if result.sell_price_pln <= 0:
        result.errors.append(f"FX conversion failed for {currency}")
        return result

    # --- Platform fees ---
    fees = calculate_amazon_fees(
        sell_price=sell,
        currency=currency,
        referral_fee=market_data.get("referral_fee"),
        fba_fee=market_data.get("fba_fee"),
        weight_kg=weight_kg,
        category=market_data.get("category"),
    )
    result.fee_breakdown = fees
    result.platform_fees_pln = fees["total_fee_pln"]

    # --- Shipping ---
    country = _MARKETPLACE_TO_COUNTRY.get(market, market)
    if market_data.get("fulfillment") == "FBA":
        # FBA inbound cost per unit (flat estimate).
        inbound_eur = cfg.fba_inbound_cost_per_unit_eur
        result.shipping_cost_pln = _to_pln(inbound_eur, "EUR")
    else:
        dpd_eur = _dpd_shipping_cost_eur(country, weight_kg)
        result.shipping_cost_pln = _to_pln(dpd_eur, "EUR")

    # --- Totals ---
    result.total_costs_pln = round(
        purchase_price_pln + result.platform_fees_pln + result.shipping_cost_pln, 2
    )
    result.profit_pln = round(result.sell_price_pln - result.total_costs_pln, 2)

    if result.sell_price_pln > 0:
        result.margin_pct = round(result.profit_pln / result.sell_price_pln * 100, 1)
    if purchase_price_pln > 0:
        result.roi_pct = round(result.profit_pln / purchase_price_pln * 100, 1)

    result.verdict = _verdict(result.margin_pct, cfg)

    # Meta
    result.bsr_rank = market_data.get("bsr_rank")
    result.competition = market_data.get("offer_count", 0)
    result.image_url = market_data.get("image_url", "")

    return result


def _analyse_allegro(
    ean: str,
    allegro_data: dict,
    purchase_price_pln: float,
    weight_kg: float,
    cfg: SourcingConfig,
) -> ProfitAnalysis:
    """Build a ProfitAnalysis for Allegro.

    *allegro_data* is expected to carry at least::

        {
            "title": str,
            "lowest_price": float | None,
            "avg_price": float | None,
            "offer_count": int,
            "category_id": str | None,
            "image_url": str,
        }
    """
    result = ProfitAnalysis(
        ean=ean,
        platform="allegro",
        title=allegro_data.get("title", ""),
        sell_currency="PLN",
        purchase_price_pln=purchase_price_pln,
    )

    # Pick the best available sell price.
    sell = allegro_data.get("lowest_price")
    if sell is None or sell <= 0:
        sell = allegro_data.get("avg_price")
    if sell is None or sell <= 0:
        result.errors.append("No sell price found on Allegro")
        return result

    result.sell_price = sell
    result.sell_price_pln = sell  # already PLN

    # --- Platform fees ---
    fees = calculate_allegro_fees(
        sell_price_pln=sell,
        category_id=allegro_data.get("category_id"),
    )
    result.fee_breakdown = fees
    result.platform_fees_pln = fees["total_fee_pln"]

    # --- Shipping ---
    # Allegro orders are domestic PL; use a fixed estimate for InPost/DPD
    # domestic since DPD Export contract does not cover PL well.
    # Average domestic parcel cost for a <5 kg package.
    domestic_shipping_pln = 8.50
    result.shipping_cost_pln = domestic_shipping_pln

    # --- Totals ---
    result.total_costs_pln = round(
        purchase_price_pln + result.platform_fees_pln + result.shipping_cost_pln, 2
    )
    result.profit_pln = round(result.sell_price_pln - result.total_costs_pln, 2)

    if result.sell_price_pln > 0:
        result.margin_pct = round(result.profit_pln / result.sell_price_pln * 100, 1)
    if purchase_price_pln > 0:
        result.roi_pct = round(result.profit_pln / purchase_price_pln * 100, 1)

    result.verdict = _verdict(result.margin_pct, cfg)

    # Meta
    result.competition = allegro_data.get("offer_count", 0)
    result.image_url = allegro_data.get("image_url", "")

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_profitability(
    ean: str,
    purchase_price: float,
    purchase_currency: str = "PLN",
    amazon_data: dict | None = None,
    allegro_data: dict | None = None,
    config: SourcingConfig | None = None,
    weight_kg: float | None = None,
) -> list[ProfitAnalysis]:
    """Analyse profitability of *ean* across all available marketplaces.

    Parameters
    ----------
    ean : str
        EAN-13 barcode.
    purchase_price : float
        Unit purchase price in *purchase_currency*.
    purchase_currency : str
        Currency of the purchase price.
    amazon_data : dict | None
        Mapping of ``{marketplace_code: market_data_dict}`` as returned by
        the Amazon lookup module.  Each value follows the schema described
        in ``_analyse_amazon_market``.
    allegro_data : dict | None
        Single dict of Allegro listing data (see ``_analyse_allegro``).
    config : SourcingConfig | None
        Thresholds and assumptions.  Defaults are used when ``None``.
    weight_kg : float | None
        Product weight override.  Falls back to ``config.default_weight_kg``.

    Returns
    -------
    list[ProfitAnalysis]
        One entry per marketplace, sorted by ``profit_pln`` descending.
    """
    cfg = config or SourcingConfig()
    wt = weight_kg if weight_kg is not None else cfg.default_weight_kg
    purchase_pln = _purchase_in_pln(purchase_price, purchase_currency)

    results: list[ProfitAnalysis] = []

    # --- Amazon markets ---
    if amazon_data:
        for market_code, mdata in amazon_data.items():
            market_code_upper = market_code.upper()
            if market_code_upper not in cfg.amazon_markets:
                continue
            analysis = _analyse_amazon_market(
                ean=ean,
                market=market_code_upper,
                market_data=mdata,
                purchase_price_pln=purchase_pln,
                weight_kg=wt,
                cfg=cfg,
            )
            results.append(analysis)

    # --- Allegro ---
    if allegro_data:
        analysis = _analyse_allegro(
            ean=ean,
            allegro_data=allegro_data,
            purchase_price_pln=purchase_pln,
            weight_kg=wt,
            cfg=cfg,
        )
        results.append(analysis)

    # Sort best-first.
    results.sort(key=lambda r: r.profit_pln, reverse=True)
    return results
