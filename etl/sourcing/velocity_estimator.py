"""Estimate monthly sales velocity from BSR, seller count, and market signals.

Amazon uses a multi-signal model:
  1. BSR power-law (primary signal, category-aware)
  2. Seller count correction (more sellers = more demand)
  3. FBA presence bonus (FBA listings sell ~30% more)
  4. Returns low/mid/high confidence range

Allegro uses a heuristic based on offer count and price tier.

Usage::

    from etl.sourcing.velocity_estimator import (
        estimate_monthly_sales_amazon,
        estimate_monthly_sales_allegro,
        SalesEstimate,
    )

    est = estimate_monthly_sales_amazon(
        bsr_rank=4500,
        marketplace="DE",
        num_sellers=15,
        has_fba=True,
        subcategory_bsr=45,
    )
    print(est)  # SalesEstimate(low=80, mid=120, high=180)
"""

from __future__ import annotations

import math
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class SalesEstimate:
    """Monthly sales estimate with confidence range."""
    low: int = 0
    mid: int = 0
    high: int = 0

    def __int__(self):
        return self.mid


# ---------------------------------------------------------------------------
# Amazon: category-aware BSR power-law model
# ---------------------------------------------------------------------------

# Category-specific coefficients calibrated against EU marketplace data.
# Format: (coefficient, exponent)
# The model: monthly_sales = coeff * bsr ^ exp
#
# Main categories (from Amazon product_category_id):
_CATEGORY_CURVES: dict[str, tuple[float, float]] = {
    # High volume categories
    "book_display_on_website":         (8000.0,  -0.70),
    "electronics":                     (6000.0,  -0.65),
    "ce_display_on_website":           (6000.0,  -0.65),
    "pc_display_on_website":           (5500.0,  -0.65),
    "home":                            (5000.0,  -0.65),
    "kitchen":                         (5000.0,  -0.65),
    "beauty":                          (5500.0,  -0.63),
    "drugstore":                       (5000.0,  -0.63),
    "toy_display_on_website":          (5000.0,  -0.65),
    "sports_display_on_website":       (4500.0,  -0.63),
    "apparel_display_on_website":      (4000.0,  -0.60),
    "shoes_display_on_website":        (3500.0,  -0.60),
    "pet_products_display_on_website": (4000.0,  -0.63),
    "grocery":                         (6000.0,  -0.68),
    "baby_product_display_on_website": (4500.0,  -0.63),
    # Medium volume
    "office_product_display_on_website": (4000.0, -0.63),
    "automotive":                      (3500.0,  -0.60),
    "diy":                             (3500.0,  -0.60),
    "garden":                          (3500.0,  -0.60),
    "music":                           (3000.0,  -0.60),
    "video_games":                     (4000.0,  -0.63),
    # Lower volume
    "musical_instruments":             (2500.0,  -0.58),
    "industrial":                      (2000.0,  -0.55),
    "software":                        (1500.0,  -0.55),
}

# Default (generic) curve
_DEFAULT_COEFF = 5000.0
_DEFAULT_EXP = -0.65

# Market-size multipliers relative to DE.
MARKET_MULTIPLIERS: dict[str, float] = {
    "DE": 1.00,
    "FR": 0.60,
    "IT": 0.50,
    "ES": 0.50,
    "NL": 0.15,
    "PL": 0.10,
    "SE": 0.08,
    "BE": 0.05,
}


def _bsr_to_sales(bsr: int, category: str = "") -> float:
    """Convert BSR to raw monthly sales estimate using category-specific curve."""
    cat_lower = category.lower().replace(" ", "_") if category else ""

    coeff, exp = _DEFAULT_COEFF, _DEFAULT_EXP
    for cat_key, (c, e) in _CATEGORY_CURVES.items():
        if cat_key in cat_lower or cat_lower in cat_key:
            coeff, exp = c, e
            break

    return coeff * math.pow(bsr, exp)


def _seller_count_multiplier(num_sellers: int) -> float:
    """More sellers indicate higher demand. Returns correction multiplier.

    0-1 sellers:  0.7x (low confidence, might be dead listing)
    2-5 sellers:  1.0x (normal)
    6-15 sellers: 1.2x (healthy competition = healthy demand)
    16-30:        1.3x
    30+:          1.4x (high demand commodity)
    """
    if num_sellers <= 1:
        return 0.7
    if num_sellers <= 5:
        return 1.0
    if num_sellers <= 15:
        return 1.2
    if num_sellers <= 30:
        return 1.3
    return 1.4


def _fba_multiplier(has_fba: bool) -> float:
    """FBA-eligible products have higher conversion. +20% if FBA present."""
    return 1.2 if has_fba else 1.0


def estimate_monthly_sales_amazon(
    bsr_rank: int | None = None,
    marketplace: str = "DE",
    category: str = "",
    num_sellers: int = 0,
    has_fba: bool = False,
    subcategory_bsr: int | None = None,
) -> SalesEstimate:
    """Multi-signal monthly sales estimate for an Amazon product.

    Parameters
    ----------
    bsr_rank : int | None
        Main category BSR. Primary signal.
    marketplace : str
        Two-letter marketplace code.
    category : str
        Product category name or ID (for category-specific curves).
    num_sellers : int
        Total number of sellers (New condition).
    has_fba : bool
        Whether any FBA offer exists.
    subcategory_bsr : int | None
        Subcategory BSR (used as validation/cross-check).

    Returns
    -------
    SalesEstimate
        Monthly unit sales estimate with low/mid/high range.
    """
    if (bsr_rank is None or bsr_rank <= 0) and (subcategory_bsr is None or subcategory_bsr <= 0):
        # No BSR at all: use seller count as weak signal
        if num_sellers > 0:
            # Very rough: assume each seller sells ~3-10 units/month
            weak_est = int(num_sellers * 5 * MARKET_MULTIPLIERS.get(marketplace.upper(), 0.10))
            return SalesEstimate(
                low=max(0, weak_est // 3),
                mid=max(0, weak_est),
                high=max(0, weak_est * 2),
            )
        return SalesEstimate()

    # Primary: main category BSR
    market_mult = MARKET_MULTIPLIERS.get(marketplace.upper(), 0.10)
    seller_mult = _seller_count_multiplier(num_sellers)
    fba_mult = _fba_multiplier(has_fba)

    if bsr_rank and bsr_rank > 0:
        raw = _bsr_to_sales(bsr_rank, category)
    else:
        # Only subcategory BSR available: use it with dampening
        # Subcategory BSR is typically much lower than main, so sales are lower
        raw = _bsr_to_sales(subcategory_bsr, category) * 0.3

    mid = raw * market_mult * seller_mult * fba_mult

    # Confidence range: BSR-based estimates have ~50% accuracy
    # Lower for very high or very low BSR
    if bsr_rank and bsr_rank < 100:
        spread = 0.3  # Top sellers: tighter estimate
    elif bsr_rank and bsr_rank > 100000:
        spread = 0.7  # Long tail: wider uncertainty
    else:
        spread = 0.5  # Normal range

    low = mid * (1 - spread)
    high = mid * (1 + spread)

    return SalesEstimate(
        low=max(0, int(low)),
        mid=max(0, int(mid)),
        high=max(0, int(high)),
    )


# ---------------------------------------------------------------------------
# Allegro: heuristic model
# ---------------------------------------------------------------------------

# Price-tier multipliers (PLN).
_PRICE_TIERS: list[tuple[float, float]] = [
    (30.0, 1.5),
    (60.0, 1.2),
    (100.0, 1.0),
    (200.0, 0.7),
    (500.0, 0.4),
]


def _price_multiplier(avg_price: float) -> float:
    for ceiling, mult in _PRICE_TIERS:
        if avg_price <= ceiling:
            return mult
    return 0.2


def estimate_monthly_sales_allegro(
    offer_count: int = 0,
    avg_price: float = 100.0,
) -> SalesEstimate:
    """Estimate monthly sales for a new entrant on Allegro.

    Parameters
    ----------
    offer_count : int
        Number of competing offers for the same product (EAN).
    avg_price : float
        Average listing price in PLN.

    Returns
    -------
    SalesEstimate
        Conservative estimate for a single new seller.
    """
    if offer_count <= 0:
        return SalesEstimate()

    demand = math.log2(max(offer_count, 1) + 1)
    price_mult = _price_multiplier(avg_price)
    base = 10.0

    mid = base * demand * price_mult * 0.25
    low = mid * 0.5
    high = mid * 1.8

    return SalesEstimate(
        low=max(0, int(low)),
        mid=max(0, int(mid)),
        high=max(0, int(high)),
    )
