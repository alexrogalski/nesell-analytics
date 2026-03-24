"""Estimate monthly sales velocity from BSR (Amazon) and offer count (Allegro).

Amazon uses a power-law approximation calibrated against the German
marketplace.  Other EU marketplaces are scaled by a relative-volume
multiplier.

Allegro uses a heuristic based on offer count and average price tier,
since BSR is not available outside Amazon.

Usage::

    from etl.sourcing.velocity_estimator import (
        estimate_monthly_sales_amazon,
        estimate_monthly_sales_allegro,
    )

    sales = estimate_monthly_sales_amazon(bsr_rank=4500, marketplace="DE")
    sales_alg = estimate_monthly_sales_allegro(offer_count=25, avg_price=89.99)
"""

from __future__ import annotations

import math

# ---------------------------------------------------------------------------
# Amazon: BSR-to-sales power-law model
# ---------------------------------------------------------------------------

# The model is: monthly_sales = COEFFICIENT * bsr ^ EXPONENT
# Calibrated against German marketplace (DE) panel data.  The values
# approximate the mid-2025/2026 observed distribution:
#
#   BSR      1-100      -> ~2000 sales/month
#   BSR    100-1000     -> ~200-500
#   BSR   1000-5000     -> ~50-200
#   BSR   5000-20000    -> ~10-50
#   BSR  20000-100000   -> ~2-10
#   BSR 100000+         -> ~0-2

_COEFFICIENT = 5000.0
_EXPONENT = -0.65

# Market-size multipliers relative to DE.  These reflect the approximate
# ratio of total e-commerce GMV (or Amazon category depth) to Germany.
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


def estimate_monthly_sales_amazon(
    bsr_rank: int | None,
    marketplace: str = "DE",
) -> int:
    """Estimate monthly unit sales from a Best Sellers Rank.

    Parameters
    ----------
    bsr_rank : int | None
        Current BSR.  ``None`` or ``<= 0`` returns 0.
    marketplace : str
        Two-letter Amazon marketplace code (e.g. ``"DE"``, ``"FR"``).

    Returns
    -------
    int
        Estimated monthly unit sales (floored to int, minimum 0).
    """
    if bsr_rank is None or bsr_rank <= 0:
        return 0

    # Power-law estimate for DE.
    raw = _COEFFICIENT * math.pow(bsr_rank, _EXPONENT)

    # Scale to target marketplace.
    multiplier = MARKET_MULTIPLIERS.get(marketplace.upper(), 0.10)
    estimate = raw * multiplier

    return max(0, int(estimate))


# ---------------------------------------------------------------------------
# Allegro: heuristic model
# ---------------------------------------------------------------------------

# Allegro does not expose BSR.  Instead we use the number of competing
# offers and the average selling price to triangulate demand:
#
# * More offers -> higher demand category (sellers follow demand).
# * Higher price -> fewer impulse purchases, lower volume per seller.
#
# The model is intentionally conservative.  For a product with 10-20
# offers at ~100 PLN the estimate should land around 5-15 sales/month
# for a new entrant (not the market leader).

_ALLEGRO_BASE_MONTHLY = 10.0
_ALLEGRO_OFFER_SCALE = 0.25         # more offers = more demand
_ALLEGRO_PRICE_DAMPENER = -0.30     # higher price = lower velocity

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
) -> int:
    """Estimate monthly sales for a new entrant on Allegro.

    Parameters
    ----------
    offer_count : int
        Number of competing offers for the same product (EAN).
    avg_price : float
        Average listing price in PLN.

    Returns
    -------
    int
        Estimated monthly unit sales (conservative, for a single new seller).
    """
    if offer_count <= 0:
        return 0

    # Demand signal: log-scale of offer count.
    demand = math.log2(max(offer_count, 1) + 1)

    # Price dampening.
    price_mult = _price_multiplier(avg_price)

    estimate = _ALLEGRO_BASE_MONTHLY * demand * price_mult * _ALLEGRO_OFFER_SCALE
    return max(0, int(estimate))
