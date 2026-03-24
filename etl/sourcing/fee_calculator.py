"""Calculate total marketplace fees for Amazon and Allegro.

Amazon fees come from either the SP-API fee estimate endpoint (preferred)
or a fallback model based on referral-fee tiers and weight-based FBA
fulfillment costs.

Allegro fees use the 2026 rate card: commission + transaction fee + Smart
delivery surcharge + payment processing.

Usage::

    from etl.sourcing.fee_calculator import calculate_amazon_fees, calculate_allegro_fees

    amz = calculate_amazon_fees(sell_price=29.99, currency="EUR", weight_kg=0.4)
    alg = calculate_allegro_fees(sell_price_pln=129.99)
"""

from __future__ import annotations

from datetime import date

from ..fx_rates import fetch_nbp_rate

# ---------------------------------------------------------------------------
# Amazon fee constants (fallback model)
# ---------------------------------------------------------------------------

# Default referral-fee percentage by category keyword.
# Amazon EU referral fees range from 5% to 15.45% depending on category and
# price point.  The mapping below covers the most common overrides; the
# generic default is 15%.
_REFERRAL_RATE_DEFAULT = 0.15

_REFERRAL_OVERRIDES: dict[str, float] = {
    "clothing": 0.05,       # under 15 EUR
    "clothing_high": 0.15,  # 15 EUR and above
    "electronics": 0.07,
    "computers": 0.07,
    "video_games": 0.15,
    "books": 0.15,
    "home": 0.15,
    "toys": 0.15,
    "beauty": 0.15,
    "health": 0.15,
    "sports": 0.15,
}

# FBA fulfillment cost tiers by package weight (EUR).
_FBA_TIERS: list[tuple[float, float]] = [
    (0.5, 2.50),
    (1.0, 3.30),
    (2.0, 4.50),
    (5.0, 5.50),
    (10.0, 7.00),
    (20.0, 9.50),
    (31.5, 12.00),
]

# Monthly storage cost per unit (EUR) -- standard-size, non-peak.
_STORAGE_PER_UNIT_EUR = 0.03

# Closing fee (media-only categories; zero for most products).
_CLOSING_FEE_EUR = 0.0


def _fba_fulfillment_cost(weight_kg: float) -> float:
    """Estimate FBA fulfillment cost in EUR from package weight."""
    for bracket_kg, cost in _FBA_TIERS:
        if weight_kg <= bracket_kg:
            return cost
    return _FBA_TIERS[-1][1]


def _get_eur_pln_rate() -> float:
    """Fetch current EUR/PLN rate with a sensible fallback."""
    rate = fetch_nbp_rate("EUR", date.today())
    return rate if rate else 4.27


# ---------------------------------------------------------------------------
# Amazon
# ---------------------------------------------------------------------------

def calculate_amazon_fees(
    sell_price: float,
    currency: str = "EUR",
    *,
    referral_fee: float | None = None,
    fba_fee: float | None = None,
    weight_kg: float = 0.5,
    category: str | None = None,
    storage_months: float = 1.0,
) -> dict:
    """Return a breakdown of estimated Amazon fees for one unit.

    Parameters
    ----------
    sell_price : float
        Unit selling price in *currency*.
    currency : str
        ISO currency code (``EUR``, ``SEK``, ``PLN``, etc.).
    referral_fee : float | None
        If the SP-API returned a referral fee amount (in *currency*), pass
        it here.  Otherwise the fallback percentage model is used.
    fba_fee : float | None
        If the SP-API returned an FBA fulfillment fee (in *currency*), pass
        it here.  Otherwise the weight-based estimate is used.
    weight_kg : float
        Estimated product weight for the FBA fallback calculation.
    category : str | None
        Optional category hint (e.g. ``"clothing"``) used to pick the
        correct referral-fee tier in fallback mode.
    storage_months : float
        Number of storage months to include in the estimate.

    Returns
    -------
    dict
        ``referral_fee``, ``fba_fee``, ``storage_fee``, ``closing_fee``,
        ``total_fee`` -- all in the *currency* passed in --
        plus ``total_fee_pln`` converted at the current NBP rate.
    """
    # --- Referral fee ---
    if referral_fee is not None:
        ref_fee = abs(referral_fee)
    else:
        rate = _REFERRAL_RATE_DEFAULT
        if category:
            cat_key = category.lower().replace(" ", "_")
            if cat_key == "clothing" and sell_price >= 15.0:
                cat_key = "clothing_high"
            rate = _REFERRAL_OVERRIDES.get(cat_key, _REFERRAL_RATE_DEFAULT)
        ref_fee = round(sell_price * rate, 2)

    # --- FBA fulfillment ---
    if fba_fee is not None:
        fba = abs(fba_fee)
    else:
        fba = _fba_fulfillment_cost(weight_kg)

    # --- Storage ---
    storage = round(_STORAGE_PER_UNIT_EUR * storage_months, 2)

    # --- Closing ---
    closing = _CLOSING_FEE_EUR

    total = round(ref_fee + fba + storage + closing, 2)

    # --- Convert to PLN ---
    if currency == "PLN":
        total_pln = total
    else:
        fx = fetch_nbp_rate(currency, date.today())
        if fx is None:
            fx = _get_eur_pln_rate() if currency == "EUR" else 1.0
        total_pln = round(total * fx, 2)

    return {
        "referral_fee": round(ref_fee, 2),
        "fba_fee": round(fba, 2),
        "storage_fee": storage,
        "closing_fee": closing,
        "total_fee": total,
        "currency": currency,
        "total_fee_pln": total_pln,
    }


# ---------------------------------------------------------------------------
# Allegro (2026 rate card)
# ---------------------------------------------------------------------------

# Default commission rate (most categories).
_ALLEGRO_COMMISSION_DEFAULT = 0.10

# Category-specific overrides (Allegro category_id -> rate).
_ALLEGRO_COMMISSION_OVERRIDES: dict[str, float] = {
    "odzież": 0.10,
    "elektronika": 0.08,
    "motoryzacja": 0.08,
    "dom_i_ogrod": 0.10,
    "zdrowie": 0.10,
    "supermarket": 0.08,
}

# Fixed per-transaction fee (PLN).
_ALLEGRO_TRANSACTION_FEE_PLN = 1.00

# Allegro Smart delivery average surcharge (PLN).  The actual value varies
# by price tier and delivery method but 3.00 PLN is a conservative average
# that holds across the majority of standard-size parcels.
_ALLEGRO_SMART_SURCHARGE_PLN = 3.00

# Payment processing rate (Allegro Pay / BlueMedia).
_ALLEGRO_PAYMENT_PROCESSING_RATE = 0.012


def calculate_allegro_fees(
    sell_price_pln: float,
    category_id: str | None = None,
    *,
    commission_rate: float | None = None,
    include_smart: bool = True,
) -> dict:
    """Return a breakdown of estimated Allegro fees for one unit.

    Parameters
    ----------
    sell_price_pln : float
        Unit selling price in PLN.
    category_id : str | None
        Allegro category id or keyword for commission-rate lookup.
    commission_rate : float | None
        Explicit override for the commission percentage (0.0 -- 1.0).
    include_smart : bool
        Whether to include the Smart delivery surcharge estimate.

    Returns
    -------
    dict
        ``commission``, ``transaction_fee``, ``smart_surcharge``,
        ``payment_processing``, ``total_fee``, ``total_fee_pln`` -- all in PLN.
    """
    # Commission
    if commission_rate is not None:
        rate = commission_rate
    elif category_id and category_id in _ALLEGRO_COMMISSION_OVERRIDES:
        rate = _ALLEGRO_COMMISSION_OVERRIDES[category_id]
    else:
        rate = _ALLEGRO_COMMISSION_DEFAULT

    commission = round(sell_price_pln * rate, 2)

    # Transaction fee
    transaction = _ALLEGRO_TRANSACTION_FEE_PLN

    # Smart delivery
    smart = _ALLEGRO_SMART_SURCHARGE_PLN if include_smart else 0.0

    # Payment processing
    payment = round(sell_price_pln * _ALLEGRO_PAYMENT_PROCESSING_RATE, 2)

    total = round(commission + transaction + smart + payment, 2)

    return {
        "commission": commission,
        "commission_rate": rate,
        "transaction_fee": transaction,
        "smart_surcharge": smart,
        "payment_processing": payment,
        "total_fee": total,
        "total_fee_pln": total,  # already in PLN
    }
