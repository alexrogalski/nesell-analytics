"""Recommend purchase quantities based on profitability and velocity data.

The recommender filters profitable markets, sums expected monthly sales,
and then caps the order by investment budget and warehouse limits.  It
also assigns a risk score based on market diversification and margin
depth.

Usage::

    from etl.sourcing.quantity_recommender import recommend, PurchaseRecommendation

    rec = recommend(
        ean="5904066095280",
        purchase_price_pln=15.50,
        analyses=profit_results,
        monthly_sales={"amazon_de": 45, "amazon_fr": 20, "allegro": 8},
        config=cfg,
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .config import SourcingConfig
from .profit_calculator import ProfitAnalysis

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class PurchaseRecommendation:
    """Actionable purchase recommendation for a single EAN."""

    ean: str
    name: str
    recommended_qty: int
    best_markets: list[str]
    estimated_monthly_sales: int
    total_investment_pln: float
    estimated_monthly_profit_pln: float
    roi_pct: float
    payback_months: float
    risk_score: str             # LOW, MEDIUM, HIGH
    notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Risk scoring
# ---------------------------------------------------------------------------

def _assess_risk(
    profitable_count: int,
    avg_margin: float,
    monthly_sales: int,
    purchase_price_pln: float,
    recommended_qty: int,
) -> tuple[str, list[str]]:
    """Return (risk_label, notes) based on diversification and margin."""
    notes: list[str] = []

    # Start at MEDIUM and adjust.
    score = 1  # 0 = LOW, 1 = MEDIUM, 2 = HIGH

    # Market diversification
    if profitable_count >= 3:
        score -= 1
        notes.append(f"Diversified across {profitable_count} profitable markets")
    elif profitable_count == 1:
        score += 1
        notes.append("Only 1 profitable market: concentration risk")

    # Margin depth
    if avg_margin >= 30:
        score -= 1
        notes.append(f"Strong average margin ({avg_margin:.0f}%)")
    elif avg_margin < 15:
        score += 1
        notes.append(f"Thin average margin ({avg_margin:.0f}%): price-sensitive")

    # Sales velocity
    if monthly_sales < 5:
        score += 1
        notes.append(f"Very low velocity ({monthly_sales} units/month): slow turnover")
    elif monthly_sales >= 50:
        notes.append(f"High velocity ({monthly_sales} units/month)")

    # Capital at risk
    investment = purchase_price_pln * recommended_qty
    if investment >= 3000:
        score += 1
        notes.append(f"Large investment ({investment:,.0f} PLN)")

    # Clamp to valid range.
    if score <= 0:
        return "LOW", notes
    if score >= 2:
        return "HIGH", notes
    return "MEDIUM", notes


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def recommend(
    ean: str,
    purchase_price_pln: float,
    analyses: list[ProfitAnalysis],
    monthly_sales: dict[str, int],
    config: SourcingConfig | None = None,
    name: str = "",
) -> PurchaseRecommendation:
    """Generate a purchase recommendation for *ean*.

    Parameters
    ----------
    ean : str
        EAN-13 code.
    purchase_price_pln : float
        Unit purchase cost in PLN.
    analyses : list[ProfitAnalysis]
        Profitability results across all marketplaces (from
        ``profit_calculator.analyze_profitability``).
    monthly_sales : dict[str, int]
        ``{platform_key: estimated_monthly_units}`` from the velocity
        estimator.
    config : SourcingConfig | None
        Budget and threshold settings.
    name : str
        Product name (for display purposes).

    Returns
    -------
    PurchaseRecommendation
    """
    cfg = config or SourcingConfig()

    # 1. Filter to profitable markets only.
    profitable = [a for a in analyses if a.margin_pct >= cfg.min_margin_pct and not a.errors]

    if not profitable:
        return PurchaseRecommendation(
            ean=ean,
            name=name or (analyses[0].title if analyses else ""),
            recommended_qty=0,
            best_markets=[],
            estimated_monthly_sales=0,
            total_investment_pln=0.0,
            estimated_monthly_profit_pln=0.0,
            roi_pct=0.0,
            payback_months=0.0,
            risk_score="HIGH",
            notes=["No profitable markets found"],
        )

    # 2. Sum monthly sales across profitable markets.
    total_monthly = 0
    best_markets: list[str] = []
    monthly_profit_total = 0.0

    for a in profitable:
        platform_key = a.platform
        sales = monthly_sales.get(platform_key, 0)
        total_monthly += sales
        best_markets.append(platform_key)
        monthly_profit_total += a.profit_pln * max(sales, 1)

    # Use the best title available.
    product_name = name
    if not product_name:
        for a in profitable:
            if a.title:
                product_name = a.title
                break

    # 3. Target quantity = monthly sales x months of stock.
    target_qty = int(total_monthly * cfg.target_months_stock)

    # 4. Apply caps.
    if purchase_price_pln > 0:
        budget_cap = int(cfg.max_investment_per_product_pln / purchase_price_pln)
    else:
        budget_cap = cfg.max_order_qty

    recommended_qty = max(cfg.min_order_qty, min(target_qty, budget_cap, cfg.max_order_qty))

    # Ensure at least MOQ if the product is profitable at all.
    if recommended_qty < cfg.min_order_qty:
        recommended_qty = cfg.min_order_qty

    # 5. Investment and ROI.
    total_investment = round(purchase_price_pln * recommended_qty, 2)

    # Weighted-average margin and ROI across profitable markets.
    avg_margin = 0.0
    avg_roi = 0.0
    if profitable:
        avg_margin = sum(a.margin_pct for a in profitable) / len(profitable)
        avg_roi = sum(a.roi_pct for a in profitable) / len(profitable)

    # Payback period in months.
    if monthly_profit_total > 0:
        payback = round(total_investment / monthly_profit_total, 1)
    else:
        payback = 0.0

    # 6. Risk assessment.
    risk, notes = _assess_risk(
        profitable_count=len(profitable),
        avg_margin=avg_margin,
        monthly_sales=total_monthly,
        purchase_price_pln=purchase_price_pln,
        recommended_qty=recommended_qty,
    )

    # Extra notes for the analyst.
    if target_qty > cfg.max_order_qty:
        notes.append(
            f"Demand ({target_qty} units) exceeds max order qty ({cfg.max_order_qty})"
        )
    if target_qty > budget_cap:
        notes.append(
            f"Demand ({target_qty} units) exceeds budget cap ({budget_cap} units at "
            f"{purchase_price_pln:.2f} PLN)"
        )

    return PurchaseRecommendation(
        ean=ean,
        name=product_name,
        recommended_qty=recommended_qty,
        best_markets=best_markets,
        estimated_monthly_sales=total_monthly,
        total_investment_pln=total_investment,
        estimated_monthly_profit_pln=round(monthly_profit_total, 2),
        roi_pct=round(avg_roi, 1),
        payback_months=payback,
        risk_score=risk,
        notes=notes,
    )
