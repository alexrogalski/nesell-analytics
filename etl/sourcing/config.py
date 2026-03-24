"""Sourcing-specific configuration."""

from dataclasses import dataclass, field


@dataclass
class SourcingConfig:
    """Thresholds, market list, cost assumptions, and rate-limit settings."""

    # Margin thresholds (percent)
    min_margin_pct: float = 10.0
    target_margin_pct: float = 25.0
    excellent_margin_pct: float = 40.0

    # Markets to scan
    amazon_markets: list[str] = field(
        default_factory=lambda: ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"]
    )
    include_allegro: bool = True

    # Logistics cost defaults
    default_weight_kg: float = 0.5
    fba_inbound_cost_per_unit_eur: float = 0.50
    fba_monthly_storage_eur: float = 0.03
    vat_rate: float = 0.23

    # Investment limits
    max_investment_per_product_pln: float = 5000.0
    target_months_stock: float = 2.0
    min_order_qty: int = 1
    max_order_qty: int = 100

    # Cache / rate-limit
    cache_ttl_hours: int = 24
    amazon_delay_sec: float = 2.0
    allegro_delay_sec: float = 0.5

    # Optional external API keys
    keepa_api_key: str | None = None
