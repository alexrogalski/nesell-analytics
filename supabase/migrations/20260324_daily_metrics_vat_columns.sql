-- Add VAT and net revenue columns to daily_metrics
-- These align the aggregator output with the dashboard's VAT-aware profit calculation.

ALTER TABLE daily_metrics
    ADD COLUMN IF NOT EXISTS vat_pln NUMERIC(10,2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS revenue_net_pln NUMERIC(10,2) DEFAULT 0;

-- Backfill: set revenue_net_pln = revenue_pln for existing rows (pre-VAT)
-- This ensures existing data doesn't show 0 net revenue.
UPDATE daily_metrics SET revenue_net_pln = revenue_pln WHERE revenue_net_pln = 0 AND revenue_pln > 0;

COMMENT ON COLUMN daily_metrics.vat_pln IS 'VAT amount in PLN (extracted from gross revenue using EU VAT rates by shipping country)';
COMMENT ON COLUMN daily_metrics.revenue_net_pln IS 'Net revenue in PLN (revenue_pln minus vat_pln)';
