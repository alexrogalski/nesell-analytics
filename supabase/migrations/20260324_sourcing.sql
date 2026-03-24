-- Sourcing tool tables: cache layer + analysis results
-- 2026-03-24

-- ── Cache for marketplace lookups (prices, BSR, competition) ────────────

CREATE TABLE IF NOT EXISTS sourcing_cache (
    id SERIAL PRIMARY KEY,
    ean TEXT NOT NULL,
    platform TEXT NOT NULL,
    marketplace_id TEXT,
    data_json JSONB NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(ean, platform, marketplace_id)
);

CREATE INDEX IF NOT EXISTS idx_sourcing_cache_ean
    ON sourcing_cache(ean);


-- ── Per-product analysis results ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS sourcing_analyses (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    ean TEXT NOT NULL,
    asin TEXT,
    platform TEXT NOT NULL,
    supplier_price NUMERIC(10,2),
    supplier_currency TEXT DEFAULT 'PLN',
    sell_price NUMERIC(10,2),
    sell_currency TEXT,
    sell_price_pln NUMERIC(10,2),
    purchase_price_pln NUMERIC(10,2),
    total_fees NUMERIC(10,2),
    total_fees_pln NUMERIC(10,2),
    shipping_cost_pln NUMERIC(10,2),
    profit_pln NUMERIC(10,2),
    margin_pct NUMERIC(5,2),
    roi_pct NUMERIC(5,2),
    verdict TEXT,
    bsr_rank INTEGER,
    competition_count INTEGER,
    estimated_monthly_sales INTEGER,
    recommended_qty INTEGER,
    data_json JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sourcing_run
    ON sourcing_analyses(run_id);
CREATE INDEX IF NOT EXISTS idx_sourcing_ean
    ON sourcing_analyses(ean);


-- ── Row-level security (service role has full access) ───────────────────

ALTER TABLE sourcing_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE sourcing_analyses ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_all_cache"
    ON sourcing_cache FOR ALL
    USING (true) WITH CHECK (true);

CREATE POLICY "service_all_analyses"
    ON sourcing_analyses FOR ALL
    USING (true) WITH CHECK (true);
