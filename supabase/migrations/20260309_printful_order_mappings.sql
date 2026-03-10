-- nesell-analytics: Printful order mappings for Amazon-Printful auto-fulfillment
-- Run this in Supabase SQL Editor

-- Order mapping: tracks Baselinker → Printful order lifecycle
CREATE TABLE IF NOT EXISTS printful_order_mappings (
    id SERIAL PRIMARY KEY,
    baselinker_order_id BIGINT NOT NULL,
    amazon_order_id TEXT,
    printful_order_id BIGINT,
    printful_external_id TEXT,
    status TEXT DEFAULT 'pending',  -- pending, created, in_production, shipped, delivered, error, cancelled
    tracking_number TEXT,
    tracking_url TEXT,
    carrier TEXT,
    error_message TEXT,
    items JSONB,  -- [{sku, variant_id, quantity}]
    shipping_address JSONB,
    printful_cost DECIMAL(10,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pom_status ON printful_order_mappings(status);
CREATE INDEX IF NOT EXISTS idx_pom_bl_order ON printful_order_mappings(baselinker_order_id);
CREATE INDEX IF NOT EXISTS idx_pom_pf_order ON printful_order_mappings(printful_order_id);
CREATE INDEX IF NOT EXISTS idx_pom_amazon_order ON printful_order_mappings(amazon_order_id);

-- Enable RLS
ALTER TABLE printful_order_mappings ENABLE ROW LEVEL SECURITY;

-- Allow service role full access
CREATE POLICY "service_all" ON printful_order_mappings FOR ALL USING (true) WITH CHECK (true);

-- Auto-update updated_at on row change
CREATE OR REPLACE FUNCTION update_pom_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_pom_updated_at
    BEFORE UPDATE ON printful_order_mappings
    FOR EACH ROW
    EXECUTE FUNCTION update_pom_updated_at();

-- View: active fulfillment pipeline
CREATE OR REPLACE VIEW v_printful_pipeline AS
SELECT
    status,
    COUNT(*) AS order_count,
    SUM(printful_cost) AS total_cost,
    MIN(created_at) AS oldest_order,
    MAX(updated_at) AS latest_update
FROM printful_order_mappings
WHERE status NOT IN ('delivered', 'cancelled')
GROUP BY status
ORDER BY
    CASE status
        WHEN 'error' THEN 1
        WHEN 'pending' THEN 2
        WHEN 'created' THEN 3
        WHEN 'in_production' THEN 4
        WHEN 'shipped' THEN 5
    END;
