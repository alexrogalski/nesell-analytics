-- nesell-analytics v2: Amazon data pipeline tables
-- Run this in Supabase SQL Editor

-- Amazon traffic data (from Sales & Traffic Report)
CREATE TABLE IF NOT EXISTS amazon_traffic (
    id SERIAL PRIMARY KEY,
    date DATE,
    asin TEXT NOT NULL,
    parent_asin TEXT,
    marketplace_id TEXT NOT NULL,
    sessions INTEGER DEFAULT 0,
    page_views INTEGER DEFAULT 0,
    buy_box_pct NUMERIC(5,2) DEFAULT 0,
    units_ordered INTEGER DEFAULT 0,
    units_ordered_b2b INTEGER DEFAULT 0,
    ordered_product_sales NUMERIC(12,2) DEFAULT 0,
    total_order_items INTEGER DEFAULT 0,
    currency TEXT DEFAULT 'EUR',
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(date, asin, marketplace_id)
);
CREATE INDEX IF NOT EXISTS idx_amz_traffic_date ON amazon_traffic(date);
CREATE INDEX IF NOT EXISTS idx_amz_traffic_asin ON amazon_traffic(asin);

-- FBA Inventory snapshots
CREATE TABLE IF NOT EXISTS amazon_inventory (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    sku TEXT NOT NULL,
    fnsku TEXT,
    asin TEXT,
    product_name TEXT,
    marketplace_id TEXT,
    country TEXT,
    fulfillable_qty INTEGER DEFAULT 0,
    inbound_working_qty INTEGER DEFAULT 0,
    inbound_shipped_qty INTEGER DEFAULT 0,
    inbound_receiving_qty INTEGER DEFAULT 0,
    reserved_qty INTEGER DEFAULT 0,
    unfulfillable_qty INTEGER DEFAULT 0,
    total_qty INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(snapshot_date, sku)
);
CREATE INDEX IF NOT EXISTS idx_amz_inv_date ON amazon_inventory(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_amz_inv_sku ON amazon_inventory(sku);

-- FBA Storage Fees
CREATE TABLE IF NOT EXISTS amazon_storage_fees (
    id SERIAL PRIMARY KEY,
    month TEXT NOT NULL,
    asin TEXT NOT NULL,
    fnsku TEXT,
    product_name TEXT,
    fulfillment_center TEXT,
    country_code TEXT,
    avg_qty INTEGER DEFAULT 0,
    avg_qty_pending_removal INTEGER DEFAULT 0,
    estimated_storage_fee NUMERIC(10,4) DEFAULT 0,
    currency TEXT DEFAULT 'EUR',
    product_size_tier TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(month, asin)
);

-- Estimated FBA Fees per SKU
CREATE TABLE IF NOT EXISTS amazon_fba_fees (
    id SERIAL PRIMARY KEY,
    sku TEXT NOT NULL,
    fnsku TEXT,
    asin TEXT,
    product_name TEXT,
    product_group TEXT,
    brand TEXT,
    fulfillment_channel TEXT,
    your_price NUMERIC(10,2) DEFAULT 0,
    sales_price NUMERIC(10,2) DEFAULT 0,
    estimated_referral_fee NUMERIC(10,2) DEFAULT 0,
    estimated_variable_closing_fee NUMERIC(10,2) DEFAULT 0,
    estimated_pick_pack_fee NUMERIC(10,2) DEFAULT 0,
    estimated_weight_handling_fee NUMERIC(10,2) DEFAULT 0,
    estimated_fee_total NUMERIC(10,2) DEFAULT 0,
    currency TEXT DEFAULT 'EUR',
    size_tier TEXT,
    item_weight_g NUMERIC(10,2),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(sku)
);

-- FBA Customer Returns
CREATE TABLE IF NOT EXISTS amazon_returns (
    id SERIAL PRIMARY KEY,
    return_date DATE,
    order_id TEXT NOT NULL,
    sku TEXT,
    asin TEXT,
    fnsku TEXT,
    product_name TEXT,
    quantity INTEGER DEFAULT 1,
    fulfillment_center_id TEXT,
    detailed_disposition TEXT,
    reason TEXT,
    status TEXT,
    customer_comments TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_amz_returns_date ON amazon_returns(return_date);
CREATE INDEX IF NOT EXISTS idx_amz_returns_sku ON amazon_returns(sku);

-- Amazon Reimbursements
CREATE TABLE IF NOT EXISTS amazon_reimbursements (
    id SERIAL PRIMARY KEY,
    approval_date DATE,
    reimbursement_id TEXT NOT NULL,
    case_id TEXT,
    amazon_order_id TEXT,
    reason TEXT,
    sku TEXT,
    fnsku TEXT,
    asin TEXT,
    product_name TEXT,
    condition TEXT,
    currency_unit TEXT DEFAULT 'EUR',
    amount_per_unit NUMERIC(10,2) DEFAULT 0,
    amount_total NUMERIC(10,2) DEFAULT 0,
    quantity_reimbursed_cash INTEGER DEFAULT 0,
    quantity_reimbursed_inventory INTEGER DEFAULT 0,
    quantity_reimbursed_total INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(reimbursement_id, sku)
);
CREATE INDEX IF NOT EXISTS idx_amz_reimb_date ON amazon_reimbursements(approval_date);

-- BSR (Best Sellers Rank) snapshots
CREATE TABLE IF NOT EXISTS amazon_bsr (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    asin TEXT NOT NULL,
    marketplace_id TEXT NOT NULL,
    bsr_rank INTEGER,
    category_id TEXT,
    category_name TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(snapshot_date, asin, marketplace_id, category_id)
);
CREATE INDEX IF NOT EXISTS idx_amz_bsr_date ON amazon_bsr(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_amz_bsr_asin ON amazon_bsr(asin);

-- Competitive Pricing snapshots
CREATE TABLE IF NOT EXISTS amazon_pricing (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    asin TEXT NOT NULL,
    marketplace_id TEXT NOT NULL,
    buy_box_price NUMERIC(10,2),
    buy_box_shipping NUMERIC(10,2),
    buy_box_landed_price NUMERIC(10,2),
    lowest_fba_price NUMERIC(10,2),
    lowest_fbm_price NUMERIC(10,2),
    num_offers_new INTEGER DEFAULT 0,
    num_offers_used INTEGER DEFAULT 0,
    list_price NUMERIC(10,2),
    our_price NUMERIC(10,2),
    currency TEXT DEFAULT 'EUR',
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(snapshot_date, asin, marketplace_id)
);
CREATE INDEX IF NOT EXISTS idx_amz_pricing_date ON amazon_pricing(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_amz_pricing_asin ON amazon_pricing(asin);

-- Settlement data (raw line items)
CREATE TABLE IF NOT EXISTS amazon_settlements (
    id SERIAL PRIMARY KEY,
    settlement_id TEXT,
    settlement_start_date TEXT,
    settlement_end_date TEXT,
    order_id TEXT,
    sku TEXT,
    amount_type TEXT,
    amount_description TEXT,
    amount NUMERIC(12,2) DEFAULT 0,
    currency TEXT DEFAULT 'EUR',
    marketplace_name TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_amz_settle_sid ON amazon_settlements(settlement_id);
CREATE INDEX IF NOT EXISTS idx_amz_settle_order ON amazon_settlements(order_id);

-- Enable RLS on all new tables
ALTER TABLE amazon_traffic ENABLE ROW LEVEL SECURITY;
ALTER TABLE amazon_inventory ENABLE ROW LEVEL SECURITY;
ALTER TABLE amazon_storage_fees ENABLE ROW LEVEL SECURITY;
ALTER TABLE amazon_fba_fees ENABLE ROW LEVEL SECURITY;
ALTER TABLE amazon_returns ENABLE ROW LEVEL SECURITY;
ALTER TABLE amazon_reimbursements ENABLE ROW LEVEL SECURITY;
ALTER TABLE amazon_bsr ENABLE ROW LEVEL SECURITY;
ALTER TABLE amazon_pricing ENABLE ROW LEVEL SECURITY;
ALTER TABLE amazon_settlements ENABLE ROW LEVEL SECURITY;

-- Allow service role full access
CREATE POLICY "service_all" ON amazon_traffic FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON amazon_inventory FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON amazon_storage_fees FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON amazon_fba_fees FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON amazon_returns FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON amazon_reimbursements FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON amazon_bsr FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON amazon_pricing FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON amazon_settlements FOR ALL USING (true) WITH CHECK (true);

-- Useful views

-- Returns summary per SKU (last 90 days)
CREATE OR REPLACE VIEW v_returns_summary AS
SELECT
    sku,
    reason,
    COUNT(*) AS return_count,
    SUM(quantity) AS total_units,
    MIN(return_date) AS first_return,
    MAX(return_date) AS last_return
FROM amazon_returns
WHERE return_date >= CURRENT_DATE - 90
GROUP BY sku, reason
ORDER BY return_count DESC;

-- Reimbursements summary
CREATE OR REPLACE VIEW v_reimbursements_summary AS
SELECT
    reason,
    COUNT(*) AS count,
    SUM(amount_total) AS total_amount,
    currency_unit
FROM amazon_reimbursements
WHERE approval_date >= CURRENT_DATE - 90
GROUP BY reason, currency_unit
ORDER BY total_amount DESC;

-- Inventory health
CREATE OR REPLACE VIEW v_inventory_health AS
SELECT
    i.sku,
    i.asin,
    i.product_name,
    i.fulfillable_qty,
    i.reserved_qty,
    i.unfulfillable_qty,
    i.total_qty,
    i.inbound_shipped_qty + i.inbound_receiving_qty AS inbound_qty,
    COALESCE(f.estimated_fee_total, 0) AS estimated_fba_fee,
    COALESCE(p.cost_pln, 0) AS cost_pln
FROM amazon_inventory i
LEFT JOIN amazon_fba_fees f ON f.sku = i.sku
LEFT JOIN products p ON p.sku = i.sku
WHERE i.snapshot_date = CURRENT_DATE
ORDER BY i.fulfillable_qty DESC;
