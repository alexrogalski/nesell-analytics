-- nesell-analytics: e-commerce profit tracking
-- Run this in Supabase SQL Editor

-- Platforms (Amazon, Allegro, Temu, Empik)
CREATE TABLE platforms (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,        -- 'amazon_de', 'allegro', 'temu', 'empik'
    name TEXT NOT NULL,
    marketplace_id TEXT,              -- Amazon marketplace ID
    currency TEXT NOT NULL DEFAULT 'EUR',
    fee_pct NUMERIC(5,2) DEFAULT 0,  -- platform referral fee %
    created_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO platforms (code, name, marketplace_id, currency, fee_pct) VALUES
    ('amazon_de', 'Amazon.de', 'A1PA6795UKMFR9', 'EUR', 15.45),
    ('amazon_fr', 'Amazon.fr', 'A13V1IB3VIYZZH', 'EUR', 15.45),
    ('amazon_es', 'Amazon.es', 'A1RKKUPIHCS9HS', 'EUR', 15.45),
    ('amazon_it', 'Amazon.it', 'APJ6JRA9NG5V4', 'EUR', 15.45),
    ('amazon_nl', 'Amazon.nl', 'A1805IZSGTT6HS', 'EUR', 15.45),
    ('amazon_pl', 'Amazon.pl', 'A1C3SOZRARQ6R3', 'PLN', 15.45),
    ('amazon_se', 'Amazon.se', 'A2NODRKZP88ZB9', 'SEK', 15.45),
    ('amazon_be', 'Amazon.be', 'AMEN7PMS3EDWL', 'EUR', 15.45),
    ('amazon_gb', 'Amazon.co.uk', 'A1F83G8C2ARO7P', 'GBP', 15.45),
    ('allegro', 'Allegro', NULL, 'PLN', 10.0),
    ('temu', 'Temu', NULL, 'PLN', 0),
    ('empik', 'Empik', NULL, 'PLN', 15.0),
    ('baselinker_other', 'Other (Baselinker)', NULL, 'PLN', 0);

-- Products (master catalog from Baselinker)
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    sku TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    brand TEXT,
    source TEXT,                      -- 'printful', 'wholesale', 'arbitrage'
    category TEXT,
    printful_product_id INTEGER,
    printful_variant_id INTEGER,
    cost_eur NUMERIC(10,2),           -- unit COGS in EUR
    cost_pln NUMERIC(10,2),           -- unit COGS in PLN
    weight_g INTEGER,
    is_parent BOOLEAN DEFAULT FALSE,
    parent_sku TEXT,
    asin TEXT,
    ean TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_asin ON products(asin);
CREATE INDEX idx_products_parent ON products(parent_sku);

-- Orders
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    external_id TEXT NOT NULL,        -- Baselinker order ID or Amazon order ID
    platform_id INTEGER REFERENCES platforms(id),
    platform_order_id TEXT,           -- original platform order ID
    order_date TIMESTAMPTZ NOT NULL,
    status TEXT,                      -- 'pending', 'shipped', 'delivered', 'cancelled', 'returned'
    buyer_email TEXT,
    shipping_country TEXT,
    shipping_cost NUMERIC(10,2) DEFAULT 0,
    total_paid NUMERIC(10,2) DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'EUR',
    total_paid_pln NUMERIC(10,2),     -- converted to PLN
    platform_fee NUMERIC(10,2) DEFAULT 0,
    platform_fee_pln NUMERIC(10,2),
    notes TEXT,
    raw_data JSONB,                   -- full API response for debugging
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(external_id, platform_id)
);

CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_platform ON orders(platform_id);
CREATE INDEX idx_orders_external ON orders(external_id);

-- Order items (line items)
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
    sku TEXT,
    product_id INTEGER REFERENCES products(id),
    name TEXT,
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price NUMERIC(10,2) NOT NULL,
    currency TEXT NOT NULL DEFAULT 'EUR',
    unit_price_pln NUMERIC(10,2),
    unit_cost NUMERIC(10,2),          -- COGS per unit
    unit_cost_pln NUMERIC(10,2),
    asin TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_sku ON order_items(sku);

-- FX rates (from NBP API)
CREATE TABLE fx_rates (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    currency TEXT NOT NULL,           -- 'EUR', 'SEK', 'GBP', 'USD'
    rate_pln NUMERIC(10,4) NOT NULL,  -- 1 unit = X PLN
    source TEXT DEFAULT 'nbp',
    UNIQUE(date, currency)
);

CREATE INDEX idx_fx_date ON fx_rates(date);

-- Daily aggregated metrics (materialized by ETL)
CREATE TABLE daily_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    platform_id INTEGER REFERENCES platforms(id),
    sku TEXT,
    orders_count INTEGER DEFAULT 0,
    units_sold INTEGER DEFAULT 0,
    revenue NUMERIC(10,2) DEFAULT 0,       -- in original currency
    revenue_pln NUMERIC(10,2) DEFAULT 0,
    cogs NUMERIC(10,2) DEFAULT 0,          -- cost of goods in PLN
    platform_fees NUMERIC(10,2) DEFAULT 0, -- in PLN
    shipping_cost NUMERIC(10,2) DEFAULT 0, -- in PLN
    gross_profit NUMERIC(10,2) DEFAULT 0,  -- revenue - cogs - fees - shipping (PLN)
    margin_pct NUMERIC(5,2) DEFAULT 0,     -- gross_profit / revenue * 100
    UNIQUE(date, platform_id, sku)
);

CREATE INDEX idx_daily_date ON daily_metrics(date);
CREATE INDEX idx_daily_platform ON daily_metrics(platform_id);
CREATE INDEX idx_daily_sku ON daily_metrics(sku);

-- Costs log (track cost changes over time)
CREATE TABLE cost_history (
    id SERIAL PRIMARY KEY,
    sku TEXT NOT NULL,
    cost_eur NUMERIC(10,2),
    cost_pln NUMERIC(10,2),
    source TEXT,                       -- 'printful_estimate', 'manual', 'invoice'
    notes TEXT,
    effective_from DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_cost_sku ON cost_history(sku);

-- Alerts log (for Telegram bot)
CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL,                -- 'daily_summary', 'anomaly', 'stock_low', 'new_review'
    message TEXT NOT NULL,
    sent BOOLEAN DEFAULT FALSE,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Views for dashboards

-- Daily P&L summary
CREATE OR REPLACE VIEW v_daily_pnl AS
SELECT
    dm.date,
    p.code AS platform,
    SUM(dm.units_sold) AS units,
    SUM(dm.revenue_pln) AS revenue_pln,
    SUM(dm.cogs) AS cogs_pln,
    SUM(dm.platform_fees) AS fees_pln,
    SUM(dm.shipping_cost) AS shipping_pln,
    SUM(dm.gross_profit) AS profit_pln,
    CASE WHEN SUM(dm.revenue_pln) > 0
         THEN ROUND(SUM(dm.gross_profit) / SUM(dm.revenue_pln) * 100, 1)
         ELSE 0 END AS margin_pct
FROM daily_metrics dm
JOIN platforms p ON p.id = dm.platform_id
GROUP BY dm.date, p.code
ORDER BY dm.date DESC, profit_pln DESC;

-- Top sellers (last 30 days)
CREATE OR REPLACE VIEW v_top_sellers_30d AS
SELECT
    dm.sku,
    pr.name,
    pr.source,
    SUM(dm.units_sold) AS units,
    SUM(dm.revenue_pln) AS revenue_pln,
    SUM(dm.gross_profit) AS profit_pln,
    CASE WHEN SUM(dm.revenue_pln) > 0
         THEN ROUND(SUM(dm.gross_profit) / SUM(dm.revenue_pln) * 100, 1)
         ELSE 0 END AS margin_pct
FROM daily_metrics dm
LEFT JOIN products pr ON pr.sku = dm.sku
WHERE dm.date >= CURRENT_DATE - 30
GROUP BY dm.sku, pr.name, pr.source
ORDER BY profit_pln DESC;

-- Platform comparison
CREATE OR REPLACE VIEW v_platform_summary AS
SELECT
    p.code AS platform,
    p.name AS platform_name,
    COUNT(DISTINCT dm.date) AS active_days,
    SUM(dm.units_sold) AS total_units,
    SUM(dm.revenue_pln) AS total_revenue_pln,
    SUM(dm.gross_profit) AS total_profit_pln,
    CASE WHEN SUM(dm.revenue_pln) > 0
         THEN ROUND(SUM(dm.gross_profit) / SUM(dm.revenue_pln) * 100, 1)
         ELSE 0 END AS avg_margin_pct
FROM daily_metrics dm
JOIN platforms p ON p.id = dm.platform_id
WHERE dm.date >= CURRENT_DATE - 30
GROUP BY p.code, p.name
ORDER BY total_profit_pln DESC;

-- Enable Row Level Security (Supabase best practice)
ALTER TABLE platforms ENABLE ROW LEVEL SECURITY;
ALTER TABLE products ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE order_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE fx_rates ENABLE ROW LEVEL SECURITY;
ALTER TABLE daily_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE cost_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE alerts ENABLE ROW LEVEL SECURITY;

-- Allow service role full access (for ETL)
CREATE POLICY "service_all" ON platforms FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON products FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON orders FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON order_items FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON fx_rates FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON daily_metrics FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON cost_history FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON alerts FOR ALL USING (true) WITH CHECK (true);
