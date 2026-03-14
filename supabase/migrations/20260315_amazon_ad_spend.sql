-- Amazon Advertising Spend table
-- Stores PPC/Sponsored Products/Brands/Display spend data
-- Source: CSV import from Seller Central or Amazon Advertising API

CREATE TABLE IF NOT EXISTS amazon_ad_spend (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    campaign_name TEXT,
    campaign_type TEXT,           -- 'SP' (Sponsored Products), 'SB' (Sponsored Brands), 'SD' (Sponsored Display)
    marketplace_id TEXT,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    spend NUMERIC(12,4) DEFAULT 0,
    sales NUMERIC(12,4) DEFAULT 0,
    acos NUMERIC(8,4) DEFAULT 0,  -- Advertising Cost of Sales (%)
    roas NUMERIC(8,4) DEFAULT 0,  -- Return on Ad Spend
    orders INTEGER DEFAULT 0,
    currency TEXT DEFAULT 'EUR',
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(date, campaign_name, marketplace_id)
);

CREATE INDEX IF NOT EXISTS idx_amz_ads_date ON amazon_ad_spend(date);
CREATE INDEX IF NOT EXISTS idx_amz_ads_campaign ON amazon_ad_spend(campaign_name);
CREATE INDEX IF NOT EXISTS idx_amz_ads_type ON amazon_ad_spend(campaign_type);

-- Enable RLS
ALTER TABLE amazon_ad_spend ENABLE ROW LEVEL SECURITY;

-- Allow service role full access
CREATE POLICY "service_all" ON amazon_ad_spend FOR ALL USING (true) WITH CHECK (true);

-- Useful view: daily ad spend totals
CREATE OR REPLACE VIEW v_daily_ad_spend AS
SELECT
    date,
    campaign_type,
    currency,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(spend) AS total_spend,
    SUM(sales) AS total_sales,
    SUM(orders) AS total_orders,
    CASE WHEN SUM(sales) > 0 THEN SUM(spend) / SUM(sales) * 100 ELSE 0 END AS acos_pct,
    CASE WHEN SUM(spend) > 0 THEN SUM(sales) / SUM(spend) ELSE 0 END AS roas
FROM amazon_ad_spend
GROUP BY date, campaign_type, currency
ORDER BY date DESC;
