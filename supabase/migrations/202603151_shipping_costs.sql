-- Shipping costs: track actual courier costs per order (what seller pays DPD)
-- Distinct from orders.shipping_cost which is what the BUYER paid (delivery_price = revenue)

CREATE TABLE IF NOT EXISTS shipping_costs (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
    external_order_id TEXT,                   -- Baselinker order_id for easy lookup
    courier TEXT NOT NULL DEFAULT 'dpd',      -- dpd, inpost, poczta_polska, etc.
    tracking_number TEXT,
    destination_country TEXT,                 -- ISO 2-letter code
    weight_kg NUMERIC(6,2),                   -- parcel weight
    cost_net NUMERIC(10,2),                   -- net cost in original currency
    cost_gross NUMERIC(10,2),                 -- gross cost (with VAT)
    cost_currency TEXT NOT NULL DEFAULT 'EUR', -- courier billing currency
    cost_pln NUMERIC(10,2),                   -- converted to PLN
    cost_source TEXT DEFAULT 'estimate',       -- 'estimate', 'invoice_csv', 'manual'
    ship_date DATE,                           -- when label was created/shipped
    dpd_invoice_number TEXT,                  -- DPD invoice reference (from CSV import)
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(order_id)
);

CREATE INDEX IF NOT EXISTS idx_shipping_costs_order ON shipping_costs(order_id);
CREATE INDEX IF NOT EXISTS idx_shipping_costs_external ON shipping_costs(external_order_id);
CREATE INDEX IF NOT EXISTS idx_shipping_costs_date ON shipping_costs(ship_date);
CREATE INDEX IF NOT EXISTS idx_shipping_costs_country ON shipping_costs(destination_country);

-- RLS
ALTER TABLE shipping_costs ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
    CREATE POLICY "service_all" ON shipping_costs FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Add seller_shipping_cost column to orders for quick P&L access
-- (denormalized from shipping_costs for aggregator performance)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_shipping_cost NUMERIC(10,2) DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_shipping_cost_pln NUMERIC(10,2) DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_method TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS courier_code TEXT;
