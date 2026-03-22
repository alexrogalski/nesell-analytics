CREATE TABLE IF NOT EXISTS shipping_problems (
    id BIGSERIAL PRIMARY KEY,
    bl_order_id BIGINT NOT NULL,
    external_order_id TEXT,              -- Amazon/Allegro order ID
    tracking_number TEXT NOT NULL,
    platform TEXT,                        -- amazon_de, amazon_be, allegro, etc.
    courier TEXT DEFAULT 'DPD',

    -- Problem classification
    problem_type TEXT NOT NULL,           -- address_issue, stuck_in_transit, delivery_failed, returned, lost
    problem_detail TEXT,                  -- e.g. "bledny adres", "3x failed attempt"
    severity TEXT DEFAULT 'medium',       -- low, medium, high, critical

    -- Tracking data
    last_event_date TIMESTAMPTZ,
    last_event_text TEXT,
    days_since_last_event INT,
    delivery_attempts INT DEFAULT 0,

    -- Buyer info
    buyer_name TEXT,
    buyer_email TEXT,
    buyer_phone TEXT,
    buyer_address TEXT,
    destination_country TEXT,

    -- Order value (for prioritization)
    order_value_eur NUMERIC(10,2),

    -- Resolution workflow
    status TEXT DEFAULT 'open',           -- open, in_progress, resolved, closed, auto_resolved
    resolution TEXT,                       -- what was done to resolve
    resolved_at TIMESTAMPTZ,
    auto_action_taken TEXT,               -- if system took automatic action

    -- Metadata
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    notes TEXT,

    -- Constraints
    UNIQUE(bl_order_id, tracking_number, problem_type)
);

-- Indexes for common queries
CREATE INDEX idx_shipping_problems_status ON shipping_problems(status);
CREATE INDEX idx_shipping_problems_type ON shipping_problems(problem_type);
CREATE INDEX idx_shipping_problems_tracking ON shipping_problems(tracking_number);
CREATE INDEX idx_shipping_problems_detected ON shipping_problems(detected_at DESC);

-- Updated_at trigger
CREATE OR REPLACE FUNCTION update_shipping_problems_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER shipping_problems_updated_at
    BEFORE UPDATE ON shipping_problems
    FOR EACH ROW
    EXECUTE FUNCTION update_shipping_problems_updated_at();
