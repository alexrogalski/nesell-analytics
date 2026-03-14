-- Track which DPD invoice emails have been processed (to avoid re-importing)
CREATE TABLE IF NOT EXISTS dpd_invoice_imports (
    id SERIAL PRIMARY KEY,
    email_uid TEXT NOT NULL UNIQUE,           -- IMAP UID of the processed email
    email_subject TEXT,                        -- email subject for human reference
    email_date TIMESTAMPTZ,                    -- email send date
    invoice_number TEXT,                       -- DPD invoice number (e.g., BC10918587)
    invoice_period_end DATE,                   -- invoice billing period end date
    attachment_filename TEXT,                  -- original attachment filename
    shipments_found INTEGER DEFAULT 0,         -- total unique tracking numbers in spec
    shipments_matched INTEGER DEFAULT 0,       -- matched to existing shipping_costs
    shipments_updated INTEGER DEFAULT 0,       -- actually updated (had estimate before)
    total_net_pln NUMERIC(12,2) DEFAULT 0,     -- sum of all net costs in the spec
    total_gross_pln NUMERIC(12,2) DEFAULT 0,   -- sum of all gross costs (net * 1.23)
    processed_at TIMESTAMPTZ DEFAULT now(),
    error_message TEXT,                        -- if processing failed, store the error
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_dpd_imports_invoice ON dpd_invoice_imports(invoice_number);
CREATE INDEX idx_dpd_imports_date ON dpd_invoice_imports(email_date);

-- RLS
ALTER TABLE dpd_invoice_imports ENABLE ROW LEVEL SECURITY;
CREATE POLICY "service_all" ON dpd_invoice_imports FOR ALL USING (true) WITH CHECK (true);
