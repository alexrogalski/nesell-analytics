-- Allow registering Printful orders that came via native Printful-Amazon integration
-- (no Baselinker order ID) and add unique constraint on printful_order_id for upserts.

-- Drop NOT NULL on baselinker_order_id (native Printful orders have no BL ID)
ALTER TABLE printful_order_mappings
    ALTER COLUMN baselinker_order_id DROP NOT NULL;

-- Add unique constraint so discover_and_register can use ON CONFLICT upsert
ALTER TABLE printful_order_mappings
    ADD CONSTRAINT uq_printful_order_id UNIQUE (printful_order_id);
