-- nesell-analytics: Add tracking sync status columns to printful_order_mappings
-- These columns track whether tracking info has been pushed to BL and Amazon.
-- Run this in Supabase SQL Editor AFTER 20260309_printful_order_mappings.sql

-- Sync status: has tracking been pushed to Baselinker?
ALTER TABLE printful_order_mappings
  ADD COLUMN IF NOT EXISTS bl_tracking_synced BOOLEAN DEFAULT FALSE;

-- Sync status: has tracking been confirmed on Amazon?
ALTER TABLE printful_order_mappings
  ADD COLUMN IF NOT EXISTS amz_tracking_synced BOOLEAN DEFAULT FALSE;

-- Timestamp: when was tracking synced to Baselinker?
ALTER TABLE printful_order_mappings
  ADD COLUMN IF NOT EXISTS bl_synced_at TIMESTAMPTZ;

-- Timestamp: when was tracking confirmed on Amazon?
ALTER TABLE printful_order_mappings
  ADD COLUMN IF NOT EXISTS amz_synced_at TIMESTAMPTZ;

-- Index for the tracking sync query (pending orders)
CREATE INDEX IF NOT EXISTS idx_pom_tracking_sync
  ON printful_order_mappings(bl_tracking_synced)
  WHERE status NOT IN ('delivered', 'cancelled');

-- View: tracking sync pipeline status
CREATE OR REPLACE VIEW v_tracking_sync_status AS
SELECT
  status,
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE tracking_number IS NOT NULL) AS has_tracking,
  COUNT(*) FILTER (WHERE bl_tracking_synced) AS bl_synced,
  COUNT(*) FILTER (WHERE amz_tracking_synced) AS amz_synced,
  COUNT(*) FILTER (WHERE error_message IS NOT NULL) AS has_errors,
  MIN(created_at) AS oldest,
  MAX(updated_at) AS latest_update
FROM printful_order_mappings
GROUP BY status
ORDER BY
  CASE status
    WHEN 'error' THEN 1
    WHEN 'pending' THEN 2
    WHEN 'created' THEN 3
    WHEN 'in_production' THEN 4
    WHEN 'shipped' THEN 5
    WHEN 'delivered' THEN 6
    WHEN 'cancelled' THEN 7
  END;
