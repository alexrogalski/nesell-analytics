-- Fix: Remove duplicate order_items and add unique constraint on (order_id, sku)
--
-- Problem: upsert_order_items used plain INSERT with no conflict resolution,
-- causing every ETL run to re-insert items for the same orders.
-- Example: SKU 194276338162 had 33 rows but only 3 real orders (11 runs x 3).
--
-- Solution:
-- 1. Delete duplicates (keep lowest id per order_id+sku group)
-- 2. Add UNIQUE constraint so PostgREST upsert with on_conflict works

-- Step 1: Delete duplicate rows, keeping the one with the smallest id per (order_id, sku)
DELETE FROM order_items
WHERE id NOT IN (
    SELECT MIN(id)
    FROM order_items
    GROUP BY order_id, sku
);

-- Step 2: Add unique constraint for PostgREST upsert (on_conflict="order_id,sku")
ALTER TABLE order_items
    ADD CONSTRAINT order_items_order_id_sku_key UNIQUE (order_id, sku);
