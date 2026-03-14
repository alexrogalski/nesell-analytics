-- Add image_url column to products table for product thumbnails in dashboard
ALTER TABLE products ADD COLUMN IF NOT EXISTS image_url TEXT;
