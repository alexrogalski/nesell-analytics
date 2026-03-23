-- Add translation and draft reply columns to messages table
ALTER TABLE messages ADD COLUMN IF NOT EXISTS translation_pl TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS detected_language TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS draft_reply TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS draft_reply_local TEXT;  -- draft in buyer's language
ALTER TABLE messages ADD COLUMN IF NOT EXISTS ai_processed_at TIMESTAMPTZ;
