-- Unified customer message center: conversations + messages
-- Sources: Allegro Message Center API, Amazon buyer emails (IMAP), Allegro Issues

CREATE TABLE IF NOT EXISTS conversations (
    id BIGSERIAL PRIMARY KEY,

    -- Source identification
    source TEXT NOT NULL,                    -- 'allegro', 'amazon_email', 'allegro_issue'
    source_thread_id TEXT,                   -- Allegro thread UUID, or synthetic amazon key

    -- Order linkage
    bl_order_id BIGINT,
    external_order_id TEXT,                  -- Amazon order ID or Allegro checkout form ID
    platform TEXT,                            -- amazon_de, amazon_fr, allegro, etc.

    -- Buyer info (denormalized)
    buyer_name TEXT,
    buyer_login TEXT,                         -- Allegro login or Amazon buyer email

    -- Conversation state
    status TEXT DEFAULT 'open',              -- open, replied, closed, escalated
    needs_reply BOOLEAN DEFAULT TRUE,
    last_message_at TIMESTAMPTZ,
    last_message_direction TEXT,             -- 'inbound', 'outbound'
    message_count INTEGER DEFAULT 0,

    -- Response tracking
    first_response_minutes INTEGER,

    -- Classification (Phase 3)
    category TEXT,                            -- shipping, return, damage, question, complaint, other
    priority TEXT DEFAULT 'normal',           -- low, normal, high, urgent

    -- Shipping problem link
    shipping_problem_id BIGINT,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(source, source_thread_id)
);

CREATE INDEX idx_conversations_needs_reply ON conversations(needs_reply) WHERE needs_reply = TRUE;
CREATE INDEX idx_conversations_status ON conversations(status);
CREATE INDEX idx_conversations_bl_order ON conversations(bl_order_id);
CREATE INDEX idx_conversations_external_order ON conversations(external_order_id);
CREATE INDEX idx_conversations_last_msg ON conversations(last_message_at DESC);
CREATE INDEX idx_conversations_platform ON conversations(platform);

CREATE TABLE IF NOT EXISTS messages (
    id BIGSERIAL PRIMARY KEY,
    conversation_id BIGINT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,

    -- Source identification
    source_message_id TEXT,                  -- Allegro message UUID or email Message-ID

    -- Content
    direction TEXT NOT NULL,                 -- 'inbound' (buyer->seller) or 'outbound' (seller->buyer)
    sender_name TEXT,
    body_text TEXT,
    body_html TEXT,

    -- Attachments (JSON array)
    attachments JSONB DEFAULT '[]'::jsonb,

    -- State
    is_read BOOLEAN DEFAULT FALSE,

    -- Email-specific
    email_uid TEXT,
    email_subject TEXT,
    email_from TEXT,
    email_message_id TEXT,

    -- Metadata
    sent_at TIMESTAMPTZ NOT NULL,
    synced_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(conversation_id, source_message_id)
);

CREATE INDEX idx_messages_conversation ON messages(conversation_id);
CREATE INDEX idx_messages_sent ON messages(sent_at DESC);
CREATE INDEX idx_messages_email_uid ON messages(email_uid);
CREATE INDEX idx_messages_direction ON messages(direction);

-- Updated_at trigger
CREATE OR REPLACE FUNCTION update_conversations_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER conversations_updated_at
    BEFORE UPDATE ON conversations
    FOR EACH ROW
    EXECUTE FUNCTION update_conversations_updated_at();

-- RLS
ALTER TABLE conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
CREATE POLICY "service_all" ON conversations FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON messages FOR ALL USING (true) WITH CHECK (true);
