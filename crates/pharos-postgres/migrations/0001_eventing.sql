-- Pharos PostgreSQL migrations: eventing (outbox + inbox)

CREATE TABLE IF NOT EXISTS pharos_outbox (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL,
    topic TEXT NOT NULL,
    message_key TEXT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload BYTEA NOT NULL,
    content_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_pharos_outbox_pending_created_at
    ON pharos_outbox (created_at)
    WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS pharos_inbox (
    message_id UUID NOT NULL,
    consumer TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('processing', 'completed', 'failed')),
    received_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_error TEXT NULL,
    PRIMARY KEY (message_id, consumer)
);

CREATE INDEX IF NOT EXISTS idx_pharos_inbox_status_updated_at
    ON pharos_inbox (status, updated_at);
