-- Pharos PostgreSQL migrations: dead-letter queue

CREATE TABLE IF NOT EXISTS pharos_dead_letter (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL,
    topic TEXT NOT NULL,
    message_key TEXT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload BYTEA NOT NULL,
    content_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pharos_dead_lettered_at
    ON pharos_dead_letter (dead_lettered_at DESC);
