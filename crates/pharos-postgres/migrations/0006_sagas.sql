-- Pharos PostgreSQL migrations: saga instances

CREATE TABLE IF NOT EXISTS pharos_sagas (
    saga_type TEXT NOT NULL,
    saga_id TEXT NOT NULL,
    state JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    deadline_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (saga_type, saga_id)
);

CREATE INDEX IF NOT EXISTS idx_pharos_sagas_due
    ON pharos_sagas (saga_type, deadline_at)
    WHERE status = 'running' AND deadline_at IS NOT NULL;
