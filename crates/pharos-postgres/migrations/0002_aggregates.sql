-- Pharos PostgreSQL migrations: JSON aggregate repository

CREATE TABLE IF NOT EXISTS pharos_aggregates (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);

CREATE INDEX IF NOT EXISTS idx_pharos_aggregates_type_updated_at
    ON pharos_aggregates (aggregate_type, updated_at);
