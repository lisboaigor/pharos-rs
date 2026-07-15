-- Pharos PostgreSQL migrations: event streams + snapshots

CREATE TABLE IF NOT EXISTS pharos_event_streams (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (stream_type, stream_id, sequence)
);

CREATE TABLE IF NOT EXISTS pharos_snapshots (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    version BIGINT NOT NULL,
    taken_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (stream_type, stream_id)
);
