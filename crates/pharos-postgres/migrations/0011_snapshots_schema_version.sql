-- Pharos PostgreSQL migrations: schema_version for pharos_snapshots
--
-- Pairs with pharos_es::Snapshot::schema_version and PgSnapshotStore's new
-- SnapshotUpcaster hook (crates/pharos-postgres/src/event_store.rs): without
-- a stored schema version, a snapshot payload had no way to signal which
-- shape of the state type it was written under, so a serde-compatible but
-- semantically different change to that type could deserialize successfully
-- into the wrong meaning with no error anywhere.

ALTER TABLE pharos_snapshots
    ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 0;
