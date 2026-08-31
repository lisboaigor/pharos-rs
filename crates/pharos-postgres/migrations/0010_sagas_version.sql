-- Pharos PostgreSQL migrations: optimistic-concurrency version for pharos_sagas
--
-- 0006_sagas.sql never got the version column that
-- crates/pharos-postgres/src/saga_store.rs's schema constant and every
-- PgSagaStore query (the OCC-guarded UPDATE, the SELECT that reads it back)
-- have always required. A database migrated by this file chain alone,
-- without this migration, fails at runtime on the first saga read or write.

ALTER TABLE pharos_sagas ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
