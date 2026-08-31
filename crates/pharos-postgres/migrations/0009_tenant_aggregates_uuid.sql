-- Pharos PostgreSQL migrations: tenant_id/aggregate_id type fix for
-- pharos_tenant_aggregates
--
-- 0003_tenant_aggregates.sql declared both tenant_id and aggregate_id as
-- TEXT; the actual adapter (crates/pharos-postgres/src/tenant_repository.rs)
-- has always bound both as UUID and its schema constant declares
-- `tenant_id UUID NOT NULL` and `aggregate_id UUID NOT NULL`. A database
-- migrated by this file chain alone, without this migration, fails at
-- runtime the moment a query binds a Rust Uuid against either TEXT column.
--
-- `USING ...::uuid` is safe whether a column is still TEXT (casts the
-- stored text) or already UUID (a no-op cast) — idempotent either way.

ALTER TABLE pharos_tenant_aggregates
    ALTER COLUMN tenant_id TYPE UUID USING tenant_id::uuid,
    ALTER COLUMN aggregate_id TYPE UUID USING aggregate_id::uuid;
