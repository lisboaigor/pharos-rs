-- Pharos PostgreSQL migrations: tenant scoping for the event store
--
-- Adds tenant_id to pharos_event_streams and pharos_snapshots, closing a
-- cross-tenant collision: without it, two tenants using the same natural or
-- sequential stream_id ("order-42", "ledger-1") shared one stream — an
-- append from one tenant could conflict with, or interleave into, another
-- tenant's history.
--
-- Safe to run against an existing deployment: the column add is a no-op
-- once present, and the primary-key swap is skipped once tenant_id is
-- already part of the key. Idempotent, so it is also safe to run more than
-- once (e.g. as part of every application startup).

ALTER TABLE pharos_event_streams
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000000';

DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tc.constraint_name
         AND kcu.table_schema = tc.table_schema
        WHERE tc.table_name = 'pharos_event_streams'
          AND tc.constraint_type = 'PRIMARY KEY'
          AND kcu.column_name = 'tenant_id'
    ) THEN
        ALTER TABLE pharos_event_streams DROP CONSTRAINT pharos_event_streams_pkey;
        ALTER TABLE pharos_event_streams
            ADD PRIMARY KEY (tenant_id, stream_type, stream_id, sequence);
    END IF;
END
$do$;

ALTER TABLE pharos_snapshots
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000000';

DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tc.constraint_name
         AND kcu.table_schema = tc.table_schema
        WHERE tc.table_name = 'pharos_snapshots'
          AND tc.constraint_type = 'PRIMARY KEY'
          AND kcu.column_name = 'tenant_id'
    ) THEN
        ALTER TABLE pharos_snapshots DROP CONSTRAINT pharos_snapshots_pkey;
        ALTER TABLE pharos_snapshots
            ADD PRIMARY KEY (tenant_id, stream_type, stream_id);
    END IF;
END
$do$;
