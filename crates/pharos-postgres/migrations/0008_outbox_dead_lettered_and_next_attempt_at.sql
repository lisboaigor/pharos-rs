-- Pharos PostgreSQL migrations: outbox dead-letter status + retry scheduling
--
-- Brings pharos_outbox in line with POSTGRES_EVENTING_SCHEMA in
-- crates/pharos-postgres/src/eventing.rs, which 0001_eventing.sql had
-- drifted from: the CHECK constraint never allowed 'dead_lettered' (the
-- terminal state OutboxDispatcher::mark_dead_lettered writes), and
-- next_attempt_at (the claim-lease / retry-backoff column OutboxRepository
-- ::pending reads) did not exist at all. A database migrated by this file
-- chain alone, without this migration, breaks at runtime: the claim query
-- references a nonexistent column, and mark_dead_lettered violates the
-- constraint.
--
-- Safe to run against an existing deployment and safe to run more than once.

DO $do$
DECLARE
    status_check_name text;
BEGIN
    SELECT con.conname INTO status_check_name
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    WHERE rel.relname = 'pharos_outbox'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) LIKE '%status%';
    IF status_check_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE pharos_outbox DROP CONSTRAINT %I', status_check_name);
    END IF;
END
$do$;

ALTER TABLE pharos_outbox
    ADD CONSTRAINT pharos_outbox_status_check
    CHECK (status IN ('pending', 'published', 'failed', 'dead_lettered'));

ALTER TABLE pharos_outbox
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_pharos_outbox_pending_next_attempt_at
    ON pharos_outbox (next_attempt_at)
    WHERE status = 'pending';
