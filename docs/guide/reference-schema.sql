-- Pharos RS — reference PostgreSQL schema
-- ============================================================================
--
-- This is not code the framework runs. `pharos-rs` ships no storage adapter
-- and applies no migration on your behalf — see `writing-an-adapter.md` for
-- why, and for the two traits (`Repository`/`TransactionalStore` +
-- `TransactionalRepository`) you implement against whatever's below.
--
-- What this file is: the exact schema `pharos-postgres`'s adapters used to
-- create for you, kept here so a new adapter (a SeaORM one, a hand-rolled
-- one, a different database entirely) starts from a schema every non-obvious
-- decision in it has already been made and explained, instead of a blank
-- page. Every table below pairs with a `pharos-testing::contract` conformance
-- suite that proves an implementation against it (or an equivalent schema on
-- your own database) is correct — see that crate's docs for which suite goes
-- with which table.
--
-- Copy what you need, drop what you don't (JSONB repositories, event
-- sourcing, sagas, and messaging infrastructure are independent — most
-- applications use two or three of these tables, not all of them), and adapt
-- the SQL dialect if you're not on Postgres. The comments explain *why* each
-- non-obvious piece is there, not just what it does, so a straight port to
-- another engine (MySQL, SQLite) knows what it must preserve semantically
-- even where the syntax has to change (e.g. every engine's own take on
-- `SELECT ... FOR UPDATE SKIP LOCKED`, or an advisory-lock/single-writer
-- substitute where it's missing).


-- ── Aggregate repository (JSONB) ────────────────────────────────────────────
-- Pairs with: Repository<A> — pharos_testing::contract::repository
--
-- One row per aggregate, payload as an opaque JSON blob. This is the
-- "aggregate as a document" storage shape: cheapest to stand up, no
-- per-field columns to maintain as the aggregate's shape evolves. Use this
-- unless you specifically need to query on an aggregate's internal fields
-- from SQL (in which case model those fields as real columns instead) or
-- need the append-only history event sourcing gives you (see the event
-- store section below).

CREATE TABLE IF NOT EXISTS pharos_aggregates (
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    payload        JSONB NOT NULL,
    version        BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
CREATE INDEX IF NOT EXISTS idx_pharos_aggregates_type_updated_at
    ON pharos_aggregates (aggregate_type, updated_at);

-- The single query this table exists to make correct: an upsert that only
-- ever creates a never-seen row or advances one whose stored version still
-- matches what the caller loaded. Any other outcome — the row exists but at
-- a different version, or an update predicate that doesn't check version at
-- all — must return zero affected rows, which is how `Repository::save`
-- distinguishes success from `RepositoryError::ConcurrencyConflict`.
--
-- INSERT ... ON CONFLICT DO NOTHING (`version == 0`, i.e. never persisted):
--   INSERT INTO pharos_aggregates (aggregate_type, aggregate_id, payload, version, updated_at)
--   VALUES ($1, $2, $3::jsonb, $4, $5)
--   ON CONFLICT (aggregate_type, aggregate_id) DO NOTHING;
--   -- 0 rows affected => someone else created this aggregate first => conflict.
--
-- UPDATE ... WHERE version = $expected (`version > 0`, i.e. updating):
--   UPDATE pharos_aggregates
--   SET payload = $3::jsonb, version = $4, updated_at = $5
--   WHERE aggregate_type = $1 AND aggregate_id = $2 AND version = $expected;
--   -- 0 rows affected => the stored version moved => conflict; re-SELECT the
--   -- current version to report in RepositoryError::ConcurrencyConflict's
--   -- `actual` field.


-- ── Multi-tenant aggregate repository (JSONB + row-level security) ─────────
-- Pairs with: Repository<A> — pharos_testing::contract::repository
--
-- Same shape as `pharos_aggregates`, with `tenant_id` as the leading key
-- column and an RLS policy that makes cross-tenant leakage a database-level
-- impossibility rather than an application-level discipline: an application
-- bug that forgets a `WHERE tenant_id = ...` clause gets zero rows back, not
-- another tenant's rows. `aggregate_id` is UUID here (not TEXT) end-to-end —
-- see the note on `TenantId(Uuid)` in the framework's tenant handling: the
-- tenant boundary is validated once, at the edge, and every layer beneath it
-- trusts the type instead of re-parsing a string.

CREATE TABLE IF NOT EXISTS pharos_tenant_aggregates (
    tenant_id      UUID NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   UUID NOT NULL,
    payload        JSONB NOT NULL,
    version        BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, aggregate_type, aggregate_id)
);
CREATE INDEX IF NOT EXISTS idx_pharos_tenant_aggregates_type_updated_at
    ON pharos_tenant_aggregates (tenant_id, aggregate_type, updated_at);

ALTER TABLE pharos_tenant_aggregates ENABLE ROW LEVEL SECURITY;

-- The session setting the policy reads to scope every query. Set it once per
-- connection checkout (or per request, if you pool connections) from
-- whatever carries the current tenant in your application — the framework's
-- own `TenantContext`/`CURRENT_TENANT` task-local, if you're using it.
--
-- The `NULLIF` is what makes "no tenant in scope" deny-by-default instead of
-- exposing every tenant's rows: with the setting empty, the cast yields NULL,
-- and `tenant_id = NULL` matches nothing.
--
--   SELECT set_config('app.tenant_id', $1, false); -- $1 = '' when no tenant is in scope

CREATE POLICY tenant_isolation ON pharos_tenant_aggregates
    USING      (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

-- Same compare-and-swap shape as the single-tenant table above, with
-- `tenant_id` added to both the conflict target and the WHERE clause:
--
--   INSERT INTO pharos_tenant_aggregates
--       (tenant_id, aggregate_type, aggregate_id, payload, version, updated_at)
--   VALUES ($1, $2, $3, $4::jsonb, $5, $6)
--   ON CONFLICT (tenant_id, aggregate_type, aggregate_id) DO NOTHING;
--
--   UPDATE pharos_tenant_aggregates
--   SET payload = $4::jsonb, version = $5, updated_at = $6
--   WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3 AND version = $expected;


-- ── Transactional outbox + idempotent inbox ─────────────────────────────────
-- Pairs with: TransactionalStore + TransactionalRepository (outbox insert) —
--             pharos_testing::contract::transactional
--             OutboxRepository — pharos_testing::contract::outbox
--             InboxStore — pharos_testing::contract::inbox
--
-- The outbox row and its aggregate's row must commit in the same database
-- transaction (that's the whole point of the pattern: an event is durable
-- if and only if the state change that raised it is). `seq` is what a
-- dispatcher orders and paginates by, not `created_at`: `created_at` is a
-- producer's wall clock, and two producers' clocks are never guaranteed to
-- agree on "same instant" the way one `GENERATED ALWAYS AS IDENTITY`
-- sequence does.

CREATE TABLE IF NOT EXISTS pharos_outbox (
    id              UUID PRIMARY KEY,
    message_id      UUID NOT NULL,
    topic           TEXT NOT NULL,
    message_key     TEXT NULL,
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload         BYTEA NOT NULL,
    content_type    TEXT NOT NULL,
    status          TEXT NOT NULL
        CHECK (status IN ('pending', 'published', 'failed', 'dead_lettered')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT NULL,
    seq             BIGINT GENERATED ALWAYS AS IDENTITY
);
CREATE INDEX IF NOT EXISTS idx_pharos_outbox_pending_seq
    ON pharos_outbox (seq)
    WHERE status = 'pending';

-- The claim: atomically selects up to `limit` due pending rows and leases
-- them by pushing `next_attempt_at` into the future, in one statement, so
-- two concurrent dispatchers never both receive the same row. This is
-- `pharos_testing::contract::outbox::no_double_claim`'s exact guarantee.
--
-- Both `now()` calls come from Postgres, not the dispatcher process's clock:
-- two dispatchers with skewed system clocks still agree on what the
-- database calls "now", so neither can see the other's still-live lease as
-- already expired.
--
--   UPDATE pharos_outbox
--   SET next_attempt_at = now() + $2::interval, updated_at = now()
--   WHERE id IN (
--       SELECT id FROM pharos_outbox
--       WHERE status = 'pending' AND next_attempt_at <= now()
--       ORDER BY seq ASC
--       LIMIT $1
--       FOR UPDATE SKIP LOCKED
--   )
--   RETURNING id, message_id, topic, message_key, headers, payload, content_type,
--             status, attempts, created_at, updated_at, next_attempt_at, last_error, seq;
--   -- `UPDATE ... RETURNING` does not guarantee its output preserves the
--   -- claim subquery's `ORDER BY seq` — re-sort the returned rows by `seq`
--   -- on the way out.

CREATE TABLE IF NOT EXISTS pharos_inbox (
    message_id  UUID NOT NULL,
    consumer    TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('processing', 'completed', 'failed')),
    received_at TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    last_error  TEXT NULL,
    PRIMARY KEY (message_id, consumer)
);
CREATE INDEX IF NOT EXISTS idx_pharos_inbox_status_updated_at
    ON pharos_inbox (status, updated_at);

-- `begin_processing`'s whole `IdempotencyDecision` state machine in one
-- statement: insert if never seen, touch `updated_at` if it already exists
-- (a caller-side staleness check on `updated_at` then decides
-- AlreadyProcessing vs. a stale-lease takeover), and report via
-- `xmax = 0` whether this call was the one that inserted the row —
-- Postgres's own way of saying "did my INSERT win or did the ON CONFLICT
-- branch run instead", without a second round trip.
--
--   INSERT INTO pharos_inbox (message_id, consumer, status, received_at, updated_at, last_error)
--   VALUES ($1, $2, 'processing', now(), now(), NULL)
--   ON CONFLICT (message_id, consumer)
--   DO UPDATE SET updated_at = pharos_inbox.updated_at
--   RETURNING status, updated_at, (xmax = 0) AS inserted, now() AS db_now;


-- ── Dead-letter queue ────────────────────────────────────────────────────────
-- Pairs with: DeadLetterQueue — pharos_testing::contract::dead_letter

CREATE TABLE IF NOT EXISTS pharos_dead_letter (
    id               UUID PRIMARY KEY,
    message_id       UUID NOT NULL,
    topic            TEXT NOT NULL,
    message_key      TEXT NULL,
    headers          JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload          BYTEA NOT NULL,
    content_type     TEXT NOT NULL,
    reason           TEXT NOT NULL,
    attempts         INTEGER NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pharos_dead_lettered_at
    ON pharos_dead_letter (dead_lettered_at DESC);


-- ── Saga instances ───────────────────────────────────────────────────────────
-- Pairs with: SagaStore + SagaTimeoutStore —
--             pharos_testing::contract::saga::{run, timeout}
--
-- `version` is the same compare-and-swap discipline as the aggregate
-- repositories above, applied to saga state: `SagaStore::save` must refuse
-- with `SagaSaveError::ConcurrencyConflict` whenever the row's stored
-- version has moved since the caller loaded it — including on create
-- (`expected == 0` against a row that already exists is a conflict too,
-- not just updates).

CREATE TABLE IF NOT EXISTS pharos_sagas (
    saga_type   TEXT NOT NULL,
    saga_id     TEXT NOT NULL,
    state       JSONB NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    deadline_at TIMESTAMPTZ NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    version     BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (saga_type, saga_id)
);
CREATE INDEX IF NOT EXISTS idx_pharos_sagas_due
    ON pharos_sagas (saga_type, deadline_at)
    WHERE status = 'running' AND deadline_at IS NOT NULL;

-- `SagaStore::save` — the same INSERT ... ON CONFLICT DO NOTHING /
-- UPDATE ... WHERE version = $expected pair as the aggregate repositories,
-- with `version + 1` computed client-side and bound as `$new_version`
-- either way:
--
--   -- expected == 0 (create):
--   INSERT INTO pharos_sagas (saga_type, saga_id, state, status, deadline_at, updated_at, version)
--   VALUES ($1, $2, $3::jsonb, $4, $5, $6, $new_version)
--   ON CONFLICT (saga_type, saga_id) DO NOTHING;
--
--   -- expected > 0 (update):
--   UPDATE pharos_sagas
--   SET state = $3::jsonb, status = $4, deadline_at = $5, updated_at = $6, version = $new_version
--   WHERE saga_type = $1 AND saga_id = $2 AND version = $expected;
--
-- `SagaTimeoutStore::claim_due` — the same claim-then-lease shape as the
-- outbox's `pending`, with the CTE's `due.deadline_at` (the *original*,
-- elapsed deadline) returned to the caller while the row itself moves to
-- the lease. Bumping `version` too means a `save` racing this claim for the
-- same saga arbitrates through the exact same compare-and-swap `save`
-- already uses — the loser gets `ConcurrencyConflict` instead of silently
-- clobbering the claim's lease.
--
--   WITH due AS (
--       SELECT saga_type, saga_id, deadline_at FROM pharos_sagas
--       WHERE saga_type = $1 AND status = 'running'
--         AND deadline_at IS NOT NULL AND deadline_at <= $2
--       ORDER BY deadline_at
--       LIMIT $3
--       FOR UPDATE SKIP LOCKED
--   )
--   UPDATE pharos_sagas p
--   SET deadline_at = $4, version = p.version + 1
--   FROM due
--   WHERE p.saga_type = due.saga_type AND p.saga_id = due.saga_id
--   RETURNING p.saga_id, p.state, p.status, due.deadline_at AS deadline_at, p.updated_at, p.version;
--   -- $4 = now + lease. Re-sort the output by the original `deadline_at` on
--   -- the way out: `UPDATE ... FROM` does not guarantee output order.


-- ── Event streams + snapshots (event sourcing) ──────────────────────────────
-- Pairs with: EventStore — pharos_testing::contract::event_store::run
--             SnapshotStore — pharos_testing::contract::event_store::snapshot_store
--
-- `tenant_id` defaults to the nil UUID so a single-tenant application never
-- has to think about it; a multi-tenant one sets it per row like the
-- aggregate repository above (RLS is equally applicable here — add the same
-- policy shape if you need it; omitted here to keep this section focused on
-- the append-only/optimistic-concurrency shape event sourcing itself needs).
--
-- `event_type` is nullable on purpose: a row written before this column
-- existed has no retroactive way to know its event type, and `EventStore`
-- implementations should treat a NULL as "no upcasting opinion" rather than
-- failing to load old history.

CREATE TABLE IF NOT EXISTS pharos_event_streams (
    tenant_id      UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    stream_type    TEXT NOT NULL,
    stream_id      TEXT NOT NULL,
    sequence       BIGINT NOT NULL,
    payload        JSONB NOT NULL,
    recorded_at    TIMESTAMPTZ NOT NULL,
    event_type     TEXT NULL,
    schema_version INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, stream_type, stream_id, sequence)
);

CREATE TABLE IF NOT EXISTS pharos_snapshots (
    tenant_id      UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    stream_type    TEXT NOT NULL,
    stream_id      TEXT NOT NULL,
    payload        JSONB NOT NULL,
    version        BIGINT NOT NULL,
    taken_at       TIMESTAMPTZ NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, stream_type, stream_id)
);

-- `EventStore::append`'s optimistic concurrency: `expected_version` must
-- equal the stream's current highest `sequence` (0 for a never-appended
-- stream). A stale caller's INSERT collides on the primary key instead of
-- silently interleaving with a concurrent writer's events — catch the
-- unique-violation and report it as `RepositoryError::ConcurrencyConflict`
-- with the stream's actual current sequence as `actual`.
--
--   INSERT INTO pharos_event_streams
--       (tenant_id, stream_type, stream_id, sequence, payload, recorded_at, event_type, schema_version)
--   VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8);
--   -- $4 = expected_version + 1, expected_version + 2, ... one row per event,
--   -- in the same transaction. A unique-violation on the primary key means
--   -- another writer's append landed first between your read and this write.
--
-- `SnapshotStore::save` replaces unconditionally — it has no concurrency
-- contract of its own; the event store's own optimistic concurrency on the
-- underlying stream is what guards against a stale writer:
--
--   INSERT INTO pharos_snapshots
--       (tenant_id, stream_type, stream_id, payload, version, taken_at, schema_version)
--   VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
--   ON CONFLICT (tenant_id, stream_type, stream_id) DO UPDATE
--   SET payload = $4::jsonb, version = $5, taken_at = $6, schema_version = $7;
