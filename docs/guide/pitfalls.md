# Pitfalls

Common mistakes when building on Pharos, and the fix for each. Most are the flip
side of a default recommended in the [decision matrix](decision-matrix.md).

## Publishing to a broker directly from a command handler

**Symptom:** events occasionally go missing after a deploy or crash.

**Why:** if you save the aggregate, commit, then publish to Kafka/Redis/NATS in
the handler, a crash between commit and publish loses the event with no record
that it ever existed.

**Fix:** use the outbox. Persist the aggregate and the outbox rows in the same
transaction (`pharos::postgres::save_aggregate_and_enqueue`), and let an
`OutboxDispatcher` move them to the broker. See the
[cookbook](cookbook.md#command-handler-with-transactional-save--enqueue).

## Forgetting that consumers see duplicates

**Symptom:** double-charged customers, duplicate side effects.

**Why:** every broker Pharos supports is at-least-once. Redelivery is normal,
not exceptional.

**Fix:** wrap non-idempotent consumers in an `InboxStore`
(`begin_processing` → handle → `mark_completed`/`mark_failed`). Treat
`AlreadyCompleted`/`AlreadyProcessing` as a no-op skip.

## Ignoring `ConcurrencyConflict`

**Symptom:** lost updates under concurrent writes to the same aggregate.

**Why:** repositories use optimistic concurrency control. `save` advances a
version and returns `ApplicationError::ConcurrencyConflict` (or
`RepositoryError::ConcurrencyConflict`) when the stored version moved underneath
you.

**Fix:** surface the conflict to the caller (HTTP 409) or reload-and-retry the
command. Never swallow it.

## Not draining events before saving by hand

**Symptom:** events published twice, or never.

**Why:** `save_and_publish` / `save_and_enqueue` already drain the aggregate's
pending events as part of the call. If you also drain manually, or save the
aggregate through the bare `Repository::save` and expect events to fire, the
counts will be wrong.

**Fix:** go through `save_and_publish` / `save_and_enqueue` (or the postgres
transactional helper). Use bare `Repository::save` only for aggregates with no
events.

## Losing the tenant across an async boundary

**Symptom:** a query returns another tenant's rows, or none.

**Why:** the tenant is not ambient. If a handler derives or defaults the tenant
instead of receiving it, isolation breaks.

**Fix:** thread `TenantContext` explicitly from the edge through the application
layer into the adapter, and `stamp` outgoing integration events. With
PostgreSQL, use `TenantJsonRepository` so isolation is enforced at the row
level. See the [cookbook](cookbook.md#tenant-propagation).

## Reaching for a normalized schema too early

**Symptom:** heavy mapping code and migrations before there is a query that
needs them.

**Why:** the JSON repository stores the aggregate as a document and needs no
per-field schema. A normalized relational schema only pays off when you query
across columns, enforce foreign keys, or report on the same tables.

**Fix:** start with the JSON repository; introduce a hand-written normalized
`Repository` (as in `examples/order`) when a real relational query appears.

## Enabling more features than you ship

**Symptom:** slow builds, large binaries, confusing surface area.

**Why:** the `full` bundle compiles every adapter (Kafka, NATS, ES, saga, …).

**Fix:** use `full` for exploration only. For a service, use `starter` or list
the exact flags you depend on. See [Choosing features](decision-matrix.md#feature-bundle).

## Running JSONB schema changes without a rollback plan

**Symptom:** a payload-shape change breaks reads of already-stored documents.

**Fix:** follow `guide/jsonb-schema-rollback.md` before changing a stored JSON
payload.

## See also

- [Decision matrix](decision-matrix.md)
- [Cookbook](cookbook.md)
- [Production guide](production.md)
