# Running Pharos RS in production

This guide collects the moving parts you need to wire and the decisions you need
to make before putting a Pharos RS service into production. It assumes you have
read the README and built something with the in-memory adapters.

## 1. Pick the right write path

Pharos gives you three ways to persist an aggregate and surface its events.
Choose per use case:

| Helper                | What it does                                                                | Use when                                                                                                    |
| ---------------------- | ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `save_and_publish`    | Saves the aggregate, then publishes events on the in-process `EventBus`.    | All event handlers run in the same process.                                                                 |
| `save_and_enqueue`    | Saves the aggregate, then inserts events into an outbox.                    | Events must reach an external broker, but the save and the outbox insert can be separate statements.        |
| `save_and_enqueue_in` | Saves the aggregate **and** inserts the outbox rows in **one transaction**, against your `TransactionalStore`. | You need the strongest guarantee: a crash can never persist an aggregate without its events, or vice versa. |

For money, inventory, or anything where a lost or duplicated event causes a real
incident, use the transactional `save_and_enqueue_in` — which means your
storage adapter needs to implement `TransactionalStore` +
`TransactionalRepository`; see [Writing an adapter](writing-an-adapter.md).

## 2. Use a connection pool, sized deliberately

Build one connection pool and share it (cloning is cheap) across every
adapter that needs it. Size the pool to your database's connection budget,
not your request concurrency — requests queue for a pooled connection rather
than opening new ones. A common starting point is `2 × CPU cores` per
service instance, then tune from metrics.

Configure TLS on the pool yourself — Pharos does not build or own the
connection pool; that belongs to your adapter (SeaORM's `ConnectOptions`, or
whichever client library you chose).

## 3. Enforce optimistic concurrency

Every aggregate carries a `version`. Any correct `Repository::save`
implementation only writes when the expected version matches the stored
one, returning `RepositoryError::ConcurrencyConflict` otherwise —
`pharos-testing`'s `contract::repository` suite verifies this for your
adapter. Handle that error by reloading the aggregate and retrying the use
case — never by ignoring it. This is what prevents lost updates under
concurrent writers.

## 4. Run the outbox dispatcher

Publishing from the outbox is a background job. Configure it with
`DispatchConfig`:

```rust
use std::time::Duration;
use pharos_app::{DispatchConfig, OutboxDispatcher, RetryPolicy};

let config = DispatchConfig::new(
    200, // batch size per run
    RetryPolicy::exponential(8, Duration::from_millis(200), 2.0, Duration::from_secs(30)),
);
let dispatcher = OutboxDispatcher::with_config(outbox, publisher, config);

// In a background loop, on your chosen interval:
let result = dispatcher.dispatch_batch().await;
```

- A failed publish keeps the message `pending` until its retry budget is
  exhausted, then marks it `failed` for a dead-letter sweep.
- One poisoned message never blocks the rest of the batch.
- Running several dispatchers concurrently safely is your `OutboxRepository`
  implementation's job: `pending` must atomically claim rows (e.g.
  `FOR UPDATE SKIP LOCKED` in a relational adapter) and lease them so two
  dispatchers never take the same batch — see `reference-schema.sql`'s
  claim query and `contract::outbox::no_double_claim`.

Schedule a periodic cleanup of `published` outbox rows so the table does not
grow without bound.

## 5. Make consumers idempotent

At-least-once delivery means consumers will occasionally see a message twice.
Wrap processing in the inbox:

1. `begin_processing(message_id, consumer)` returns an `IdempotencyDecision`.
2. Process only on `StartProcessing` or `RetryPreviousFailure`; skip
   `AlreadyProcessing` / `AlreadyCompleted`.
3. Call `mark_completed` or `mark_failed` when done.

Your inbox implementation needs a single atomic upsert on
`(message_id, consumer)`, so two concurrent consumers cannot both start
processing the same message — `contract::inbox` verifies this.

## 6. Transactional boundaries

When several writes must commit together, use your `TransactionalStore`:

```rust
let mut tx = store.begin().await?;
// every statement here commits or rolls back together
store.commit(tx).await?;
```

Compose `save_in_tx` (via `TransactionalRepository`) and
`insert_outbox_in_tx` inside a transaction when you need custom
multi-statement transactions beyond `save_and_enqueue_in`.

## 7. Multi-tenancy

For multi-tenant data, thread a `TenantContext` from the request edge down to
the infrastructure, and build a tenant-scoped repository (per tenant, or
filtering by `tenant_id` on every query). Every read and write is filtered
by `tenant_id`, so one tenant's instance can never read or overwrite another
tenant's rows. See the `examples/multi-tenant` crate and
`reference-schema.sql`'s row-level-security variant.

## 8. Observability

Pharos instruments its operations with `tracing` spans and emits `metrics`
counters (`pharos.outbox.published`, `pharos.outbox.dead_lettered`,
`pharos.event_bus.unknown_topic`, and more — see `pharos-app`/
`pharos-messaging` for the full list). Install a `tracing` subscriber and a
`metrics` recorder/exporter in your binary — `pharos-observability` wires
this for you (see [Observability](observability.md)); Pharos does not choose
a collector for you. Propagate correlation and trace ids through
`IntegrationEvent` so a business flow is traceable across services.

## Production checklist

- [ ] One shared connection pool, sized to the database's connection budget.
- [ ] TLS configured if the database is not on a trusted network.
- [ ] Aggregates use the transactional `save_and_enqueue_in` where event
      loss or duplication would be an incident.
- [ ] `ConcurrencyConflict` is handled with reload-and-retry, never ignored.
- [ ] Outbox dispatcher running with a `DispatchConfig` retry policy.
- [ ] Periodic cleanup of `published` outbox rows.
- [ ] Consumers idempotent through the inbox.
- [ ] Multi-tenant services thread `TenantContext` and filter every query by
      `tenant_id`.
- [ ] `tracing` subscriber and `metrics` exporter installed in the binary
      (`pharos-observability` covers both).
- [ ] Schema installed via your migration tool, not at runtime in production.
- [ ] Payload contract changes for JSON-document aggregates follow
      `docs/guide/jsonb-schema-rollback.md`.
- [ ] Benchmark regressions are tracked against `docs/guide/benchmarks.md`.
- [ ] Your adapter passes `pharos-testing`'s `contract` conformance kit in CI.
