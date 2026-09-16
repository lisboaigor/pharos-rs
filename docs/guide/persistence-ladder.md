# The persistence ladder

The same aggregate climbs two steps as your system matures. Nothing in the
domain model changes between steps — only the composition root.

## Step 1 — In-memory (tests, prototypes)

```rust
let repo = InMemoryRepository::<Order>::new();
save_and_publish(&repo, &bus, &mut order).await?;
```

Free, instant, and enough to design the whole domain. `pharos-memory` ships
`InMemoryRepository`, `InMemoryOutboxRepository`, `InMemoryInboxStore`, and
`InMemoryUnitOfWork` — the same seam a real backend implements.

## Step 2 — Your own adapter (first deployment)

Pharos RS ships no database, broker, or ORM dependency. Implement
`Repository<Order>` against whichever storage you choose — a JSON-document
table, a normalized relational schema, or an ORM like SeaORM — and, for the
atomic save+outbox guarantee, `TransactionalStore` +
`TransactionalRepository<Order, Store>`:

```rust
let repo = MyOrderRepository::new(pool.clone());
let store = MyUnitOfWork::new(pool.clone());

// Atomic aggregate + outbox in one transaction:
save_and_enqueue_in(&store, &repo, &mut order, map_event).await?;
```

[Writing an adapter](writing-an-adapter.md) walks through this end to end
against SeaORM 2.0, and [`reference-schema.sql`](reference-schema.sql) has a
copyable starting schema — a generic JSON-document table
(`pharos_aggregates`), its row-level-secured multi-tenant variant
(`pharos_tenant_aggregates`), and the outbox/inbox/dead-letter/saga/
event-store tables. Prove your adapter correct against `pharos-testing`'s
`contract` conformance kit (`contract::repository`,
`contract::transactional`, …) instead of hoping it's right.

`save_and_enqueue_in` and the `TransactionalStore`/`TransactionalRepository`
traits it composes live in `pharos-app` and never name a concrete connection
type — the same composing function works against any backend that
implements them, whether your `Store::Tx` is a `sqlx::Transaction`, a
SeaORM `DatabaseTransaction`, or anything else.

Whether you start with a JSON-document repository (least ceremony, one
generic table, full optimistic concurrency and transactional outbox with no
per-aggregate schema) or go straight to normalized relational tables depends
on whether you need relational queries, foreign keys, or reporting against
the aggregate's *insides* from day one — see the
[decision matrix](decision-matrix.md#persistence). Moving from one shape to
the other later is a data migration plus a new repository type in the
composition root: handlers, domain model, events, and the outbox pipeline
are untouched, because they only ever depend on `Repository<A>` and
`TransactionalStore`/`TransactionalRepository`, never on the table layout.
