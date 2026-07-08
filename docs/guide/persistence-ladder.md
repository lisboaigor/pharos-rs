# The persistence ladder

The same aggregate climbs three steps as your system matures. Nothing in the
domain model changes between steps — only the composition root.

## Step 1 — In-memory (tests, prototypes)

```rust
let repo = InMemoryRepository::<Order>::new();
save_and_publish(&repo, &bus, &mut order).await?;
```

Free, instant, and enough to design the whole domain. Every step after this is
a swap in `main.rs`.

## Step 2 — PostgreSQL JSONB (first deployment)

```rust
let pool = connect_pool(url, 16)?;
let repo = PostgresJsonRepository::<Order>::with_aggregate_type(pool.clone(), "order");
repo.migrate().await?;

// Atomic aggregate + outbox in one transaction:
save_and_enqueue_in(&pool, &repo, &mut order, map_event).await?;
```

One generic table (`pharos_aggregates`), full optimistic concurrency, and the
transactional outbox — with zero SQL written. This step carries most systems
further than expected; move on only when you need relational queries over the
aggregate's _insides_.

## Step 3 — Explicit relational tables (production scale)

Write your schema and your SQL; keep the framework contracts:

1. Implement `Repository<Order>` with normalized tables (see
   `examples/order/src/infrastructure/postgres_order_repository.rs`).
2. Extract the body of `save` into `save_in_tx(&self, conn, aggregate)` and
   implement `TransactionalRepository<Order>` with it — the OCC contract is
   the same, only the connection is now caller-provided.
3. Keep calling **the same** `save_and_enqueue_in(&pool, &repo, ...)` as in
   step 2: the atomic save+outbox guarantee follows the trait, not the table
   layout.

The step-2 → step-3 migration is a data migration (JSONB rows → normalized
rows) plus a new repository type in the composition root. Handlers, domain
model, events, and the outbox pipeline are untouched.
