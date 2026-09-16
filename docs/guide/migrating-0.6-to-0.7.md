# Migrating from 0.6 to 0.7

0.7.0 finishes decoupling Pharos RS from any specific storage or broker
technology. Four changes touch public API shape or the workspace layout;
the rest (native `async fn` in trait definitions, the cross-layer error
classification contract) are additive. This page covers all six.

## 1. `pharos-postgres`, `pharos-redis`, `pharos-kafka`, and `pharos-nats` are gone

These four crates no longer exist in the workspace. If you depended on any
of them, port to your own adapter against the same traits — see
[Writing an adapter](writing-an-adapter.md) (SeaORM 2.0 as the running
example) and [`reference-schema.sql`](reference-schema.sql) (a copyable
PostgreSQL schema covering every table these crates used to own: aggregate
repository, tenant-scoped RLS variant, outbox, inbox, dead-letter, saga
instances, event streams, snapshots).

The most common case — a JSONB aggregate repository plus the atomic
save+outbox transaction — looks like this before and after:

```toml
# before
pharos-postgres = { git = "https://github.com/lisboaigor/pharos-rs", rev = "..." }

# after
sea-orm = { version = "2", features = ["sqlx-postgres", "runtime-tokio-rustls"] }
# your own crate/module implementing Repository<A> + TransactionalStore +
# TransactionalRepository<A, YourStore> against it
```

```rust
// before
use pharos_postgres::{PostgresJsonRepository, PostgresUnitOfWork, connect_pool};

let pool = connect_pool(url, 16).await?;
let repo = PostgresJsonRepository::<Order>::with_aggregate_type(pool.clone(), "order");
let store = PostgresUnitOfWork::new(pool.clone());
save_and_enqueue_in(&store, &repo, &mut order, map_event).await?;

// after — same call shape, your own types underneath
let repo = MyOrderRepository::new(db.clone());   // implements Repository<Order>
let store = MyUnitOfWork::new(db.clone());       // implements TransactionalStore
                                                  // + TransactionalRepository<Order, Self>
save_and_enqueue_in(&store, &repo, &mut order, map_event).await?;
```

`save_and_enqueue_in` and the `TransactionalStore`/`TransactionalRepository`
traits did not change — they were already backend-agnostic before 0.7, this
release just removes the one PostgreSQL implementation that used to ship
alongside them. Follow [Writing an adapter](writing-an-adapter.md) for the
full walkthrough (entity definition, `Repository` impl, `TransactionalStore`
impl, and verifying both against `pharos-testing`'s conformance kit before
you trust them).

For messaging adapters (`MessagePublisher`/`MessageConsumer`/
`MessageAcknowledger`), `OutboxRepository`, `InboxStore`, `DeadLetterQueue`,
`EventStore`/`SnapshotStore`, and `SagaStore`/`SagaTimeoutStore`, the same
pattern applies: implement the trait against your broker/storage, verify
with `pharos-testing`'s `contract` feature.

## 2. `pharos-init` is removed

The scaffolding CLI is gone — its decision matrix hard-coded
PostgreSQL/Redis/Kafka choices for most profiles, which no longer matches a
framework that ships no storage/broker dependency. Scaffold a new project
manually instead; [30 minutes](30-minutes.md) walks through the same
`cargo new` → domain model → persistence → outbox path `pharos-init` used
to generate for you, and the observability stack config it used to
template (Prometheus/Loki/Tempo/Grafana) is your own infrastructure's
concern to wire — see [Observability](observability.md).

## 3. Most trait methods are now native `async fn`

Every trait in `pharos-core`, `pharos-app`, `pharos-messaging`, `pharos-es`,
`pharos-saga`, and `pharos-realtime` that used to declare its methods as
`fn foo(...) -> impl Future<Output = T> + Send` now declares them as plain
`async fn foo(...) -> T`, via `#[trait_variant::make(Send)]`. The trait's
public shape (name, methods, `+ Send` bound on the returned future) is
unchanged — this is a source-compatible rewrite for anyone who only
`.await`s these methods or implements the traits with `async fn` bodies,
which is the overwhelming majority of call sites.

It is **not** transparent if you:

- named the trait's associated `impl Future<...>` return type explicitly
  (e.g. in a generic bound or a struct field) — name the method's `async
  fn` return type instead, or box the future yourself;
- wrote a blanket `impl<T: SomeTrait> AnotherTrait for T` that relied on the
  old manual desugaring — re-check it compiles against the new native
  `async fn` signature.

Six traits kept the old manual `-> impl Future<Output = T> + Send { async
move { .. } }` form for one **default (provided) method** each, because
`#[trait_variant::make(Send)]` only rewrites a *bodyless* method's
signature — a provided method's body passes through unchanged while its
`asyncness` is stripped, which breaks a body written as `async fn` with a
bare top-level `.await`. These are unaffected by this migration (their
signature never changed):

- `OutboxRepository::insert_many` and `OutboxRepository::schedule_retry`
- `EventStore::load_after`
- `Saga::on_timeout`
- `RealtimeSubscriber::subscribe_since`
- `MessagePublisher::publish_batch`

## 4. New: cross-layer error classification (`pharos_core::classify`)

Optional to adopt — nothing breaks if you ignore it — but recommended.
`ErrorKind` (`NotFound`, `Validation`, `Conflict`, `Unauthorized`,
`Forbidden`, `RateLimited`, `Unavailable`, `Internal`) and the
`ClassifiedError` trait (`kind()` + `public_message()`) give every layer a
shared, closed vocabulary for "what HTTP status is this?" and "what can I
safely tell the client?", without leaking adapter-specific detail (a raw
`sqlx::Error`, a connection string, a constraint name) across the domain →
application → transport boundary.

`DomainError` and `RepositoryError<E>` implement it in `pharos-core`;
`ValidationError`, `StoreError`, `ApplicationError`, and any
`DispatchError<E: ClassifiedError>` implement it in `pharos-app`.
`pharos-axum` exposes `status_for(kind: ErrorKind) -> StatusCode` and
`HandlerError::from_classified(&impl ClassifiedError)`:

```rust
impl IntoResponse for MyError {
    fn into_response(self) -> Response {
        HandlerError::from_classified(&self).into_response()
    }
}
```

The rule for your own `ClassifiedError` impls, if you write one: anything
wrapping an untrusted or adapter-specific error must return a **fixed**
string from `public_message()` — never `self.to_string()` or the wrapped
cause's `Display`. That fixed string is the whole point of the trait; see
`RepositoryError::Storage`'s impl in `pharos-core` for the reference shape.

## Everything else

Non-breaking: the `DefaultAggregateStore<A, R, S>` type in `pharos-app`
(extracted from what used to be `PostgresAggregateStore`, now generic over
any `Repository<A> + TransactionalRepository<A, S>` and
`S: TransactionalStore`), `pharos-memory`'s new `InMemoryUnitOfWork`,
`InMemorySagaStore`, `InMemoryEventStore`, and `InMemorySnapshotStore`
(behind the `es`/`saga` features, default-on), and `pharos-testing`'s new
`contract` feature (adapter conformance kit — off by default, add it when
writing your own adapter). None of these require call-site changes.
