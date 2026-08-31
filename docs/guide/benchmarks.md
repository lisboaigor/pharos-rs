# Benchmark baseline

Measured with Criterion on 2026-08-30 on a local macOS development machine.

```bash
cargo bench -p pharos-benches --bench event_bus --bench outbox_dispatcher
cargo bench -p pharos-benches --bench outbox_dispatcher_postgres   # needs Docker
```

## Results

| Benchmark                                  |         Mean time | Derived throughput | What it actually measures                              |
| ------------------------------------------- | -----------------: | ------------------: | -------------------------------------------------------- |
| `event_bus_publish_no_handlers`             |          29.733 ns |      33.6M events/s | A `publish` call against a `TypeId` with **zero registered handlers** — the cost of the `HashMap` miss, not of dispatching to a handler. |
| `event_bus_publish_one_handler`             |          92.772 ns |      10.8M events/s | One handler registered and invoked per publish.          |
| `dispatch_pending_100_messages_in_memory`   | 67.891 µs per 100 msgs |     1.47M msgs/s | `dispatch_batch()` only, against `pharos-memory`'s `DashMap`-backed outbox. Setup (repo/broker construction, seeding) runs before the timer starts. |
| `dispatch_pending_100_messages_postgres`    | 79.940 ms per 100 msgs |     1.25k msgs/s | `dispatch_batch()` only, against a real PostgreSQL container: one atomic claim statement, two `UPDATE`s per message, and one publish per message, sequential (`concurrency: 1`). Setup excluded from the timer the same way. |

Throughput formulas:

- event bus: `1 / mean_time_seconds`
- outbox batch: `100 / mean_time_seconds`

The `event_bus_publish_one_handler` figure moved from a previously published
90.119 ns to 92.772 ns between baselines; treat single-digit-percent drift
between runs on different hardware as noise, not regression.

## What changed from the previous baseline

The previous table (dated 2026-06-21) had three problems, all fixed here:

1. **`no_handlers` was published as general dispatch throughput.** It only
   ever measured the fast-path `HashMap` miss when no handler is registered
   for the event type — the name now says so in the "what it measures"
   column, and the number should not be read as "events processed per
   second" for a system with handlers.
2. **The outbox benchmark timed its own setup.** Constructing the in-memory
   repository/broker and inserting 100 messages ran *inside* the timed
   closure, alongside `dispatch_batch()` itself. Moving setup outside the
   timer dropped the reported time from ~230 µs to ~68 µs for the same
   workload — the earlier number measured insert-then-dispatch, not dispatch.
3. **There was no PostgreSQL benchmark at all.** `dispatch_pending_100_messages_in_memory`
   was the only outbox number published, backed by a `DashMap`, with zero
   PostgreSQL calls in the whole `pharos-benches` dependency tree. It said
   nothing about the path every production deployment actually takes. The
   `outbox_dispatcher_postgres` benchmark above closes that gap.

## Operational envelope

Use the PostgreSQL number, not the in-memory one, to reason about capacity:
**~1.2k messages/second per `OutboxDispatcher` instance**, with the defaults
(`batch_size: 100`, `concurrency: 1`, no batching on `insert`/`publish`).
That number comes from one claim `UPDATE` plus two more `UPDATE`s and one
publish call per message, all sequential — `1 + 2·batch_size` statements and
`batch_size` publishes per dispatch cycle.

**Latency floor.** The framework has no polling loop or default interval; the
end-to-end delay from "event enqueued" to "event published" is bounded below
by whatever interval the caller polls `dispatch_batch()` at — there is no
`LISTEN`/`NOTIFY` push path. A caller polling every 100ms has a ~100ms floor
regardless of how fast `dispatch_batch()` itself runs.

**Scaling.** `pending()`'s claim uses `FOR UPDATE SKIP LOCKED`, so multiple
dispatcher instances against the same table claim disjoint batches and their
per-instance throughput is roughly additive — but see the
[decision matrix](decision-matrix.md) and the framework's own README for the
caveat that per-key ordering is not preserved across concurrent dispatchers.
Raising `concurrency` above 1 parallelizes publishes within one dispatcher's
batch but does not currently batch the underlying `INSERT`/`UPDATE`
statements.

**This is not an ingestion pipeline.** With these numbers, the outbox path is
sized for domain events at business-transaction rates (order placed, payment
captured), not high-frequency telemetry. A workload needing 10⁴–10⁵
messages/second needs batched writes (`insert_many`/`publish_batch`, not yet
implemented — see the project backlog) or a separate ingestion path outside
the outbox entirely.

## Scope and caveats

- These are single-machine baselines (in-process for the event bus, a local
  Docker container for the PostgreSQL outbox), useful for regression
  tracking and back-of-envelope capacity planning — not a substitute for load
  testing against your own hardware and network topology.
- The PostgreSQL benchmark does not include broker latency: the publisher
  used is `pharos-memory`'s in-process broker, isolating the Postgres cost.
  A real Kafka/NATS/Redis publisher adds its own round trip per message.
- Re-run on your own hardware before using any of this as an external SLO
  commitment.
