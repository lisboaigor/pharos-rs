# Benchmark baseline

Measured with Criterion on 2026-08-30 on a local macOS development machine.

```bash
cargo bench -p pharos-benches --bench event_bus --bench outbox_dispatcher
```

## Results

| Benchmark                                  |         Mean time | Derived throughput | What it actually measures                              |
| ------------------------------------------- | -----------------: | ------------------: | -------------------------------------------------------- |
| `event_bus_publish_no_handlers`             |          29.733 ns |      33.6M events/s | A `publish` call against a `TypeId` with **zero registered handlers** — the cost of the `HashMap` miss, not of dispatching to a handler. |
| `event_bus_publish_one_handler`             |          92.772 ns |      10.8M events/s | One handler registered and invoked per publish.          |
| `dispatch_pending_100_messages_in_memory`   | 67.891 µs per 100 msgs |     1.47M msgs/s | `dispatch_batch()` only, against `pharos-memory`'s `DashMap`-backed outbox. Setup (repo/broker construction, seeding) runs before the timer starts. |

Throughput formulas:

- event bus: `1 / mean_time_seconds`
- outbox batch: `100 / mean_time_seconds`

The `event_bus_publish_one_handler` figure moved from a previously published
90.119 ns to 92.772 ns between baselines; treat single-digit-percent drift
between runs on different hardware as noise, not regression.

## Why there's no adapter-backed number here anymore

Earlier baselines of this document included a PostgreSQL-backed
`dispatch_pending_100_messages_postgres` number
(~79.940 ms per 100 msgs, ~1.25k msgs/s), measured against
`pharos-postgres`'s `PgOutboxRepository`. `pharos-rs` no longer ships that
crate — see `docs/guide/writing-an-adapter.md` — so there is no longer a
framework-provided adapter for this repository to benchmark against. The
in-memory number above still measures exactly what it always did
(`OutboxRepository::pending`'s claim-then-dispatch path against `DashMap`)
and is still useful as an upper bound and for regression tracking on the
dispatcher logic itself, but it is **not** representative of what your own
adapter will do: a real database round-trips over the network, and its
`pending()` claim's cost depends entirely on your schema, indexes, and
connection pool — not on anything `pharos-rs` controls.

If you need a throughput number for capacity planning, benchmark your own
`OutboxRepository` implementation with `cargo bench` (or a load test)
against the same `dispatch_batch()` call this file benchmarks, using
representative data volumes and your production connection pool settings.
`docs/guide/reference-schema.sql`'s outbox claim query (`FOR UPDATE SKIP
LOCKED`) is the one this project's own PostgreSQL numbers were measured
against, so it remains a reasonable starting point if you're implementing
against Postgres specifically.

## Operational envelope

These numbers describe the dispatcher's own overhead, not your adapter's:

**Latency floor.** The framework has no polling loop or default interval; the
end-to-end delay from "event enqueued" to "event published" is bounded below
by whatever interval the caller polls `dispatch_batch()` at — there is no
built-in push path (an adapter backed by e.g. Postgres `LISTEN`/`NOTIFY`, or
`pharos-app`'s in-process `OutboxSignal` for same-process writer/dispatcher
pairs, can close this gap; neither is on by default). A caller polling every
100ms has a ~100ms floor regardless of how fast `dispatch_batch()` itself
runs.

**Scaling.** `OutboxRepository::pending`'s contract requires the claim to be
atomic and to lease claimed rows (see `pharos_testing::contract::outbox::
no_double_claim`), so multiple dispatcher instances against a correctly
implemented adapter claim disjoint batches and their per-instance throughput
is roughly additive — but see the [decision matrix](decision-matrix.md) and
the framework's own README for the caveat that per-key ordering is not
preserved across concurrent dispatchers. Raising `concurrency` above 1
parallelizes publishes within one dispatcher's batch but does not batch the
underlying claim/update statements unless your adapter overrides
`insert_many`/`publish_batch` to actually batch them.

**This is not an ingestion pipeline.** The outbox path is sized for domain
events at business-transaction rates (order placed, payment captured), not
high-frequency telemetry. A workload needing 10⁴–10⁵ messages/second needs
batched writes (`insert_many`/`publish_batch`, overridden by your own
adapter) or a separate ingestion path outside the outbox entirely.

## Scope and caveats

- This is a single-machine, in-process baseline, useful for regression
  tracking on the dispatcher's own logic — not a substitute for benchmarking
  your own adapter, or for load testing against your own hardware and
  network topology.
- Re-run on your own hardware before using any of this as an external SLO
  commitment.
