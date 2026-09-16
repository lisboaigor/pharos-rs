# Getting Started

1. Pick the entry point.
   - `pharos` if you want the convenience prelude and derive macros.
   - focused crates (`pharos-core`, `pharos-app`, …) if you want minimal
     dependencies.
2. Model aggregates with `Entity`, `AggregateRoot`, and `DomainEvent`.
3. Persist them through `Repository` — `pharos-memory`'s `InMemoryRepository`
   for local dev/tests, your own adapter for production (see
   [Writing an adapter](https://github.com/lisboaigor/pharos-rs/blob/main/docs/guide/writing-an-adapter.md)).
4. Decide whether the side effect is in-process (`save_and_publish`) or external (`save_and_enqueue`).
5. Add crates only when you need them — `pharos-axum` for HTTP,
   `pharos-saga` for sagas, `pharos-es` for event sourcing,
   `pharos-realtime` for WebSocket fan-out.

Useful features:

- `macros` (on the `pharos` facade — derives and `id_type!`)
- `tower` (on `pharos-app` — the cross-cutting pipeline seam)
- `retry` (on `pharos-app` — the `Retrying` event-handler decorator)
- `tenant-task-local` (on `pharos-app` — task-local tenant propagation)

There is no `postgres`/`redis`/`kafka`/`nats` feature flag: Pharos RS ships
no database or broker dependency. Bring your own by implementing the
relevant trait.
