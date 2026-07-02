# Getting Started

1. Pick the entry point.
   - `pharos` if you want the convenience prelude and feature flags.
   - focused crates if you want minimal dependencies.
2. Model aggregates with `Entity`, `AggregateRoot`, and `DomainEvent`.
3. Persist them through `Repository`.
4. Decide whether the side effect is in-process (`save_and_publish`) or external (`save_and_enqueue`).
5. Add infrastructure adapters only when you need them.

Useful feature flags on `pharos`:

- `postgres`
- `redis`
- `axum`
- `saga`
- `es`
- `kafka`
- `nats`
- `tower`
