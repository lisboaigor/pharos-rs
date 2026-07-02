# Contributing to Pharos RS

Thanks for your interest in improving Pharos RS. This guide covers the basics
for getting a change merged.

## Getting started

```bash
git clone https://github.com/lisboaigor/pharos-rs
cd pharos-rs
cargo build --workspace
cargo test --workspace
```

The container-backed integration tests are a first-class part of the suite and
are **never** `#[ignore]`d — they exercise the real PostgreSQL/Redis behavior
(optimistic concurrency, the atomic aggregate+outbox transaction, tenant
isolation, inbox idempotency) that in-memory adapters cannot. They use
`testcontainers` to spin up an ephemeral PostgreSQL or Redis instance per test
and tear it down automatically when the test finishes, so **running the test
suite requires a running Docker daemon**.

```bash
cargo test --workspace --all-features
# or, to bound concurrent containers on a small machine:
cargo test-docker   # = cargo test --workspace --all-features -- --test-threads=1
```

CI runs the full suite, including the container tests, on every push and pull
request via `.github/workflows/ci.yml`.

## Before opening a pull request

Every change should keep the workspace green:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
```

- Add or update tests for the behavior you change. In-memory adapters and the
  `pharos-testing` crate make this cheap.
- Update `CHANGELOG.md` under `[Unreleased]` for any user-visible change.
- Keep doc comments focused on how to use a type; design rationale belongs in
  `docs/`, not in the public API docs.
- Breaking design changes and new top-level crates should go through the RFC
  process documented in `docs/rfc/README.md`.

## Design principles

Pharos is **Rust first**. When in doubt, prefer:

- explicit wiring over hidden magic (no dependency-injection container, no
  reflection);
- concrete types over trait objects on hot paths;
- borrowing over allocating;
- the existing ecosystem (Tokio, Tower, Tracing) over reinventing it.

These principles keep the library small, fast, and predictable.

### Error handling law

**`unwrap()` is banned in library and application code.** The Rust core team
itself now discourages `unwrap()` outside of genuine proof-by-inspection cases
because it converts recoverable errors into unrecoverable panics, making
failures invisible to callers and impossible to handle gracefully.

| Context                        | Rule                                                                                                                                                                                                                              |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Library crate (`pharos-*`)     | **Never** use `.unwrap()`. Propagate with `?`. Define a crate-local error type or re-export a shared one.                                                                                                                         |
| Application / integration code | **Never** use `.unwrap()`. Use `?` up to the entry point; convert to a concrete error there.                                                                                                                                      |
| Tests (`#[cfg(test)]`)         | Prefer `?` on `async` test functions. `.unwrap()` is tolerated **only** on values whose construction is trivially infallible (e.g. `Utc.with_ymd_and_hms(…).single().unwrap()` in fixture setup), never on the result under test. |
| Examples                       | Use `?` and a boxed error (`Box<dyn std::error::Error>`).                                                                                                                                                                         |

**Alternatives to reach for:**

```rust
// Instead of:                       // Use:
value.unwrap()                        value?
option.unwrap()                       option.ok_or(MyError::Missing)?
result.unwrap_or_else(|e| panic!())   return Err(e.into())
```

`expect()` is acceptable only when the message explains the **invariant that
guarantees the value is present** — not just what crashed. It is still
prohibited in library code on fallible operations.

Clippy will catch most violations via the `clippy::unwrap_used` lint, which is
enabled workspace-wide in `.cargo/config.toml`.

## Crate boundaries

- `pharos-core` — pure domain primitives. New external dependencies here are a
  red flag.
- `pharos-app` — application contracts (CQRS, event bus, outbox/inbox, messaging),
  plus the optional Tower adapters (`tower` feature).
- `pharos-infra` — in-memory adapters only.
- `pharos-postgres` — pooled PostgreSQL adapters.
- `pharos-redis` — Redis messaging adapter.
- `pharos-axum` — Axum integration.
- `pharos-saga` — saga/process-manager primitives.
- `pharos-es` — event-sourcing primitives.
- `pharos-kafka` — Kafka + remote schema-registry adapters.
- `pharos-nats` — NATS messaging adapters.
- `pharos-testing` — test helpers.
- `pharos` — convenience meta-crate.

Adapter crates depend on `app`/`core`; never the other way.

## Documentation and RFCs

- The dedicated docs site source lives under `docs/site/` and is built with `mdbook build docs/site`.
- The 30-minute tutorial lives in `docs/guide/30-minutes.md` and should stay runnable against the current API.
- The starter workspace template lives in `templates/workspace/` and should compile when copied into a fresh folder.
- RFCs live in `docs/rfc/`; use `docs/rfc/0000-template.md` as the starting point.

## Licensing

By contributing, you agree that your contributions are dual-licensed under
MIT OR Apache-2.0, matching the project license.
