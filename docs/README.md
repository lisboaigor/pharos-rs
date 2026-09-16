# Documentation Index

This folder is the canonical documentation entrypoint for contributors and adopters.

## Start here

- `../README.md` for project overview and crate map.
- `guide/30-minutes.md` for the fastest path to a working aggregate.
- `guide/decision-matrix.md` to pick persistence, event delivery, and transport
  without early decision fatigue.
- `guide/complete-usage.md` for an end-to-end usage guide.

## Decision guides

- `guide/decision-matrix.md`: which path should I use? Defaults for every axis.
- `guide/cookbook.md`: copy-paste templates (command handler, transactional
  save + enqueue, idempotent consumer, tenant propagation, HTTP route).
- `guide/pitfalls.md`: common mistakes and their fixes.

## Guides

- `guide/30-minutes.md`: fast tutorial from zero to a working aggregate.
- `guide/complete-usage.md`: complete usage walkthrough, from local development to production.
- `guide/architecture.md`: event-driven model, outbox/inbox patterns, diagrams, and limitations.
- `guide/crates.md`: per-crate API reference with tables and examples.
- `guide/production.md`: deployment and operations checklist.
- `guide/observability.md`: tracing, metrics, OTLP wiring examples.
- `guide/jsonb-schema-rollback.md`: rollback playbook for JSONB payload changes.
- `guide/writing-an-adapter.md`: implement `Repository`/`TransactionalStore` (and
  the rest of the storage/messaging traits) against your own database, with
  SeaORM as the running example, verified by `pharos-testing`'s conformance kit.
- `guide/reference-schema.sql`: reference PostgreSQL schema (copyable, not
  code the framework runs) each conformance suite in `pharos-testing::contract`
  is proven against.
- `guide/benchmarks.md`: benchmark baseline and interpretation.
- `guide/ergonomics-review.md`: ergonomics assessment with recommendations.
- `guide/migrating-0.4-to-0.5.md`: what broke going to 0.5.0 and how to update.
- `guide/migrating-0.6-to-0.7.md`: what broke going to 0.7.0 (storage/broker
  crates removed, `pharos-init` removed) and how to update.

## Architecture Decisions

- `adr/ADR-001-optimistic-concurrency-control.md`
- `adr/ADR-002-eventbus-concrete-struct.md`
- `adr/ADR-005-postgresql-connection-pool-sqlx.md`
- `adr/ADR-016-observability-instrumentation.md`

## RFC process

- `rfc/README.md`: how to propose changes.
- `rfc/0000-template.md`: RFC template.

## Documentation maintenance policy

- Keep conceptual explanations in `docs/guide`.
- Keep decision history in `docs/adr`.
- Keep process and governance under root docs (`CONTRIBUTING.md`, `SUPPORT.md`, RFC docs).
- Keep runnable examples as source-of-truth for behavior under `examples/` and integration tests.
- Treat duplicated docs as stale by default; prefer linking to one canonical page.
