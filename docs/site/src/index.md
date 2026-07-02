# Pharos RS

Pharos RS is a Rust-first toolkit for domain-driven, CQRS-friendly, event-driven services.

The workspace is intentionally split:

- `pharos-core` for pure domain primitives
- `pharos-app` for application contracts and orchestration helpers
- adapters such as `pharos-postgres`, `pharos-redis`, `pharos-kafka`, and `pharos-nats`
- optional higher-level crates such as `pharos-axum`, `pharos-saga`, and `pharos-es`

Start with the meta-crate when you want one dependency, or depend on the focused crates directly when you need tighter control.
