# Ecosystem

Available crates in the workspace:

- `pharos-memory` for in-process storage/broker adapters (tests, local dev)
- `pharos-axum` for HTTP route integration with Axum
- `pharos-saga` for long-lived workflows and process managers
- `pharos-es` for event-sourced aggregates and append-only event stores
- `pharos-realtime` for WebSocket/pub-sub fan-out delivery
- `pharos-observability` for metrics, logs, and traces wiring
- `pharos-proto` for Protobuf integration-event serialization

Pharos RS ships no database or broker crate. For Kafka, NATS, Redis,
PostgreSQL, or anything else, implement the relevant trait
(`MessagePublisher`/`MessageConsumer`/`MessageAcknowledger`, `Repository`,
`OutboxRepository`, …) yourself — see
[Writing an adapter](https://github.com/lisboaigor/pharos-rs/blob/main/docs/guide/writing-an-adapter.md).

The design rule stays the same: application/domain crates do not depend on transport crates; adapters sit at the edge.
