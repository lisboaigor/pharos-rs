# Ecosystem

Available crates in the workspace:

- `pharos-axum` for HTTP route integration with Axum
- `pharos-saga` for long-lived workflows and process managers
- `pharos-es` for event-sourced aggregates and append-only event stores
- `pharos-kafka` for Kafka messaging and remote schema registries (Confluent and Apicurio)
- `pharos-nats` for core NATS messaging

The design rule stays the same: application/domain crates do not depend on transport crates; adapters sit at the edge.
