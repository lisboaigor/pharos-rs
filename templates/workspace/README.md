# Pharos Workspace Template

This template is the starting point for a new Pharos-based service.

## Layout

- `crates/domain` for aggregates and domain events
- `crates/application` for commands, queries, and orchestration
- `crates/api` for the entrypoint and transport adapters

## Quick start

```bash
cargo build --workspace
cargo test --workspace
cargo run -p api
```
