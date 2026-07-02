# Production Guide

The production checklist lives in [docs/guide/production.md](../../guide/production.md).

Use it when you are deciding between:

- `save_and_publish` and `save_and_enqueue`
- `PostgresUnitOfWork` and a simpler repository-only flow
- single-tenant and `TenantContext`/`TenantJsonRepository`
- broker adapters such as Redis, Kafka, and NATS
