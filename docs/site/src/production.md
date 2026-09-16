# Production Guide

The production checklist lives in [docs/guide/production.md](../../guide/production.md).

Use it when you are deciding between:

- `save_and_publish` and `save_and_enqueue`
- the atomic `save_and_enqueue_in` (against your own `TransactionalStore`) and a simpler repository-only flow
- single-tenant and a tenant-scoped, row-isolated repository
- which broker adapter to implement (Redis, Kafka, NATS, …) against Pharos's messaging traits
