# Decision Matrix

The "which path should I use?" guide lives in [docs/guide/decision-matrix.md](../../guide/decision-matrix.md).

Use it when you need a default (and the conditions to deviate) for:

- the feature bundle (`starter` vs `full` vs individual flags)
- persistence (in-memory, JSON repository, normalized schema, multi-tenant)
- event delivery (`save_and_publish` vs outbox)
- broker, consumer idempotency, and HTTP exposure
