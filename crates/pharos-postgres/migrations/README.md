# pharos-postgres migrations

Versioned SQL history for the built-in PostgreSQL schemas.

Apply in numeric order:

1. 0001_eventing.sql
2. 0002_aggregates.sql
3. 0003_tenant_aggregates.sql
4. 0004_dead_letter.sql
5. 0005_event_store.sql
6. 0006_sagas.sql
7. 0007_event_store_tenant_id.sql
8. 0008_outbox_dead_lettered_and_next_attempt_at.sql
9. 0009_tenant_aggregates_uuid.sql
10. 0010_sagas_version.sql
11. 0011_snapshots_schema_version.sql
12. 0012_outbox_seq.sql
13. 0013_event_streams_event_type_and_schema_version.sql

Notes:
- These files mirror the schema constants in `pharos-postgres` and are intended
  for production migration tools (sqlx migrate, refinery, Flyway, Liquibase).
- Keep these SQL files additive and backward-compatible.
- For aggregate payload contract changes, follow the rollback procedure in
  `docs/guide/jsonb-schema-rollback.md`.
- 0008–0011 close a drift that had crept in between these files and the
  `pharos-postgres` schema constants: applying only 0001–0007 built a
  database several production queries (the outbox dead-letter path,
  tenant/aggregate id lookups on `pharos_tenant_aggregates`, saga
  reads/writes) failed against at runtime. A
  regression test (`crates/pharos-postgres/tests/docker_integration.rs`,
  `migrations_directory_matches_the_schema_constants_used_at_runtime`)
  applies this exact file chain against a container and then exercises the
  same code paths the drift broke, so a future drift fails CI instead of
  reaching production.
