# pharos-postgres migrations

Versioned SQL history for the built-in PostgreSQL schemas.

Apply in numeric order:

1. 0001_eventing.sql
2. 0002_aggregates.sql
3. 0003_tenant_aggregates.sql
4. 0004_dead_letter.sql

Notes:
- These files mirror the schema constants in `pharos-postgres` and are intended
  for production migration tools (sqlx migrate, refinery, Flyway, Liquibase).
- Keep these SQL files additive and backward-compatible.
- For aggregate payload contract changes, follow the rollback procedure in
  `docs/guide/jsonb-schema-rollback.md`.
