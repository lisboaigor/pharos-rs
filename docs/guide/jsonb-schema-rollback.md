# JSONB schema rollback guide

This guide describes a safe rollback strategy for payload changes stored in
`pharos_aggregates.payload` and `pharos_tenant_aggregates.payload`.

## Principles

- Prefer roll-forward over hard rollback when possible.
- Use additive migrations first, then remove legacy fields only after a full
  compatibility window.
- Keep aggregate `version` as the OCC source of truth; do not reset versions.

## Safe deployment sequence

1. Expand:
   - Add new optional fields in readers/writers.
   - Keep old fields readable.
2. Dual-write:
   - Write both old and new fields in aggregate payload serialization.
3. Flip-read:
   - Start reading the new field first, fall back to old.
4. Contract:
   - Remove old field writes only after all services are updated.

## Emergency rollback playbook

If a deployment with payload changes must be rolled back:

1. Stop writers for the affected bounded context.
2. Deploy the previous binary that can read both payload forms.
3. If needed, run a targeted SQL patch to restore expected keys.
4. Resume traffic and monitor `RepositoryError::Serialization` and
   `RepositoryError::IdParsing` rates.

## SQL patch template

```sql
BEGIN;

-- Example: restore `customer_name` from `customer.full_name`
UPDATE pharos_aggregates
SET payload = jsonb_set(
    payload,
    '{customer_name}',
    to_jsonb(payload #>> '{customer,full_name}'),
    true
)
WHERE aggregate_type = 'order'
  AND payload ? 'customer'
  AND NOT (payload ? 'customer_name');

UPDATE pharos_tenant_aggregates
SET payload = jsonb_set(
    payload,
    '{customer_name}',
    to_jsonb(payload #>> '{customer,full_name}'),
    true
)
WHERE aggregate_type = 'order'
  AND payload ? 'customer'
  AND NOT (payload ? 'customer_name');

COMMIT;
```

## Verification checklist

- Read-path integration tests pass on both old and new payload shapes.
- `cargo test --workspace --all-features -- --test-threads=1` passes.
- No increase in dead letters caused by payload incompatibility.
- OCC conflict rate remains in expected bounds.
