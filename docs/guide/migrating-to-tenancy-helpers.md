# Migrating to the tenancy helpers

This release adds tenancy helpers to `pharos-app` (feature `tenant-task-local`).
**Nothing breaks.** Every addition is new API, so an application that does not
adopt them keeps building and behaving exactly as before, and can stay on the
revision it is pinned to. This page is for the ones that adopt, and for deciding
whether and when to.

What is new (see [tenancy](tenancy.md) for what each does):

| Addition | Replaces |
|---|---|
| `spawn_scoped` | a `tokio::spawn` whose task reads the tenant |
| `TenantSource` + `for_each_tenant` | the hand-written "list tenants, open scope, run" loop |
| `TENANT_HEADER`, `tenant_of`, `with_message_scope` | a consumer that rebuilds the tenant from a message header by hand |
| `Elevator`, `Purpose`, `ElevationPolicy`, `ScopeAuditSink` | a global "platform" flag that lifts tenant filters |
| `reference-schema.sql`: event tables | the nil-UUID default on `pharos_event_streams` / `pharos_snapshots` |

One change is not additive, and only for people who **copied** the reference
schema (step 5): the two event tables lost their nil-UUID default and gained RLS.
The framework never ran that SQL, so nothing changes until you copy it again.

## Order

The steps are independent and each can ship alone. Do them in this order, cheapest
and lowest-risk first; stop wherever the value stops.

| Step | Risk | Behaviour change |
|---|---|---|
| 1. `for_each_tenant` | low | one tenant's error no longer stops the pass |
| 2. `spawn_scoped` | low | none, unless the task was relying on having no tenant |
| 3. `with_message_scope` | low | a malformed tenant header is refused instead of ignored |
| 4. Elevation | medium | the cross-tenant flag goes away; a purpose not in the policy is refused |
| 5. Reference schema | medium | only if you copied the event tables |

## 1. `for_each_tenant`

Find the loops:

```
grep -rnE "for .* in .*tenants|list_all|\.all\(\)" src | grep -v test
```

For each, implement `TenantSource` once (whatever lists your tenants, including
any elevation it needs) and replace the loop body with a closure returning
`Result<(), E>`.

Two things change, both deliberate. A tenant whose work fails is reported in
`ForEachReport::failed` and the pass **continues** (a hand loop that used `?`
stopped at the first failure). And the scope is opened by the helper, so the body
can no longer forget it. If a job did depend on stopping at the first failure,
check `report.failed` afterwards.

Verify: a test with a source of three tenants, the middle one failing, asserts the
other two ran and the failure is reported.

## 2. `spawn_scoped`

`tokio::spawn` drops the tenant. Find the spawns whose task reads it, directly or
through a repository:

```
grep -rn "tokio::spawn\|JoinSet" src | grep -v test
```

Most spawns in an application are long-lived workers that open their own scope per
item; those need nothing. Change only the ones that are started from a scoped
request and rely on it, to `spawn_scoped`. `spawn_scoped` carries the tenant and
**not an elevation** (step 4): a task must ask for its own.

Verify: a test that spawns from inside a tenant scope and reads the tenant in the
task. It fails with `tokio::spawn` and passes with `spawn_scoped`.

## 3. `with_message_scope`

Where a consumer rebuilds the tenant from the `tenant_id` header, replace it with
`with_message_scope(&message, work)`.

The behaviour to decide on: a header that is present but not a valid tenant id
returns `Err` and the work **does not run**. A hand-written consumer that parsed
with `.ok()` ran such a message with no scope, where a deny-by-default store then
saw nothing. If you would rather keep the old behaviour for a transition, map the
`Err` to `tenant_of(...).ok().flatten()` yourself; but a corrupt stamp is better
sent to the retry/dead-letter path than guessed at.

Verify: three messages (stamped, unstamped, malformed): the work runs in the
tenant's scope, with none, and not at all.

## 4. Elevation

The largest step, and the one where your design matters. The goal is to replace a
global "platform" switch with named, audited crossings.

### 4.1 Inventory

List every place that crosses tenants today: `grep` for the flag or helper. Group
the callers by **why**:

- a person operating a console (needs an actor);
- a worker on a queue or table shared by all tenants (no actor);
- a lookup made before a tenant is known (a domain, a device);
- a command-line tool.

Each group is a `Purpose`. Define them together, in one module, as constants; the
list is closed on purpose.

### 4.2 Choose the kind of crossing

| If the caller... | Use |
|---|---|
| needs to see rows of many tenants at once (a shared queue, a directory) | `elevated` |
| acts on behalf of one tenant (support fixing a customer's data) | `impersonating` (narrower: nothing is lifted, the actor is on the record) |

Prefer `impersonating` wherever the caller really is working on one tenant.

### 4.3 Policy and sink

- Build an `AllowPurposes` policy from your purposes. Mark the person-driven ones
  `requiring_actor`.
- Start with a sink that logs. A durable audit trail can come later, as a sink that
  hands entries to a channel and writes from its own task. Decide per purpose how
  much to record: a console every entry, a worker that elevates several times a
  second one line per interval.
- Install the `Elevator` once at startup and expose one function to your code
  (`elevated(purpose, actor, work)`), so call sites do not carry the elevator.

### 4.4 Storage adapter

Wherever your adapter used to read the old flag to lift its tenant filter, read
`current_elevation()` and lift **only when both hold**:

- the kind is `ElevationKind::CrossTenant` (an impersonation runs inside the
  tenant's ordinary scope and must lift nothing), **and**
- `your_elevator.issued(&elevation)` is true.

The second check is not optional. An `Elevation` cannot be built by hand, but any
code can build its own `Elevator` with a policy that grants everything and obtain a
valid one. An adapter that lifts for any elevation it finds has only moved the flag.
Keep one elevator for the application and accept nothing else. A test support crate
that brings its own elevator has to be introduced to the adapter explicitly, behind
a feature that production builds never enable.

### 4.5 Call sites

Each call site now returns `Result<_, ElevationDenied>` (outer) around whatever it
returned before. The request is decided and recorded when `elevated` is *called*, with
the line that made it, not when the returned future is first polled. Two patterns cover almost everything:

```rust
// a function returning Result<T, MyError> where MyError: From<ElevationDenied>
let rows = elevated(purposes::QUEUE, None, fetch()).await??;

// a caller with no error channel for it: treat a denial as a bug, not a runtime case
```

A denial is a **programming error** (a purpose missing from the policy, an actor not
named), never a runtime condition; give it a variant in your error type rather than
handling it.

### 4.6 Tests

Tests must not borrow a production purpose. Give them their own elevator with a
purpose that exists only in the test crate (a few lines, shared through your test-support crate), so
the production policy has nothing a test could use. Replace the test-side helper
in bulk: it is a mechanical change to imports.

### 4.7 Verify

- A test that the production policy **refuses** an unknown purpose and a missing actor.
- Against a real database: a cross-tenant elevation sees every tenant's rows; an
  impersonation sees only one tenant's; no elevation and no tenant sees none.
- A test that a spawned task does not inherit the elevation.

### 4.8 Rollout

Ship the module and the call-site changes with the **default logging sink** first.
Watch the `elevated` / `elevation refused` log lines for a release: a refusal in
production means a caller missing from the policy. Only then add the durable sink
or tighten anything.

Rollback is reverting the call sites; the pharos revision can stay.

## 5. Reference schema (only if you copied the event tables)

The reference `pharos_event_streams` and `pharos_snapshots` used to default
`tenant_id` to the nil UUID, so a write that forgot its tenant landed in a shared
bucket. Now the column has no default and the tables have the same RLS policy as
the aggregate table.

Before changing your copy, check whether anything relies on the nil tenant:

```sql
SELECT count(*) FROM pharos_event_streams  WHERE tenant_id = '00000000-0000-0000-0000-000000000000';
SELECT count(*) FROM pharos_snapshots      WHERE tenant_id = '00000000-0000-0000-0000-000000000000';
```

- **Zero rows, multi-tenant application:** apply the change.

  ```sql
  ALTER TABLE pharos_event_streams ALTER COLUMN tenant_id DROP DEFAULT;
  ALTER TABLE pharos_snapshots     ALTER COLUMN tenant_id DROP DEFAULT;
  ALTER TABLE pharos_event_streams ENABLE ROW LEVEL SECURITY;
  ALTER TABLE pharos_snapshots     ENABLE ROW LEVEL SECURITY;
  -- then the two tenant_isolation policies from reference-schema.sql
  ```

- **Rows under the nil tenant:** either the application is single-tenant (keep the
  default and skip RLS, as the schema's comment now describes), or those rows are
  data written by a missing tenant scope, which needs cleaning up first. Do not
  enable RLS over them until you know which.

RLS binds only a role that is not the table owner and has no `BYPASSRLS`; check the
application role before relying on it.

A catalog check worth keeping in your test suite, so a new tenant table cannot ship
without isolation:

```sql
-- every ordinary table with a tenant_id column must have RLS on and a policy
SELECT c.relname FROM pg_class c
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'tenant_id' AND NOT a.attisdropped
WHERE c.relkind = 'r' AND c.relnamespace = 'public'::regnamespace
  AND (NOT c.relrowsecurity
       OR NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid));
```

Zero rows is the pass condition.

## Staying on the previous revision

Nothing forces adoption. An application pinned to a revision before this release
keeps working; the only thing it does not get is the helpers. When it does adopt,
the steps above apply unchanged, and the cheap ones (1 to 3) need no design
decision.

One application has already adopted all of steps 1 to 4 (Geofencer). For scale:
18 production call sites and 51 test call sites moved from a global flag to named
purposes, the test-side change being an import swap, and no test needed a
production purpose.

## What is not covered here

- A tenant-tree abstraction (a parent seeing its children). Applications with one
  keep it in their schema and policies for now.
- A generic cross-repository leak test suite. The catalog check above and a
  per-tenant read test cover the ground meanwhile.
- Concurrency in `for_each_tenant`; tenants are visited one at a time.
