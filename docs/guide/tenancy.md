# Tenancy: keeping the tenant across tasks, jobs, messages and crossings

`pharos-app` (feature `tenant-task-local`) carries the current tenant in the
`CURRENT_TENANT` task-local. The task-local does not follow you everywhere. These
helpers cover the places it does not, and the one place you cross the boundary
on purpose.

| Situation | Use |
|---|---|
| A task you spawn from a request must keep the tenant | `spawn_scoped` |
| A background job must visit every tenant | `TenantSource` + `for_each_tenant` |
| A consumer handles a message on another task or node | `with_message_scope` (pair of `TenantHeader`) |
| Code must act across tenants, or as one tenant, on the record | `Elevator` |

## `spawn_scoped`

`tokio::spawn` starts with empty task-locals. Work handed to it silently loses the
tenant, and under a deny-by-default store that means zero rows or a refused write
with nothing pointing at the spawn. `spawn_scoped` reads the scope where it is
called and reopens it around the future. With no tenant in scope, the task has
none, as with `tokio::spawn`.

It carries **only the tenant**. An elevation (below) is not inherited: a task
started from an elevated request has to ask for its own.

## `for_each_tenant`

The loop every per-tenant background job writes by hand: list the tenants, then
for each one open the scope and run. Written by hand it goes wrong in two ways: a
call that forgets the scope, and one tenant's error aborting the pass for the rest.

```rust
let report = for_each_tenant(&my_tenants, |tenant| async move {
    escalate_alerts(tenant).await        // Result<(), E>
}).await?;                               // Err only if the tenants could not be listed
tracing::info!(ok = report.succeeded, failed = report.failed.len());
```

You implement `TenantSource` (a query on your tenants table, a fixed list in a
test). Listing every tenant is itself not scoped to one, so if your store needs an
elevation for it, put that inside the `TenantSource`, not in every job. Tenants are
visited one at a time, in the order the source gives them.

## `with_message_scope`

`TenantHeader` stamps the tenant on an outgoing message. `with_message_scope` is
the consumer side: it reopens that tenant around the handling.

- No header: the work runs with no scope, as the message was produced.
- Header present but not a valid tenant id: `Err`, and the work **does not run**.
  Running it unscoped would look like a message from no tenant. Let the caller retry
  or dead-letter it.

## Elevation: crossing the boundary on purpose

A few callers legitimately cross tenants: a back-office console, a worker draining
a queue shared by all tenants, a lookup made before anyone signs in. A global
"platform" flag any code can switch on turns isolation into a convention.
Instead, ask an `Elevator`:

```rust
const BACKOFFICE: Purpose = Purpose::new("backoffice");
const QUEUE: Purpose = Purpose::new("worker:queue");
const SUPPORT: Purpose = Purpose::new("support");

let policy = AllowPurposes::new()
    .allow(BACKOFFICE).requiring_actor(BACKOFFICE)
    .allow(QUEUE)
    .allow_impersonation(SUPPORT);
let elevator = Elevator::new(policy, my_audit_sink);

// Across tenants:
elevator.elevated(QUEUE, None, drain_queue()).await?;

// As one tenant, with the actor on the record:
elevator.impersonating(customer, SUPPORT, "ana", fix_their_data()).await?;
```

- `Purpose` is `&'static str`: the purposes are a closed list in your code, never
  built from a request.
- An `Elevation` has no public constructor. It only exists inside work run by an
  `Elevator`, whose `ElevationPolicy` decided.
- **Every request is recorded, granted or denied**, through your `ScopeAuditSink`.
  It is synchronous and cannot fail, so recording never decides whether the work
  runs. A sink that writes to a database hands entries to a channel; it also
  decides how much to record (a console every entry, a worker one line per
  interval).
- `elevated` is cross-tenant: your storage adapter reads `current_elevation()` and
  lifts its tenant filter. `impersonating` is narrower: the work runs inside the
  tenant's ordinary scope and nothing is lifted; only the actor is on the record.
  An impersonation always names its actor.
- A refused request returns `ElevationDenied` and the work does not run.

Strategies shipped: `DenyAll`, `AllowPurposes` (policies); `NoAudit`,
`MemoryAudit` (sinks, the latter for tests). Write your own by implementing
`ElevationPolicy` / `ScopeAuditSink`.

## What is deliberately not here

- No storage adapter: how a connection learns the tenant (a session setting, a
  schema, a database) stays in your adapter; see
  [`writing-an-adapter.md`](writing-an-adapter.md).
- No concurrency in `for_each_tenant` yet. Add it when a job needs it.

Adopting these in an existing application: [migrating to the tenancy helpers](migrating-to-tenancy-helpers.md).
