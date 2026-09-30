//! Offensive tests for the tenancy helpers.
//!
//! Written from the point of view of code that wants to escape tenant isolation
//! or forge who did it, using only the public API (this is an integration test,
//! so it sees what an application crate sees). Each test names the attack.

#![cfg(feature = "tenant-task-local")]

use std::sync::Mutex;

use pharos_app::MessageEnricher;
use pharos_app::{
    AllowPurposes, CURRENT_TENANT, ElevationKind, ElevationOutcome, ElevationPolicy,
    ElevationRequest, Elevator, FixedTenants, MemoryAudit, Message, NoAudit, Purpose,
    TENANT_HEADER, TenantContext, TenantHeader, TenantId, current_elevation, for_each_tenant,
    spawn_scoped, tenant_of, with_message_scope,
};
use uuid::Uuid;

const WORKER: Purpose = Purpose::new("worker");
const CONSOLE: Purpose = Purpose::new("console");
const SUPPORT: Purpose = Purpose::new("support");

fn tenant() -> TenantId {
    TenantId::new(Uuid::now_v7())
}

fn current_tenant() -> Option<TenantId> {
    CURRENT_TENANT
        .try_with(|t| t.map(|c| c.tenant_id()))
        .ok()
        .flatten()
}

fn elevator() -> (Elevator, MemoryAudit) {
    let audit = MemoryAudit::new();
    let policy = AllowPurposes::new()
        .allow(WORKER)
        .allow(CONSOLE)
        .requiring_actor(CONSOLE)
        .allow_impersonation(SUPPORT);
    (Elevator::new(policy, audit.clone()), audit)
}

fn message() -> Message {
    Message::new("orders", Vec::new(), "application/json")
}

// ── The elevation must not leak into code that means to be tenant-scoped ────

/// ATTACK: a per-tenant job runs inside an elevation (a back-office handler that
/// calls `for_each_tenant`). Each tenant's work is meant to see only that tenant,
/// but the elevation of the caller would still be in force and lift the filter.
#[tokio::test]
async fn for_each_tenant_inside_an_elevation_runs_each_tenant_unelevated() {
    let (elevator, _) = elevator();
    let tenants = FixedTenants(vec![tenant(), tenant()]);
    let leaked = Mutex::new(0);

    let _ = elevator
        .elevated(WORKER, None, async {
            let _ = for_each_tenant(&tenants, |_| {
                let leaked = &leaked;
                async move {
                    if current_elevation().is_some() {
                        *leaked.lock().unwrap_or_else(|p| p.into_inner()) += 1;
                    }
                    Ok::<(), std::convert::Infallible>(())
                }
            })
            .await;
        })
        .await;

    assert_eq!(
        *leaked.lock().unwrap_or_else(|p| p.into_inner()),
        0,
        "per-tenant work ran with the caller's cross-tenant elevation still active"
    );
}

/// ATTACK: same, through a consumed message.
#[tokio::test]
async fn with_message_scope_inside_an_elevation_runs_the_work_unelevated() {
    let (elevator, _) = elevator();
    let stamped = message().with_header(TENANT_HEADER, tenant().to_string());

    let seen = elevator
        .elevated(WORKER, None, async {
            with_message_scope(&stamped, async { current_elevation() }).await
        })
        .await;

    assert_eq!(
        seen.ok().and_then(Result::ok).flatten(),
        None,
        "the consumer's work inherited the elevation of whoever called it"
    );
}

/// ATTACK: an impersonation says "I am tenant T"; a message for tenant U is then
/// handled inside it. The elevation would claim T while the scope says U.
#[tokio::test]
async fn an_impersonation_does_not_survive_into_another_tenants_message() {
    let (elevator, _) = elevator();
    let (t, u) = (tenant(), tenant());
    let for_u = message().with_header(TENANT_HEADER, u.to_string());

    let seen = elevator
        .impersonating(t, SUPPORT, "ana", async {
            with_message_scope(&for_u, async { (current_tenant(), current_elevation()) }).await
        })
        .await;

    let Ok(Ok((scope, elevation))) = seen else {
        panic!("both calls are allowed");
    };
    assert_eq!(scope, Some(u));
    assert_eq!(
        elevation, None,
        "a stale impersonation of T survived into U's work"
    );
}

/// ATTACK: hope that some way of starting other work hands the elevation on.
#[tokio::test]
async fn no_way_of_starting_other_work_inherits_the_elevation() {
    let (elevator, _) = elevator();

    let results = elevator
        .elevated(WORKER, None, async {
            let scoped = spawn_scoped(async { current_elevation() }).await;
            let plain = tokio::spawn(async { current_elevation() }).await;
            let blocking = tokio::task::spawn_blocking(current_elevation).await;
            let thread = std::thread::spawn(current_elevation).join();
            (scoped, plain, blocking, thread)
        })
        .await;

    let Ok((scoped, plain, blocking, thread)) = results else {
        panic!("granted");
    };
    assert_eq!(scoped.ok().flatten(), None, "spawn_scoped");
    assert_eq!(plain.ok().flatten(), None, "tokio::spawn");
    assert_eq!(blocking.ok().flatten(), None, "spawn_blocking");
    assert_eq!(thread.ok().flatten(), None, "std::thread::spawn");
}

/// ATTACK: two futures interleaved on one task (`join!`): does the elevation of
/// one bleed into the other while they alternate?
#[tokio::test]
async fn an_elevation_does_not_bleed_into_a_sibling_future_on_the_same_task() {
    let (elevator, _) = elevator();

    let (elevated, sibling) = tokio::join!(
        elevator.elevated(WORKER, None, async {
            tokio::task::yield_now().await;
            let seen = current_elevation().is_some();
            tokio::task::yield_now().await;
            seen && current_elevation().is_some()
        }),
        async {
            tokio::task::yield_now().await;
            let seen = current_elevation().is_some();
            tokio::task::yield_now().await;
            seen || current_elevation().is_some()
        }
    );

    assert_eq!(
        elevated.ok(),
        Some(true),
        "the elevated future keeps its own"
    );
    assert!(!sibling, "the sibling future saw the other's elevation");
}

/// ATTACK: nest an elevation to widen what an impersonation may do.
#[tokio::test]
async fn nesting_cannot_widen_beyond_what_the_policy_grants() {
    let (elevator, _) = elevator();
    let t = tenant();

    // Inside an impersonation, a cross-tenant request for a purpose that may
    // only impersonate is refused, and the impersonation is still the scope.
    let outcome = elevator
        .impersonating(t, SUPPORT, "ana", async {
            let widened = elevator.elevated(SUPPORT, None, async {}).await;
            (widened.is_err(), current_elevation().map(|e| e.kind()))
        })
        .await;
    assert_eq!(
        outcome.ok(),
        Some((true, Some(ElevationKind::Impersonation(t))))
    );

    // Inside a cross-tenant elevation, impersonating narrows it: the filter is
    // no longer lifted.
    let narrowed = elevator
        .elevated(WORKER, None, async {
            elevator
                .impersonating(t, SUPPORT, "ana", async {
                    current_elevation().map(|e| e.kind())
                })
                .await
        })
        .await;
    assert_eq!(
        narrowed.ok().and_then(Result::ok).flatten(),
        Some(ElevationKind::Impersonation(t))
    );
}

// ── Forging who did it ──────────────────────────────────────────────────────

/// ATTACK: an actor string that forges extra audit lines or hides in noise.
#[tokio::test]
async fn an_actor_that_could_forge_a_log_line_or_flood_it_is_refused() {
    let (elevator, audit) = elevator();
    let hostile = [
        "ana\n2026-01-01 audit: elevated purpose=backoffice actor=root",
        "ana\r\nforged",
        "ana\u{1b}[31mred",
        "ana\0nul",
    ];
    for actor in hostile {
        assert!(
            elevator
                .elevated(CONSOLE, Some(actor), async {})
                .await
                .is_err(),
            "accepted a control character in the actor: {actor:?}"
        );
    }
    let long = "a".repeat(10_000);
    assert!(
        elevator
            .elevated(CONSOLE, Some(&long), async {})
            .await
            .is_err(),
        "accepted a 10 000 character actor"
    );

    // And none of it reached the sink as a granted entry with the hostile text.
    assert!(
        audit
            .entries()
            .iter()
            .all(|e| e.outcome != ElevationOutcome::Granted),
        "a hostile actor was granted"
    );
    assert!(
        audit.entries().iter().all(|e| e
            .actor
            .as_deref()
            .is_none_or(|a| a.len() <= 256 && !a.chars().any(char::is_control))),
        "a hostile actor string was written to the audit sink verbatim"
    );
}

/// DOCUMENTED LIMIT: a `Purpose` is a label, not a capability. Anyone who can
/// write the name can ask for it; what stops the wrong caller is the policy and
/// the audit trail, not the type. This test pins that, so nobody leans on it.
#[tokio::test]
async fn a_purpose_is_a_label_anyone_can_name() {
    let (elevator, _) = elevator();
    let forged = Purpose::new("worker");

    assert_eq!(forged, WORKER);
    assert!(
        elevator.elevated(forged, None, async {}).await.is_ok(),
        "a same-named purpose is the same purpose"
    );
}

/// ATTACK: a policy that is permissive must still not make an anonymous
/// impersonation possible, and a policy that panics must not grant.
#[tokio::test]
async fn a_misbehaving_policy_cannot_grant_an_anonymous_impersonation_or_a_panic() {
    struct Anything;
    impl ElevationPolicy for Anything {
        fn authorize(&self, _: &ElevationRequest<'_>) -> Result<(), &'static str> {
            Ok(())
        }
    }
    let permissive = Elevator::new(Anything, NoAudit);
    assert!(
        permissive
            .impersonating(tenant(), SUPPORT, "", async {})
            .await
            .is_err()
    );

    struct Panics;
    impl ElevationPolicy for Panics {
        fn authorize(&self, _: &ElevationRequest<'_>) -> Result<(), &'static str> {
            panic!("policy bug")
        }
    }
    let broken = Elevator::new(Panics, NoAudit);
    let ran = std::sync::atomic::AtomicBool::new(false);
    let outcome = tokio::spawn(async move {
        broken
            .elevated(WORKER, None, async {
                ran.store(true, std::sync::atomic::Ordering::SeqCst);
            })
            .await
    })
    .await;
    assert!(
        outcome.is_err(),
        "the panic propagates; nothing was granted"
    );
}

// ── The tenant stamp on a message ───────────────────────────────────────────

/// ATTACK: a header that parses to a tenant other than the one the sender meant,
/// or that slips through as "no tenant".
#[test]
fn hostile_tenant_headers_are_refused_or_canonical() {
    let real = Uuid::now_v7();
    let id = TenantId::new(real);

    // Every spelling of the same UUID is the same tenant (no second identity).
    for spelling in [
        real.to_string(),
        real.to_string().to_uppercase(),
        real.simple().to_string(),
        format!("{{{real}}}"),
        real.urn().to_string(),
    ] {
        let m = message().with_header(TENANT_HEADER, spelling.clone());
        assert_eq!(
            tenant_of(&m).ok().flatten().map(|c| c.tenant_id()),
            Some(id),
            "{spelling}"
        );
    }

    // Anything else is an error, never "no tenant" and never a guess.
    let padded = format!(" {real}");
    let trailing = format!("{real}\n");
    let injected = format!("{real},{}", Uuid::now_v7());
    let huge = "9".repeat(100_000);
    for bad in [
        "",
        " ",
        "acme",
        "null",
        "*",
        "' OR 1=1 --",
        padded.as_str(),
        trailing.as_str(),
        injected.as_str(),
        huge.as_str(),
    ] {
        let m = message().with_header(TENANT_HEADER, bad);
        assert!(tenant_of(&m).is_err(), "accepted {bad:.40?}");
    }
}

/// DOCUMENTED LIMIT: the nil UUID is a valid tenant id. A single-tenant
/// application uses it as its one tenant, so the framework cannot reject it; a
/// multi-tenant one must not let it exist as a tenant (see the migration guide).
#[test]
fn the_nil_uuid_parses_as_a_tenant() {
    let m = message().with_header(TENANT_HEADER, Uuid::nil().to_string());
    assert_eq!(
        tenant_of(&m).ok().flatten().map(|c| c.tenant_id()),
        Some(TenantId::new(Uuid::nil()))
    );
}

/// ATTACK: a message arrives already carrying a forged `tenant_id`, and the
/// producer stamps it from its own scope: the real tenant must win.
#[tokio::test]
async fn the_producers_stamp_overwrites_a_forged_header() {
    let (real, forged) = (tenant(), tenant());
    let mut m = message().with_header(TENANT_HEADER, forged.to_string());

    CURRENT_TENANT
        .scope(Some(TenantContext::new(real)), async {
            TenantHeader.enrich(&mut m);
        })
        .await;

    assert_eq!(
        tenant_of(&m).ok().flatten().map(|c| c.tenant_id()),
        Some(real)
    );
}

// ── A failing tenant, cancellation ──────────────────────────────────────────

/// DOCUMENTED LIMIT: an `Err` from one tenant does not stop the pass, but a
/// panic does (it unwinds through the pass). What it must never do is leave a
/// scope or an elevation behind.
#[tokio::test]
async fn a_panic_in_one_tenant_aborts_the_pass_and_leaves_no_scope_behind() {
    let tenants = FixedTenants(vec![tenant(), tenant(), tenant()]);
    let visited = std::sync::Arc::new(Mutex::new(0));
    let v = visited.clone();

    let outcome = tokio::spawn(async move {
        let _ = for_each_tenant(&tenants, |_| {
            let v = v.clone();
            async move {
                let mut n = v.lock().unwrap_or_else(|p| p.into_inner());
                *n += 1;
                if *n == 2 {
                    panic!("tenant work bug");
                }
                Ok::<(), std::convert::Infallible>(())
            }
        })
        .await;
    })
    .await;

    assert!(outcome.is_err(), "the panic reaches the caller");
    assert_eq!(*visited.lock().unwrap_or_else(|p| p.into_inner()), 2);
    assert_eq!(current_tenant(), None);
    assert_eq!(current_elevation(), None);
}

// ── Who granted it, and who asked ───────────────────────────────────────────

/// ATTACK: code that cannot build an `Elevation` builds an `Elevator` that
/// grants everything and uses its elevation. The adapter must be able to tell
/// that it is not the application's own.
#[tokio::test]
async fn an_elevation_minted_by_a_foreign_elevator_is_not_the_adapters() {
    let (legit, _) = elevator();
    let attacker = Elevator::new(AllowPurposes::new().allow(WORKER), NoAudit);

    let forged = attacker
        .elevated(WORKER, None, async { current_elevation() })
        .await;
    let Ok(Some(forged)) = forged else {
        panic!("the attacker's own elevator grants it");
    };
    let real = legit
        .elevated(WORKER, None, async { current_elevation() })
        .await;
    let Ok(Some(real)) = real else {
        panic!("granted");
    };

    assert!(
        !legit.issued(&forged),
        "accepted another elevator's elevation"
    );
    assert!(legit.issued(&real));
    assert!(attacker.issued(&forged) && !attacker.issued(&real));
    // A clone is the same elevator.
    assert!(legit.clone().issued(&real));
}

/// ATTACK: a purpose is only a name, so the trail must say which code asked.
#[tokio::test]
async fn the_audit_records_which_line_asked_even_when_refused() {
    let (elevator, audit) = elevator();

    let _ = elevator.elevated(WORKER, None, async {}).await; // granted
    let _ = elevator
        .elevated(Purpose::new("not-registered"), None, async {})
        .await; // refused

    let entries = audit.entries();
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        assert!(
            entry.caller.file().ends_with("tenancy_offensive.rs"),
            "attributed to {}",
            entry.caller
        );
    }
    assert_ne!(entries[0].caller.line(), entries[1].caller.line());
}

/// ATTACK: ask, then never poll, hoping to hold a grant without a trace.
#[tokio::test]
async fn a_request_is_recorded_when_made_not_when_the_future_is_polled() {
    let (elevator, audit) = elevator();

    let never_polled = elevator.elevated(WORKER, None, async {});
    assert_eq!(audit.entries().len(), 1, "recorded at the call");
    drop(never_polled);
    assert_eq!(audit.entries().len(), 1);
}

// ── More ways in ────────────────────────────────────────────────────────────

/// ATTACK: a sink that panics (a full disk, a bug) must not turn into a grant.
#[tokio::test]
async fn a_panicking_audit_sink_grants_nothing() {
    struct Panics;
    impl pharos_app::ScopeAuditSink for Panics {
        fn record(&self, _: pharos_app::ElevationAudit) {
            panic!("sink bug");
        }
    }
    let elevator = Elevator::new(AllowPurposes::new().allow(WORKER), Panics);
    let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = ran.clone();

    let outcome = tokio::spawn(async move {
        elevator
            .elevated(WORKER, None, async move {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            })
            .await
    })
    .await;

    assert!(outcome.is_err(), "the failure is loud");
    assert!(
        !ran.load(std::sync::atomic::Ordering::SeqCst),
        "the work ran although recording failed"
    );
}

/// DOCUMENTED LIMIT: a granted future is a capability. Whoever holds it can await
/// it later, elsewhere. Nothing can stop that in Rust; what the design gives is
/// that the grant was decided and recorded at the call, attributed to the line
/// that made it, so handing one over leaves a trace in the place it was made.
#[tokio::test]
async fn a_granted_future_can_be_handed_to_other_code_but_was_recorded_where_made() {
    let (elevator, audit) = elevator();

    let handed_over = elevator.elevated(WORKER, None, async { current_elevation().is_some() });
    let granted_line = audit.entries()[0].caller.line();

    // Unprivileged code, elsewhere, later:
    let used = tokio::spawn(handed_over).await;

    assert_eq!(
        used.ok().and_then(Result::ok),
        Some(true),
        "it works for the holder"
    );
    assert_eq!(
        audit.entries().len(),
        1,
        "and it was recorded once, at its creation"
    );
    assert!(granted_line > 0);
}

/// ATTACK: per-tenant work that elevates internally (it is allowed to ask) must
/// not leave that elevation for the next tenant's turn.
#[tokio::test]
async fn an_elevation_taken_inside_one_tenants_work_does_not_reach_the_next() {
    let (elevator, _) = elevator();
    let tenants = FixedTenants(vec![tenant(), tenant(), tenant()]);
    let seen_at_start = Mutex::new(Vec::new());

    let _ = for_each_tenant(&tenants, |_| {
        let (elevator, seen) = (&elevator, &seen_at_start);
        async move {
            seen.lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(current_elevation().is_some());
            // This tenant's work asks for, and finishes with, an elevation.
            let _ = elevator.elevated(WORKER, None, async {}).await;
            Ok::<(), std::convert::Infallible>(())
        }
    })
    .await;

    assert_eq!(
        *seen_at_start.lock().unwrap_or_else(|p| p.into_inner()),
        vec![false, false, false]
    );
}

/// ATTACK: nested passes must each see their own tenant and restore the outer one.
#[tokio::test]
async fn nested_passes_see_their_own_tenant_and_restore_the_outer_one() {
    let (outer, inner) = (tenant(), tenant());
    let trail = Mutex::new(Vec::new());

    let _ = for_each_tenant(&FixedTenants(vec![outer]), |_| {
        let trail = &trail;
        async move {
            trail
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(current_tenant());
            let _ = for_each_tenant(&FixedTenants(vec![inner]), |_| async move {
                trail
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(current_tenant());
                Ok::<(), std::convert::Infallible>(())
            })
            .await;
            trail
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(current_tenant());
            Ok::<(), std::convert::Infallible>(())
        }
    })
    .await;

    assert_eq!(
        *trail.lock().unwrap_or_else(|p| p.into_inner()),
        vec![Some(outer), Some(inner), Some(outer)]
    );
}

/// DOCUMENTED LIMIT: the source is trusted. A source that lists a tenant twice,
/// or one the caller may not serve, is visited as listed; filtering is the
/// source's job, since only it knows what "all tenants" means.
#[tokio::test]
async fn the_source_is_trusted_and_duplicates_are_visited_as_listed() {
    let same = tenant();
    let visits = Mutex::new(0);

    let report = for_each_tenant(&FixedTenants(vec![same, same]), |_| {
        let visits = &visits;
        async move {
            *visits.lock().unwrap_or_else(|p| p.into_inner()) += 1;
            Ok::<(), std::convert::Infallible>(())
        }
    })
    .await
    .unwrap_or_else(|never| match never {});

    assert_eq!(report.succeeded, 2);
    assert_eq!(*visits.lock().unwrap_or_else(|p| p.into_inner()), 2);
}
