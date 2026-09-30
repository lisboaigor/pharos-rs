//! Acting across tenants on purpose, and on the record.
//!
//! A tenant-scoped application has a few callers that legitimately cross the
//! boundary: a back-office console, a worker that drains a queue shared by every
//! tenant, a lookup that runs before anyone has signed in. The usual shortcut is
//! one global "platform" flag any code can switch on, which turns the isolation
//! into a convention.
//!
//! This module makes the crossing explicit instead. To act across tenants, code
//! asks an [`Elevator`] for an [`Elevation`] naming a [`Purpose`]. The
//! application's [`ElevationPolicy`] decides whether that purpose may do it, and
//! every request, granted or denied, is handed to a [`ScopeAuditSink`]. An
//! [`Elevation`] has no public constructor: the only way to get one is through an
//! `Elevator`, so a purpose nobody registered cannot be invented at the call site.
//!
//! Two kinds of crossing, because applications choose differently:
//!
//! - [`Elevator::elevated`]: cross-tenant. Storage adapters read
//!   [`current_elevation`] and lift their per-tenant filter (for a Postgres
//!   adapter, whatever it puts in the session setting its policies read).
//! - [`Elevator::impersonating`]: the caller acts **as one tenant**, inside that
//!   tenant's ordinary scope, with the actor on the record. Nothing is lifted, so
//!   it is the narrower of the two.
//!
//! The elevation lives in its own task-local and is deliberately **not**
//! inherited by [`spawn_scoped`](crate::spawn_scoped): a background task must ask
//! for its own elevation, so an elevated request cannot silently hand its
//! privilege to whatever it spawns.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::panic::Location;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};

use crate::tenant::{TenantContext, TenantId};
use crate::tenant_local::CURRENT_TENANT;

tokio::task_local! {
    static CURRENT_ELEVATION: Option<Elevation>;
}

/// Why code is crossing the tenant boundary.
///
/// A name fixed at compile time (`&'static str`), so the set of purposes is a
/// closed list in the code, never built from a request. Applications define
/// theirs as constants: `const BACKOFFICE: Purpose = Purpose::new("backoffice");`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Purpose(&'static str);

impl Purpose {
    /// A purpose with this name.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Its name, as it appears in the audit trail.
    pub const fn name(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for Purpose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// How an elevation crosses the boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElevationKind {
    /// Across every tenant: the storage filter is lifted.
    CrossTenant,
    /// As one tenant, inside its own scope, with the actor recorded.
    Impersonation(TenantId),
}

/// A granted crossing of the tenant boundary.
///
/// Only an [`Elevator`] makes one. Read it back with [`current_elevation`].
///
/// It cannot be built by hand, nor obtained by defaulting or by naming its
/// fields:
///
/// ```compile_fail
/// use pharos_app::{Elevation, ElevationKind, Purpose};
/// let forged = Elevation {
///     purpose: Purpose::new("backoffice"),
///     actor: None,
///     kind: ElevationKind::CrossTenant,
///     granted_at: chrono::Utc::now(),
///     issuer: 1,
///     caller: std::panic::Location::caller(),
/// };
/// ```
///
/// ```compile_fail
/// let forged = pharos_app::Elevation::default();
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elevation {
    purpose: Purpose,
    actor: Option<String>,
    kind: ElevationKind,
    granted_at: DateTime<Utc>,
    issuer: u64,
    caller: &'static Location<'static>,
}

impl Elevation {
    /// What it was granted for.
    pub fn purpose(&self) -> Purpose {
        self.purpose
    }

    /// Who asked, when the caller said.
    pub fn actor(&self) -> Option<&str> {
        self.actor.as_deref()
    }

    /// Cross-tenant or impersonation.
    pub fn kind(&self) -> ElevationKind {
        self.kind
    }

    /// When it was granted.
    pub fn granted_at(&self) -> DateTime<Utc> {
        self.granted_at
    }

    /// Where in the code it was asked for.
    pub fn caller(&self) -> &'static Location<'static> {
        self.caller
    }
}

/// The elevation of the current task, if it has one.
///
/// What a storage adapter calls to decide whether to lift its tenant filter.
pub fn current_elevation() -> Option<Elevation> {
    CURRENT_ELEVATION
        .try_with(|elevation| elevation.clone())
        .ok()
        .flatten()
}

/// Runs `work` with no elevation, whatever the caller holds.
///
/// For the helpers that open a **tenant** scope (`for_each_tenant`,
/// `with_message_scope`): the work they run is meant to see one tenant, so it must
/// not inherit a cross-tenant elevation, or an impersonation of another tenant,
/// from whoever called them.
pub(crate) fn without_elevation<F: Future>(work: F) -> impl Future<Output = F::Output> {
    CURRENT_ELEVATION.scope(None, work)
}

/// What an [`ElevationPolicy`] is asked to decide.
#[derive(Debug, Clone, Copy)]
pub struct ElevationRequest<'a> {
    /// What the caller wants to do it for.
    pub purpose: Purpose,
    /// Who is asking, if the caller said (blank counts as not said).
    pub actor: Option<&'a str>,
    /// Across tenants, or as one tenant.
    pub kind: ElevationKind,
}

/// Which purposes may cross the boundary, and how.
///
/// The application's own rule, registered once when it starts. Return the
/// reason for a refusal: it goes to the audit trail and to the caller.
pub trait ElevationPolicy: Send + Sync {
    /// `Ok(())` to grant, or why not.
    fn authorize(&self, request: &ElevationRequest<'_>) -> Result<(), &'static str>;
}

/// Refuses every elevation: for an application with no cross-tenant caller, or
/// as the safe starting point of a test.
#[derive(Debug, Clone, Copy, Default)]
pub struct DenyAll;

impl ElevationPolicy for DenyAll {
    fn authorize(&self, _: &ElevationRequest<'_>) -> Result<(), &'static str> {
        Err("no purpose may cross the tenant boundary")
    }
}

/// Grants the purposes it was told about, for the kind of crossing it was told,
/// and nothing else.
#[derive(Debug, Clone, Default)]
pub struct AllowPurposes {
    cross_tenant: BTreeSet<Purpose>,
    impersonation: BTreeSet<Purpose>,
    actor_required: BTreeSet<Purpose>,
}

impl AllowPurposes {
    /// A policy that grants nothing yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lets `purpose` act across every tenant.
    pub fn allow(mut self, purpose: Purpose) -> Self {
        self.cross_tenant.insert(purpose);
        self
    }

    /// Lets `purpose` act as one tenant (an impersonation).
    pub fn allow_impersonation(mut self, purpose: Purpose) -> Self {
        self.impersonation.insert(purpose);
        self
    }

    /// Refuses `purpose` unless the caller names who is acting. Meant for the
    /// human-driven ones (a back-office console); a worker has no one to name.
    pub fn requiring_actor(mut self, purpose: Purpose) -> Self {
        self.actor_required.insert(purpose);
        self
    }
}

impl ElevationPolicy for AllowPurposes {
    fn authorize(&self, request: &ElevationRequest<'_>) -> Result<(), &'static str> {
        let allowed = match request.kind {
            ElevationKind::CrossTenant => &self.cross_tenant,
            ElevationKind::Impersonation(_) => &self.impersonation,
        };
        if !allowed.contains(&request.purpose) {
            return Err("this purpose may not cross the tenant boundary this way");
        }
        if self.actor_required.contains(&request.purpose) && request.actor.is_none() {
            return Err("this purpose needs to say who is acting");
        }
        Ok(())
    }
}

/// What became of an elevation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElevationOutcome {
    /// Granted.
    Granted,
    /// Refused, and why.
    Denied(&'static str),
}

/// One elevation request, as the audit trail sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElevationAudit {
    /// What it was asked for.
    pub purpose: Purpose,
    /// Who asked, if said.
    pub actor: Option<String>,
    /// Across tenants, or as one tenant.
    pub kind: ElevationKind,
    /// Granted or denied.
    pub outcome: ElevationOutcome,
    /// When it was decided.
    pub at: DateTime<Utc>,
    /// Where in the code it was asked for. A purpose is only a name, so this is
    /// what ties an entry to the code that made the request.
    pub caller: &'static Location<'static>,
}

/// Where elevation requests are recorded.
///
/// Synchronous and non-failing on purpose: recording must not decide whether the
/// work runs. An implementation that writes to a database hands the entry to a
/// channel and writes from its own task. It also decides *how much* to record: a
/// console records every entry, a worker that elevates several times a second can
/// count and write one line per interval.
pub trait ScopeAuditSink: Send + Sync {
    /// Takes note of one request.
    fn record(&self, entry: ElevationAudit);
}

/// Records nothing: for tests that are not about the audit trail.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoAudit;

impl ScopeAuditSink for NoAudit {
    fn record(&self, _: ElevationAudit) {}
}

/// Keeps every entry in memory, for tests.
#[derive(Debug, Clone, Default)]
pub struct MemoryAudit(Arc<Mutex<Vec<ElevationAudit>>>);

impl MemoryAudit {
    /// An empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// What has been recorded so far, oldest first.
    pub fn entries(&self) -> Vec<ElevationAudit> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

impl ScopeAuditSink for MemoryAudit {
    fn record(&self, entry: ElevationAudit) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).push(entry);
    }
}

/// A request that was refused. The work it was for did not run.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("elevation for `{purpose}` refused: {reason}")]
pub struct ElevationDenied {
    /// What was asked for.
    pub purpose: Purpose,
    /// Why not.
    pub reason: &'static str,
}

/// Grants elevations under the application's policy, and records every request.
///
/// Built once at startup and shared (it is cheap to clone).
#[derive(Clone)]
pub struct Elevator {
    id: u64,
    policy: Arc<dyn ElevationPolicy>,
    audit: Arc<dyn ScopeAuditSink>,
}

/// Each elevator gets its own number, so an elevation can be traced to the one
/// that granted it. Clones share it: they are the same elevator.
static NEXT_ELEVATOR: AtomicU64 = AtomicU64::new(1);

/// The longest actor an elevation will carry.
const MAX_ACTOR_LEN: usize = 256;

impl fmt::Debug for Elevator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Elevator").finish_non_exhaustive()
    }
}

impl Elevator {
    /// An elevator that asks `policy` and reports to `audit`.
    pub fn new(
        policy: impl ElevationPolicy + 'static,
        audit: impl ScopeAuditSink + 'static,
    ) -> Self {
        Self {
            id: NEXT_ELEVATOR.fetch_add(1, Ordering::Relaxed),
            policy: Arc::new(policy),
            audit: Arc::new(audit),
        }
    }

    /// Whether **this** elevator granted `elevation`.
    ///
    /// An `Elevation` cannot be built by hand, but any code can build an
    /// `Elevator` with a policy that grants everything and obtain a valid one.
    /// A storage adapter that lifts a filter must therefore not accept any
    /// elevation it finds: it keeps its own elevator and accepts only what that
    /// elevator issued.
    pub fn issued(&self, elevation: &Elevation) -> bool {
        elevation.issuer == self.id
    }

    /// Runs `work` across every tenant, for `purpose`.
    ///
    /// The request is decided and recorded when this is **called**, not when the
    /// returned future is first polled. Returns [`ElevationDenied`], without
    /// running `work`, when the policy refuses.
    #[track_caller]
    pub fn elevated<F: Future>(
        &self,
        purpose: Purpose,
        actor: Option<&str>,
        work: F,
    ) -> impl Future<Output = Result<F::Output, ElevationDenied>> + use<F> {
        let admitted = self.admit(
            purpose,
            actor,
            ElevationKind::CrossTenant,
            Location::caller(),
        );
        async move {
            let elevation = admitted?;
            Ok(CURRENT_ELEVATION.scope(Some(elevation), work).await)
        }
    }

    /// Runs `work` as `tenant`, for `purpose`, with `actor` on the record.
    ///
    /// The work sees exactly what the tenant itself would: its ordinary scope is
    /// opened, nothing is lifted. An impersonation always names its actor; a
    /// blank one is refused. Decided and recorded when called, like
    /// [`elevated`](Self::elevated).
    #[track_caller]
    pub fn impersonating<F: Future>(
        &self,
        tenant: TenantId,
        purpose: Purpose,
        actor: &str,
        work: F,
    ) -> impl Future<Output = Result<F::Output, ElevationDenied>> + use<F> {
        let admitted = self.admit(
            purpose,
            Some(actor),
            ElevationKind::Impersonation(tenant),
            Location::caller(),
        );
        async move {
            let elevation = admitted?;
            let scope = Some(TenantContext::new(tenant));
            Ok(CURRENT_TENANT
                .scope(scope, CURRENT_ELEVATION.scope(Some(elevation), work))
                .await)
        }
    }

    fn admit(
        &self,
        purpose: Purpose,
        actor: Option<&str>,
        kind: ElevationKind,
        caller: &'static Location<'static>,
    ) -> Result<Elevation, ElevationDenied> {
        let actor = actor.map(str::trim).filter(|a| !a.is_empty());
        let at = Utc::now();

        let decision = match (kind, actor) {
            // An actor ends up in logs and audit rows: a label, not free text.
            // Control characters would let it forge a line of the trail, and an
            // unbounded one would flood the sink.
            (_, Some(a)) if !is_plain_label(a) => Err("the actor is not a plain label"),
            (ElevationKind::Impersonation(_), None) => Err("an impersonation needs an actor"),
            _ => self.policy.authorize(&ElevationRequest {
                purpose,
                actor,
                kind,
            }),
        };

        self.audit.record(ElevationAudit {
            purpose,
            // A refused, malformed actor is not echoed into the trail.
            actor: actor.filter(|a| is_plain_label(a)).map(str::to_owned),
            kind,
            outcome: match decision {
                Ok(()) => ElevationOutcome::Granted,
                Err(reason) => ElevationOutcome::Denied(reason),
            },
            at,
            caller,
        });

        match decision {
            Ok(()) => Ok(Elevation {
                purpose,
                actor: actor.map(str::to_owned),
                kind,
                granted_at: at,
                issuer: self.id,
                caller,
            }),
            Err(reason) => Err(ElevationDenied { purpose, reason }),
        }
    }
}

/// Short and free of control characters.
fn is_plain_label(actor: &str) -> bool {
    actor.chars().count() <= MAX_ACTOR_LEN && !actor.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use uuid::Uuid;

    use super::*;

    const CONSOLE: Purpose = Purpose::new("console");
    const WORKER: Purpose = Purpose::new("worker:queue");
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

    fn policy() -> AllowPurposes {
        AllowPurposes::new()
            .allow(CONSOLE)
            .requiring_actor(CONSOLE)
            .allow(WORKER)
            .allow_impersonation(SUPPORT)
    }

    fn elevator() -> (Elevator, MemoryAudit) {
        let audit = MemoryAudit::new();
        (Elevator::new(policy(), audit.clone()), audit)
    }

    #[tokio::test]
    async fn a_granted_elevation_is_visible_inside_and_only_inside() {
        let (elevator, _) = elevator();
        assert_eq!(current_elevation(), None);

        let seen = elevator
            .elevated(WORKER, None, async { current_elevation() })
            .await;

        let Ok(Some(elevation)) = seen else {
            panic!("the worker purpose is granted and visible to the work");
        };
        assert_eq!(elevation.purpose(), WORKER);
        assert_eq!(elevation.kind(), ElevationKind::CrossTenant);
        assert_eq!(current_elevation(), None, "it does not outlive the work");
    }

    #[tokio::test]
    async fn a_refused_elevation_does_not_run_the_work() {
        let (elevator, _) = elevator();
        let ran = AtomicBool::new(false);

        let outcome = elevator
            .elevated(Purpose::new("nobody-registered-this"), None, async {
                ran.store(true, Ordering::SeqCst);
            })
            .await;

        assert!(outcome.is_err());
        assert!(!ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn deny_all_refuses_everything() {
        let elevator = Elevator::new(DenyAll, NoAudit);
        assert!(elevator.elevated(WORKER, None, async {}).await.is_err());
        assert!(
            elevator
                .impersonating(tenant(), SUPPORT, "ana", async {})
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_purpose_that_needs_an_actor_is_refused_without_one() {
        let (elevator, _) = elevator();

        assert!(elevator.elevated(CONSOLE, None, async {}).await.is_err());
        assert!(
            elevator
                .elevated(CONSOLE, Some("   "), async {})
                .await
                .is_err(),
            "a blank actor is no actor"
        );
        assert!(
            elevator
                .elevated(CONSOLE, Some("ana"), async {})
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn every_request_is_recorded_granted_or_denied() {
        let (elevator, audit) = elevator();

        let _ = elevator.elevated(CONSOLE, Some("ana"), async {}).await;
        let _ = elevator.elevated(CONSOLE, None, async {}).await;

        let entries = audit.entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].outcome, ElevationOutcome::Granted);
        assert_eq!(entries[0].actor.as_deref(), Some("ana"));
        assert_eq!(entries[0].purpose, CONSOLE);
        assert!(matches!(entries[1].outcome, ElevationOutcome::Denied(_)));
    }

    #[tokio::test]
    async fn an_impersonation_runs_as_the_tenant_with_the_actor_on_record() {
        let (elevator, audit) = elevator();
        let customer = tenant();

        let seen = elevator
            .impersonating(customer, SUPPORT, "ana", async {
                (current_tenant(), current_elevation())
            })
            .await;

        let Ok((scope, Some(elevation))) = seen else {
            panic!("support may impersonate");
        };
        assert_eq!(scope, Some(customer), "the tenant's ordinary scope is open");
        assert_eq!(elevation.actor(), Some("ana"));
        assert_eq!(elevation.kind(), ElevationKind::Impersonation(customer));
        assert_eq!(current_tenant(), None, "and it closes afterwards");
        assert_eq!(audit.entries().len(), 1);
    }

    #[tokio::test]
    async fn an_impersonation_needs_an_actor_whatever_the_policy_says() {
        // A policy that grants everything still cannot impersonate anonymously.
        struct Anything;
        impl ElevationPolicy for Anything {
            fn authorize(&self, _: &ElevationRequest<'_>) -> Result<(), &'static str> {
                Ok(())
            }
        }
        let elevator = Elevator::new(Anything, NoAudit);

        assert!(
            elevator
                .impersonating(tenant(), SUPPORT, "  ", async {})
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn the_kind_of_crossing_is_checked_separately() {
        let (elevator, _) = elevator();
        // SUPPORT may impersonate but not cross tenants; WORKER the opposite.
        assert!(elevator.elevated(SUPPORT, None, async {}).await.is_err());
        assert!(
            elevator
                .impersonating(tenant(), WORKER, "ana", async {})
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_spawned_task_does_not_inherit_the_elevation() {
        let (elevator, _) = elevator();

        let inherited = elevator
            .elevated(WORKER, None, async {
                crate::spawn_scoped(async { current_elevation() }).await
            })
            .await;

        let Ok(Ok(in_task)) = inherited else {
            panic!("the elevation is granted and the task joins");
        };
        assert_eq!(in_task, None, "a task must ask for its own elevation");
    }
}
