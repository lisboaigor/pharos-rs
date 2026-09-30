use std::future::Future;

use crate::tenant::{TenantContext, TenantId};
use crate::tenant_local::CURRENT_TENANT;

/// Where a background job learns which tenants exist.
///
/// The framework does not know how an application stores its tenants, so the
/// application supplies the list: a query on its tenants table, a config file,
/// a fixed set in a test. Whatever reads it may need elevated access, because
/// listing every tenant is by nature not scoped to one; that concern stays in
/// the implementation.
#[trait_variant::make(Send)]
pub trait TenantSource: Sync {
    /// Why the tenants could not be listed.
    type Error: std::error::Error + Send + Sync + 'static;

    /// The tenants a job should visit now (typically the active ones).
    async fn tenants(&self) -> Result<Vec<TenantId>, Self::Error>;
}

/// A fixed list of tenants: for tests, and for jobs bound to a known set.
#[derive(Debug, Clone, Default)]
pub struct FixedTenants(pub Vec<TenantId>);

impl TenantSource for FixedTenants {
    type Error = std::convert::Infallible;

    async fn tenants(&self) -> Result<Vec<TenantId>, Self::Error> {
        Ok(self.0.clone())
    }
}

/// What a [`for_each_tenant`] pass did.
#[derive(Debug)]
pub struct ForEachReport<E> {
    /// Tenants whose work returned `Ok`.
    pub succeeded: usize,
    /// Tenants whose work failed, in visit order. One tenant's failure never
    /// stops the pass or hides another tenant's result.
    pub failed: Vec<(TenantId, E)>,
}

impl<E> ForEachReport<E> {
    /// Tenants visited, successful or not.
    pub fn visited(&self) -> usize {
        self.succeeded + self.failed.len()
    }
}

/// Runs `work` once per tenant from `source`, each inside that tenant's scope.
///
/// This is the shape every per-tenant background job (escalation, reports,
/// maintenance sweeps, reprocessing) otherwise writes by hand: list the
/// tenants, then for each one open [`CURRENT_TENANT`] and run. Two things go
/// wrong in the hand-written version and are fixed here: forgetting the scope
/// for one call (deny-by-default stores then see nothing, silently), and
/// letting one tenant's error abort the pass for everyone after it.
///
/// Tenants are visited one at a time, in the order the source returned them.
/// `Err` is returned only when the tenants could not be listed; a failing
/// tenant is reported in [`ForEachReport::failed`] instead.
pub async fn for_each_tenant<S, F, Fut, E>(
    source: &S,
    mut work: F,
) -> Result<ForEachReport<E>, S::Error>
where
    S: TenantSource,
    F: FnMut(TenantId) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let tenants = source.tenants().await?;
    let mut report = ForEachReport {
        succeeded: 0,
        failed: Vec::new(),
    };
    for tenant in tenants {
        let scope = Some(TenantContext::new(tenant));
        match CURRENT_TENANT.scope(scope, work(tenant)).await {
            Ok(()) => report.succeeded += 1,
            Err(e) => report.failed.push((tenant, e)),
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use uuid::Uuid;

    use super::*;

    fn ids(n: usize) -> Vec<TenantId> {
        (0..n).map(|_| TenantId::new(Uuid::now_v7())).collect()
    }

    fn current() -> Option<TenantId> {
        CURRENT_TENANT
            .try_with(|t| t.map(|c| c.tenant_id()))
            .ok()
            .flatten()
    }

    #[tokio::test]
    async fn each_tenant_runs_in_its_own_scope_in_order() {
        let tenants = ids(3);
        let seen = Mutex::new(Vec::new());

        let report = for_each_tenant(&FixedTenants(tenants.clone()), |tenant| {
            let seen = &seen;
            async move {
                seen.lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push((tenant, current()));
                Ok::<(), std::convert::Infallible>(())
            }
        })
        .await
        .unwrap_or_else(|never| match never {});

        assert_eq!(report.succeeded, 3);
        let seen = seen.into_inner().unwrap_or_else(|p| p.into_inner());
        let expected: Vec<_> = tenants.iter().map(|t| (*t, Some(*t))).collect();
        assert_eq!(seen, expected, "the scope is the tenant being visited");
    }

    #[tokio::test]
    async fn one_tenants_failure_does_not_stop_the_others() {
        let tenants = ids(3);
        let bad = tenants[1];

        let report = for_each_tenant(&FixedTenants(tenants.clone()), |tenant| async move {
            if tenant == bad { Err("boom") } else { Ok(()) }
        })
        .await
        .unwrap_or_else(|never| match never {});

        assert_eq!(report.succeeded, 2);
        assert_eq!(report.visited(), 3);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].0, bad);
    }

    #[tokio::test]
    async fn the_scope_does_not_leak_out_of_the_pass() {
        let _ = for_each_tenant(&FixedTenants(ids(2)), |_| async {
            Ok::<(), std::convert::Infallible>(())
        })
        .await;
        assert_eq!(current(), None);
    }

    #[derive(Debug, thiserror::Error)]
    #[error("listing failed")]
    struct ListFailed;

    struct BrokenSource;

    impl TenantSource for BrokenSource {
        type Error = ListFailed;

        async fn tenants(&self) -> Result<Vec<TenantId>, ListFailed> {
            Err(ListFailed)
        }
    }

    #[tokio::test]
    async fn a_source_that_cannot_list_is_an_error_and_runs_nothing() {
        let ran = Mutex::new(0);
        let outcome = for_each_tenant(&BrokenSource, |_| {
            *ran.lock().unwrap_or_else(|p| p.into_inner()) += 1;
            async { Ok::<(), std::convert::Infallible>(()) }
        })
        .await;

        assert!(outcome.is_err());
        assert_eq!(*ran.lock().unwrap_or_else(|p| p.into_inner()), 0);
    }
}
