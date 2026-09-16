//! Conformance suite for [`SagaStore`] and [`SagaTimeoutStore`].
use chrono::{Duration, Utc};
use pharos_saga::{SagaInstance, SagaSaveError, SagaStatus, SagaStore, SagaTimeoutStore};

/// Runs the [`SagaStore`] conformance suite against `store`, instantiated
/// at `SagaStore<String, u32>` — the saga id and state types are
/// irrelevant to the contract, so the suite fixes them the same way
/// [`crate::contract::ContractAggregate`] fixes an aggregate shape for the
/// repository suites.
///
/// Covers the full compare-and-swap contract: `version == 0` creates,
/// otherwise `save` only succeeds when the stored version still matches,
/// storing `version + 1` either way; every rejected save leaves the stored
/// state untouched.
///
/// ```ignore
/// #[tokio::test]
/// async fn saga_store_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::saga::run(&store).await
/// }
/// ```
pub async fn run<St>(store: &St) -> Result<(), Box<dyn std::error::Error>>
where
    St: SagaStore<String, u32>,
{
    let id = "contract-saga-1".to_string();
    assert!(store.load(&id).await?.is_none());

    store.save(SagaInstance::running(id.clone(), 1)).await?;
    let loaded = store
        .load(&id)
        .await?
        .ok_or("saga not found after create")?;
    assert_eq!(loaded.version, 1, "save must store expected_version + 1");
    assert_eq!(loaded.state, 1);

    // `version == 0` on an id that already exists looks like a create, but
    // must be refused: someone else created it first.
    let conflicting_create = store.save(SagaInstance::running(id.clone(), 99)).await;
    assert!(
        matches!(
            conflicting_create,
            Err(SagaSaveError::ConcurrencyConflict {
                expected: 0,
                actual: Some(1)
            })
        ),
        "a create at version 0 against an existing row must conflict"
    );

    let mut next = loaded;
    next.state = 2;
    store.save(next).await?;
    let loaded = store
        .load(&id)
        .await?
        .ok_or("saga not found after update")?;
    assert_eq!(loaded.version, 2);
    assert_eq!(loaded.state, 2);

    let mut stale = loaded.clone();
    stale.version = 1;
    stale.state = 3;
    let stale_result = store.save(stale).await;
    assert!(
        matches!(
            stale_result,
            Err(SagaSaveError::ConcurrencyConflict {
                expected: 1,
                actual: Some(2)
            })
        ),
        "a save at a stale version must conflict and not mutate the stored state"
    );
    let unchanged = store.load(&id).await?.ok_or("saga not found")?;
    assert_eq!(
        unchanged.state, 2,
        "a rejected save must not mutate storage"
    );

    Ok(())
}

/// Runs the [`SagaTimeoutStore`] conformance suite against `store`.
///
/// Covers `claim_due`'s three guarantees: only [`SagaStatus::Running`]
/// instances whose deadline has elapsed are claimed; claimed instances are
/// returned soonest-deadline-first; and claiming leases the instance (moves
/// its stored deadline into the future and advances its version) so a
/// second concurrent sweep never claims the same instance twice.
///
/// ```ignore
/// #[tokio::test]
/// async fn saga_timeout_store_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::saga::timeout(&store).await
/// }
/// ```
pub async fn timeout<St>(store: &St) -> Result<(), Box<dyn std::error::Error>>
where
    St: SagaTimeoutStore<String, u32>,
{
    let now = Utc::now();

    let mut not_running = SagaInstance::running_until(
        "contract-saga-completed".to_string(),
        1,
        now - Duration::seconds(5),
    );
    not_running.status = SagaStatus::Completed;
    store.save(not_running).await?;
    store
        .save(SagaInstance::running_until(
            "contract-saga-future".to_string(),
            1,
            now + Duration::seconds(3600),
        ))
        .await?;
    store
        .save(SagaInstance::running(
            "contract-saga-no-deadline".to_string(),
            1,
        ))
        .await?;

    assert!(
        store
            .claim_due(now, Duration::seconds(60), 10)
            .await?
            .is_empty(),
        "a completed, a not-yet-due, and a deadline-less instance must never be claimed"
    );

    let sooner = "contract-saga-sooner".to_string();
    let later = "contract-saga-later".to_string();
    store
        .save(SagaInstance::running_until(
            later.clone(),
            1,
            now - Duration::seconds(5),
        ))
        .await?;
    store
        .save(SagaInstance::running_until(
            sooner.clone(),
            1,
            now - Duration::seconds(50),
        ))
        .await?;

    let claimed = store.claim_due(now, Duration::seconds(60), 10).await?;
    assert_eq!(claimed.len(), 2);
    assert_eq!(
        claimed[0].id, sooner,
        "claim_due must return the soonest deadline first"
    );
    assert_eq!(claimed[1].id, later);

    let second_sweep = store.claim_due(now, Duration::seconds(60), 10).await?;
    assert!(
        second_sweep.is_empty(),
        "an instance just claimed must not be claimable again before its lease expires"
    );

    Ok(())
}
