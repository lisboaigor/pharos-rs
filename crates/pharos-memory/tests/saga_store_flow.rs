//! `InMemorySagaStore` against the same optimistic-concurrency and
//! due-timeout-claiming contract a durable saga store (e.g.
//! `pharos-postgres`'s `PgSagaStore`) must honor.

use chrono::{Duration, Utc};
use pharos_memory::InMemorySagaStore;
use pharos_saga::{SagaInstance, SagaSaveError, SagaStatus, SagaStore, SagaTimeoutStore};

#[tokio::test]
async fn save_enforces_optimistic_concurrency() -> Result<(), Box<dyn std::error::Error>> {
    let store = InMemorySagaStore::<String, u32>::new();
    let id = "wager-1".to_string();

    // `version == 0` means "never persisted": the first save creates the row.
    let instance = SagaInstance::running(id.clone(), 1);
    store.save(instance).await?;

    let loaded = store.load(&id).await?.ok_or("saga not found")?;
    assert_eq!(loaded.version, 1, "save must store expected_version + 1");
    assert_eq!(loaded.state, 1);

    // Saving again at the now-stale version 0 is a conflict: it looks like
    // a fresh create, but the row already exists.
    let stale_create = store.save(SagaInstance::running(id.clone(), 2)).await;
    assert!(matches!(
        stale_create,
        Err(SagaSaveError::ConcurrencyConflict {
            expected: 0,
            actual: Some(1)
        })
    ));

    // Saving at the correct version updates and advances it.
    let mut next = loaded;
    next.state = 2;
    store.save(next).await?;
    let loaded = store.load(&id).await?.ok_or("saga not found")?;
    assert_eq!(loaded.version, 2);
    assert_eq!(loaded.state, 2);

    // The version that just succeeded is now itself stale.
    let mut stale_update = loaded.clone();
    stale_update.state = 3;
    stale_update.version = 1;
    let result = store.save(stale_update).await;
    assert!(matches!(
        result,
        Err(SagaSaveError::ConcurrencyConflict {
            expected: 1,
            actual: Some(2)
        })
    ));
    Ok(())
}

#[tokio::test]
async fn claim_due_leases_instances_and_hides_them_from_concurrent_sweeps()
-> Result<(), Box<dyn std::error::Error>> {
    let store = InMemorySagaStore::<String, u32>::new();
    let now = Utc::now();

    let overdue = SagaInstance::running_until("a".to_string(), 1, now - Duration::seconds(10));
    let later = SagaInstance::running_until("b".to_string(), 1, now + Duration::seconds(3600));
    store.save(overdue).await?;
    store.save(later).await?;

    let claimed = store.claim_due(now, Duration::seconds(60), 10).await?;
    assert_eq!(claimed.len(), 1, "only the overdue instance is due");
    assert_eq!(claimed[0].id, "a");
    // The caller sees the original, elapsed deadline, not the lease.
    assert!(
        claimed[0]
            .deadline
            .ok_or("claimed instance has no deadline")?
            < now
    );

    // A second sweeper racing the same instant must not see it again: the
    // claim already pushed its deadline into the future.
    let second_claim = store.claim_due(now, Duration::seconds(60), 10).await?;
    assert!(second_claim.is_empty());

    // The stored deadline moved to the lease, and the version bumped —
    // proof a concurrent `save` at the pre-claim version now loses.
    let stored = store
        .load(&"a".to_string())
        .await?
        .ok_or("saga not found")?;
    assert!(stored.deadline.ok_or("stored instance has no deadline")? > now);
    assert_eq!(stored.version, 2);
    Ok(())
}

#[tokio::test]
async fn claim_due_returns_soonest_deadline_first() -> Result<(), Box<dyn std::error::Error>> {
    let store = InMemorySagaStore::<String, u32>::new();
    let now = Utc::now();

    store
        .save(SagaInstance::running_until(
            "later".to_string(),
            1,
            now - Duration::seconds(5),
        ))
        .await?;
    store
        .save(SagaInstance::running_until(
            "sooner".to_string(),
            1,
            now - Duration::seconds(50),
        ))
        .await?;

    let claimed = store.claim_due(now, Duration::seconds(60), 10).await?;
    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].id, "sooner");
    assert_eq!(claimed[1].id, "later");
    Ok(())
}

#[tokio::test]
async fn claim_due_ignores_completed_and_not_yet_due_instances()
-> Result<(), Box<dyn std::error::Error>> {
    let store = InMemorySagaStore::<String, u32>::new();
    let now = Utc::now();

    let mut completed =
        SagaInstance::running_until("done".to_string(), 1, now - Duration::seconds(5));
    completed.status = SagaStatus::Completed;
    store.save(completed).await?;
    store
        .save(SagaInstance::running_until(
            "future".to_string(),
            1,
            now + Duration::seconds(5),
        ))
        .await?;
    store
        .save(SagaInstance::running("no-deadline".to_string(), 1))
        .await?;

    let claimed = store.claim_due(now, Duration::seconds(60), 10).await?;
    assert!(claimed.is_empty());
    Ok(())
}
