//! Conformance suite for [`EventStore`] and [`SnapshotStore`].
use chrono::Utc;
use pharos_core::RepositoryError;
use pharos_es::{EventStore, Snapshot, SnapshotStore};
use uuid::Uuid;

use super::fixtures::ContractEvent;

/// Runs the [`EventStore`] conformance suite against `store`, instantiated
/// at `EventStore<Uuid, ContractEvent>`.
///
/// Covers per-stream optimistic concurrency (`append`'s `expected_version`
/// must equal the stream's current length, checked and updated
/// atomically), sequence numbering starting at `1`, `load_after`'s tail
/// slice, and `delete_stream`.
///
/// ```ignore
/// #[tokio::test]
/// async fn event_store_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::event_store::run(&store).await
/// }
/// ```
pub async fn run<St>(store: &St) -> Result<(), Box<dyn std::error::Error>>
where
    St: EventStore<Uuid, ContractEvent>,
{
    let id = Uuid::now_v7();
    assert!(store.load(&id).await?.is_empty());

    let event = |label: &str| ContractEvent {
        aggregate_id: id.to_string(),
        occurred_at: Utc::now(),
        label: label.to_string(),
    };

    store.append(&id, 0, vec![event("created")]).await?;
    let loaded = store.load(&id).await?;
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].sequence, 1);
    assert_eq!(loaded[0].event.label, "created");

    let stale = store.append(&id, 0, vec![event("duplicate")]).await;
    assert!(
        matches!(
            stale,
            Err(RepositoryError::ConcurrencyConflict {
                expected: 0,
                actual: Some(1)
            })
        ),
        "a stale expected_version must be rejected"
    );
    assert_eq!(
        store.load(&id).await?.len(),
        1,
        "a rejected append must not mutate the stream"
    );

    store
        .append(&id, 1, vec![event("confirmed"), event("shipped")])
        .await?;
    let loaded = store.load(&id).await?;
    assert_eq!(loaded.len(), 3);
    assert_eq!(
        loaded.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    let after_first = store.load_after(&id, 1).await?;
    assert_eq!(after_first.len(), 2);
    assert_eq!(after_first[0].sequence, 2);
    assert_eq!(after_first[0].event.label, "confirmed");

    store.delete_stream(&id).await?;
    assert!(store.load(&id).await?.is_empty());
    // A deleted stream must be indistinguishable from one that never
    // existed: the next append starts fresh at `expected_version = 0`.
    store.append(&id, 0, vec![event("recreated")]).await?;

    Ok(())
}

/// Runs the [`SnapshotStore`] conformance suite against `store`,
/// instantiated at `SnapshotStore<Uuid, u64>` — the snapshotted state's
/// shape is irrelevant to the contract.
///
/// ```ignore
/// #[tokio::test]
/// async fn snapshot_store_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::event_store::snapshot_store(&store).await
/// }
/// ```
pub async fn snapshot_store<St>(store: &St) -> Result<(), Box<dyn std::error::Error>>
where
    St: SnapshotStore<Uuid, u64>,
{
    let id = Uuid::now_v7();
    assert_eq!(store.load(&id).await?, None);

    store.save(&id, Snapshot::new(10, 3)).await?;
    let loaded = store.load(&id).await?.ok_or("snapshot not found")?;
    assert_eq!(loaded.state, 10);
    assert_eq!(loaded.version, 3);

    // `save` replaces unconditionally — a snapshot store has no
    // concurrency contract of its own; the event store's own optimistic
    // concurrency is what guards the underlying stream against a stale
    // writer.
    store.save(&id, Snapshot::new(20, 9)).await?;
    let loaded = store.load(&id).await?.ok_or("snapshot not found")?;
    assert_eq!(loaded.state, 20);
    assert_eq!(loaded.version, 9);

    Ok(())
}
