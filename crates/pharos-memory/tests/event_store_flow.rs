//! `InMemoryEventStore`/`InMemorySnapshotStore` against the same
//! optimistic-concurrency and round-trip contract a durable event store
//! (e.g. `pharos-postgres`'s `PgEventStore`) must honor.

use pharos_core::RepositoryError;
use pharos_es::{EventStore, Snapshot, SnapshotStore};
use pharos_memory::{InMemoryEventStore, InMemorySnapshotStore};

#[tokio::test]
async fn append_enforces_optimistic_concurrency_per_stream()
-> Result<(), Box<dyn std::error::Error>> {
    let store = InMemoryEventStore::<String, String>::new();
    let id = "order-1".to_string();

    // First append on a fresh stream must start at `expected_version = 0`.
    store
        .append(&id, 0, vec!["Created".to_string()])
        .await
        .map_err(|e| e.to_string())?;

    let events = store.load(&id).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[0].event, "Created");

    // A stale `expected_version` is rejected without mutating the stream.
    let stale = store.append(&id, 0, vec!["Duplicate".to_string()]).await;
    assert!(matches!(
        stale,
        Err(RepositoryError::ConcurrencyConflict {
            expected: 0,
            actual: Some(1)
        })
    ));
    assert_eq!(
        store.load(&id).await?.len(),
        1,
        "the rejected append must not append"
    );

    // The correct next version succeeds and continues the sequence.
    store
        .append(&id, 1, vec!["Confirmed".to_string(), "Shipped".to_string()])
        .await
        .map_err(|e| e.to_string())?;
    let events = store.load(&id).await?;
    assert_eq!(events.len(), 3);
    assert_eq!(
        events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    Ok(())
}

#[tokio::test]
async fn delete_stream_removes_all_events() -> Result<(), Box<dyn std::error::Error>> {
    let store = InMemoryEventStore::<String, String>::new();
    let id = "order-2".to_string();
    store
        .append(&id, 0, vec!["Created".to_string()])
        .await
        .map_err(|e| e.to_string())?;

    store.delete_stream(&id).await?;

    assert!(store.load(&id).await?.is_empty());
    // A deleted stream is indistinguishable from one that never existed:
    // the next append must be accepted at `expected_version = 0` again.
    store
        .append(&id, 0, vec!["Recreated".to_string()])
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[tokio::test]
async fn snapshot_store_round_trips_and_replaces() -> Result<(), Box<dyn std::error::Error>> {
    let store = InMemorySnapshotStore::<String, u64>::new();
    let id = "order-3".to_string();

    assert_eq!(store.load(&id).await?, None);

    store.save(&id, Snapshot::new(10, 5)).await?;
    let loaded = store.load(&id).await?.ok_or("snapshot not found")?;
    assert_eq!(loaded.state, 10);
    assert_eq!(loaded.version, 5);

    // `save` replaces unconditionally — a snapshot store has no
    // concurrency contract of its own; the event store's optimistic
    // concurrency on the underlying stream is what actually guards
    // against a stale writer.
    store.save(&id, Snapshot::new(20, 9)).await?;
    let loaded = store.load(&id).await?.ok_or("snapshot not found")?;
    assert_eq!(loaded.state, 20);
    assert_eq!(loaded.version, 9);
    Ok(())
}
