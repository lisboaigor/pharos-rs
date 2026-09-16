//! Conformance suite for [`Repository`].
use pharos_core::{AggregateRoot, Entity, Repository, RepositoryError};
use uuid::Uuid;

use super::fixtures::ContractAggregate;

/// Runs the full [`Repository`] conformance suite against `repo`.
///
/// Call this from a `#[tokio::test]` in your adapter crate:
///
/// ```ignore
/// #[tokio::test]
/// async fn repository_contract() -> Result<(), Box<dyn std::error::Error>> {
///     let repo = YourRepository::<ContractAggregate>::new(/* ... */);
///     pharos_testing::contract::repository::run(&repo).await
/// }
/// ```
///
/// Covers, in order:
/// - a fresh id round-trips as [`None`] from [`Repository::find_by_id`];
/// - saving a new aggregate (`version == 0`) creates it and advances its
///   in-memory version to `1`;
/// - `find_by_id` then returns the saved state;
/// - saving again at the correct version updates it and advances the
///   version again;
/// - saving at a stale version returns
///   [`RepositoryError::ConcurrencyConflict`] and leaves the stored state
///   untouched;
/// - `delete` removes the aggregate.
pub async fn run<R>(repo: &R) -> Result<(), Box<dyn std::error::Error>>
where
    R: Repository<ContractAggregate>,
{
    let id = Uuid::now_v7();

    assert!(
        repo.find_by_id(&id).await?.is_none(),
        "find_by_id on a never-saved id must return None"
    );

    let mut aggregate = ContractAggregate::new(id, "first");
    repo.save(&mut aggregate).await?;
    assert_eq!(
        aggregate.version(),
        1,
        "save must advance a newly created aggregate's version to 1"
    );

    let loaded = repo
        .find_by_id(&id)
        .await?
        .ok_or("saved aggregate not found by find_by_id")?;
    assert_eq!(loaded.id(), &id);
    assert_eq!(loaded.label(), "first");
    assert_eq!(loaded.version(), 1);

    // A repository must never round-trip pending events through storage:
    // what comes back from `find_by_id` is a settled snapshot, not a
    // replay of what the caller had pending before `save`.
    assert!(
        loaded.pending_events().is_empty(),
        "a loaded aggregate must never carry pending events"
    );

    let mut to_update = loaded;
    to_update.relabel("second");
    repo.save(&mut to_update).await?;
    assert_eq!(
        to_update.version(),
        2,
        "a second save must advance the version again"
    );

    let loaded = repo
        .find_by_id(&id)
        .await?
        .ok_or("updated aggregate not found by find_by_id")?;
    assert_eq!(loaded.label(), "second");
    assert_eq!(loaded.version(), 2);

    // A stale write (the caller's in-memory copy still thinks it's at
    // version 1) must be rejected without mutating storage.
    let mut stale = ContractAggregate::new(id, "stale");
    stale.set_version(1);
    let result = repo.save(&mut stale).await;
    match result {
        Err(RepositoryError::ConcurrencyConflict { expected, actual }) => {
            assert_eq!(expected, 1);
            assert_eq!(actual, Some(2));
        }
        other => {
            panic!("expected RepositoryError::ConcurrencyConflict from a stale save, got {other:?}")
        }
    }
    let loaded = repo
        .find_by_id(&id)
        .await?
        .ok_or("aggregate disappeared after a rejected stale save")?;
    assert_eq!(
        loaded.label(),
        "second",
        "a rejected stale save must not mutate the stored state"
    );

    repo.delete(&id).await?;
    assert!(
        repo.find_by_id(&id).await?.is_none(),
        "delete must remove the aggregate"
    );

    Ok(())
}
