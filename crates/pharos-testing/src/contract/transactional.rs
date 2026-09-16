//! Conformance suite for [`TransactionalStore`] + [`TransactionalRepository`],
//! and for the [`save_and_enqueue_in`] seam built on top of them.
use pharos_app::{
    Message, OutboxMessage, OutboxRepository, SaveAndEnqueueError, TransactionalRepository,
    TransactionalStore, save_and_enqueue_in,
};
use pharos_core::{AggregateRoot, Repository, RepositoryError};
use uuid::Uuid;

use super::fixtures::ContractAggregate;

/// Runs the [`TransactionalStore`] + [`TransactionalRepository`]
/// conformance suite: a committed transaction's write is visible, and a
/// transaction dropped without committing (the caller's error path) leaves
/// no trace — the same "one transaction, one outcome" guarantee
/// [`TransactionalStore::Tx`] taking the handle by value in `commit` is
/// meant to enforce at the type level.
///
/// ```ignore
/// #[tokio::test]
/// async fn transactional_repository_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::transactional::transactional_repository(&repo, &store).await
/// }
/// ```
pub async fn transactional_repository<R, S>(
    repo: &R,
    store: &S,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: Repository<ContractAggregate> + TransactionalRepository<ContractAggregate, S>,
    S: TransactionalStore,
{
    let committed_id = Uuid::now_v7();
    let mut committed = ContractAggregate::new(committed_id, "tx-committed");
    let mut tx = store.begin().await?;
    repo.save_in_tx(&mut tx, &mut committed).await?;
    store.commit(tx).await?;

    let loaded = repo
        .find_by_id(&committed_id)
        .await?
        .ok_or("a committed transactional save must be visible through Repository::find_by_id")?;
    assert_eq!(loaded.label(), "tx-committed");

    let dropped_id = Uuid::now_v7();
    let mut dropped = ContractAggregate::new(dropped_id, "tx-dropped");
    {
        let mut tx = store.begin().await?;
        repo.save_in_tx(&mut tx, &mut dropped).await?;
        // `tx` is dropped here without a call to `commit` — the caller's
        // error path. Nothing this transaction staged may be visible.
    }
    assert!(
        repo.find_by_id(&dropped_id).await?.is_none(),
        "a transaction dropped without commit must leave no trace"
    );

    Ok(())
}

/// Runs the [`save_and_enqueue_in`] atomicity suite: the aggregate save and
/// the outbox insert it composes must become visible together or not at
/// all.
///
/// `outbox` must read back what `store`'s [`TransactionalStore::insert_outbox_in_tx`]
/// writes — in a real adapter these are backed by the same underlying
/// connection/table (e.g. `pharos-memory`'s `InMemoryUnitOfWork` and
/// `InMemoryOutboxRepository` sharing one `Arc`), exactly like a
/// `DefaultAggregateStore` built with `with_outbox` expects.
///
/// The failure path forces [`RepositoryError::ConcurrencyConflict`] inside
/// the transaction (a deliberately stale `expected_version`) and asserts
/// that *neither* the aggregate *nor* the outbox message it would have
/// enqueued ends up persisted — proving the two writes share one
/// transaction rather than two independent ones. This is the strongest
/// failure this suite can inject through the trait alone: every later
/// failure point (the outbox insert itself, the commit) is backend-specific
/// with no portable way to fail it on demand.
///
/// ```ignore
/// #[tokio::test]
/// async fn save_and_enqueue_atomicity_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::transactional::save_and_enqueue_atomicity(&repo, &store, &outbox).await
/// }
/// ```
pub async fn save_and_enqueue_atomicity<R, S, O>(
    repo: &R,
    store: &S,
    outbox: &O,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: Repository<ContractAggregate> + TransactionalRepository<ContractAggregate, S>,
    S: TransactionalStore,
    O: OutboxRepository,
{
    let map_event =
        |event: &super::fixtures::ContractEvent| -> Result<Message, std::convert::Infallible> {
            Ok(Message::new(
                "ContractEvent",
                event.label.clone().into_bytes(),
                "text/plain",
            ))
        };

    // Happy path: one call, both writes visible immediately after.
    let ok_id = Uuid::now_v7();
    let mut ok_aggregate = ContractAggregate::new(ok_id, "atomic-ok");
    save_and_enqueue_in(store, repo, &mut ok_aggregate, map_event).await?;

    assert_eq!(ok_aggregate.version(), 1);
    assert!(
        ok_aggregate.pending_events().is_empty(),
        "events must be drained only after the transaction commits"
    );
    let loaded = repo
        .find_by_id(&ok_id)
        .await?
        .ok_or("the aggregate saved by save_and_enqueue_in must be visible")?;
    assert_eq!(loaded.label(), "atomic-ok");
    let pending: Vec<OutboxMessage> = outbox.pending(100).await?;
    assert!(
        pending.iter().any(|m| m.message.topic == "ContractEvent"),
        "the outbox message enqueued by save_and_enqueue_in must be visible in the same read \
         that sees the committed aggregate"
    );

    // Failure path: a stale `expected_version` makes `save_in_tx` reject the
    // write inside the transaction, before any outbox insert runs.
    let fail_id = Uuid::now_v7();
    let mut fail_aggregate = ContractAggregate::new(fail_id, "atomic-fail");
    fail_aggregate.set_version(7); // never saved, so any nonzero version is stale
    let result = save_and_enqueue_in(store, repo, &mut fail_aggregate, map_event).await;
    assert!(
        matches!(
            result,
            Err(SaveAndEnqueueError::Repository(
                RepositoryError::ConcurrencyConflict { .. }
            ))
        ),
        "a stale expected_version must surface as SaveAndEnqueueError::Repository(ConcurrencyConflict)"
    );
    assert!(
        repo.find_by_id(&fail_id).await?.is_none(),
        "a rejected save must not create the aggregate"
    );
    let pending_after_failure: Vec<OutboxMessage> = outbox.pending(100).await?;
    assert!(
        !pending_after_failure
            .iter()
            .any(|m| m.message.topic == "ContractEvent" && m.message.payload == b"atomic-fail"),
        "a rejected save must never leave a dangling outbox message behind"
    );
    assert_eq!(
        fail_aggregate.version(),
        7,
        "a rejected save must revert the aggregate's in-memory version for a clean retry"
    );
    assert_eq!(
        fail_aggregate.pending_events().len(),
        1,
        "a rejected save must keep the pending event for a clean retry"
    );

    Ok(())
}
