//! Conformance suite for [`OutboxRepository`].
use pharos_app::{Message, OutboxMessage, OutboxRepository, OutboxStatus};

/// Runs the [`OutboxRepository`] conformance suite against `outbox`.
///
/// Covers the full lifecycle a dispatcher drives a message through:
/// `insert` → claimed by `pending` → `record_attempt` → terminal via either
/// `mark_published` (leaves `pending` and `failed` for good) or
/// `mark_failed` (visible in `failed` until `mark_dead_lettered` retires
/// it). Also exercises the default `insert_many`.
///
/// ```ignore
/// #[tokio::test]
/// async fn outbox_repository_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::outbox::run(&outbox).await
/// }
/// ```
pub async fn run<O>(outbox: &O) -> Result<(), Box<dyn std::error::Error>>
where
    O: OutboxRepository,
{
    let published = OutboxMessage::new(Message::new("orders", b"a".to_vec(), "application/json"));
    let published_id = published.id;
    outbox.insert(published).await?;

    let claimed = outbox.pending(10).await?;
    assert!(
        claimed.iter().any(|m| m.id == published_id),
        "a freshly inserted message must be claimable by pending"
    );

    outbox.record_attempt(published_id).await?;
    outbox.mark_published(published_id).await?;

    let after_publish = outbox.pending(10).await?;
    assert!(
        !after_publish.iter().any(|m| m.id == published_id),
        "a published message must never be claimable again"
    );
    let never_failed = outbox.failed(10).await?;
    assert!(
        !never_failed.iter().any(|m| m.id == published_id),
        "a published message must never show up as failed"
    );

    let failing = OutboxMessage::new(Message::new("orders", b"b".to_vec(), "application/json"));
    let failing_id = failing.id;
    outbox.insert(failing).await?;
    outbox.pending(10).await?; // claim it, mirroring a real dispatch attempt
    outbox
        .mark_failed(failing_id, "broker unreachable".to_string())
        .await?;

    let failed = outbox.failed(10).await?;
    let record = failed
        .iter()
        .find(|m| m.id == failing_id)
        .ok_or("a message marked failed must appear in `failed`")?;
    assert_eq!(record.status, OutboxStatus::Failed);
    assert_eq!(record.last_error.as_deref(), Some("broker unreachable"));

    outbox.mark_dead_lettered(failing_id).await?;
    let failed_after_dlq = outbox.failed(10).await?;
    assert!(
        !failed_after_dlq.iter().any(|m| m.id == failing_id),
        "a dead-lettered message must no longer show up as failed"
    );

    let batch = vec![
        OutboxMessage::new(Message::new("orders", b"c".to_vec(), "application/json")),
        OutboxMessage::new(Message::new("orders", b"d".to_vec(), "application/json")),
    ];
    let batch_ids: Vec<_> = batch.iter().map(|m| m.id).collect();
    outbox.insert_many(batch).await?;
    let claimed = outbox.pending(10).await?;
    for id in batch_ids {
        assert!(
            claimed.iter().any(|m| m.id == id),
            "insert_many must make every message it was given claimable"
        );
    }

    Ok(())
}

/// Proves two concurrent `pending` calls never both claim the same message
/// — the guarantee that lets multiple dispatcher instances poll the same
/// outbox safely (`FOR UPDATE SKIP LOCKED` in `pharos-postgres`'s adapter,
/// a lease/claim in an in-memory one).
///
/// ```ignore
/// #[tokio::test]
/// async fn outbox_repository_no_double_claim() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::outbox::no_double_claim(&outbox).await
/// }
/// ```
pub async fn no_double_claim<O>(outbox: &O) -> Result<(), Box<dyn std::error::Error>>
where
    O: OutboxRepository + Sync,
{
    let message = OutboxMessage::new(Message::new("orders", b"race".to_vec(), "application/json"));
    let id = message.id;
    outbox.insert(message).await?;

    let (a, b) = futures::join!(outbox.pending(10), outbox.pending(10));
    let claims =
        a?.iter().filter(|m| m.id == id).count() + b?.iter().filter(|m| m.id == id).count();
    assert_eq!(
        claims, 1,
        "two concurrent `pending` calls must claim the message exactly once between them, not twice"
    );

    Ok(())
}
