//! Failed outbox messages are parked on the DLQ exactly once.

use pharos_app::{
    DeadLetterQueue, Message, OutboxMessage, OutboxRepository, OutboxStatus,
    sweep_failed_to_dead_letter,
};
use pharos_memory::{InMemoryDeadLetterQueue, InMemoryOutboxRepository};

#[tokio::test]
async fn sweeps_failed_messages_to_the_dlq_exactly_once() -> Result<(), Box<dyn std::error::Error>>
{
    let outbox = InMemoryOutboxRepository::new();
    let dlq = InMemoryDeadLetterQueue::new();

    let failing = OutboxMessage::new(
        Message::new("orders", b"{}".to_vec(), "application/json").with_key("order-1"),
    );
    let id = failing.id;
    outbox.insert(failing).await?;
    outbox.record_attempt(id).await?;
    outbox
        .mark_failed(id, "broker unavailable".to_string())
        .await?;
    // A healthy pending message must not be swept.
    outbox
        .insert(OutboxMessage::new(Message::new(
            "orders",
            b"ok".to_vec(),
            "application/json",
        )))
        .await?;

    let swept = sweep_failed_to_dead_letter(&outbox, &dlq, 10).await?;
    assert_eq!(swept, 1);

    let parked = dlq.list(10).await?;
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].reason, "broker unavailable");
    assert_eq!(parked[0].attempts, 1);

    // The row is terminally dead_lettered: a second sweep parks nothing.
    assert_eq!(sweep_failed_to_dead_letter(&outbox, &dlq, 10).await?, 0);
    let failed_after = outbox.failed(10).await?;
    assert!(failed_after.is_empty());
    // And the pending message is untouched.
    let pending = outbox.pending(10).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, OutboxStatus::Pending);
    Ok(())
}
