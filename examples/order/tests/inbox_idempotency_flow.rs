use pharos_app::{IdempotencyDecision, InboxStatus, InboxStore};
use pharos_memory::InMemoryInboxStore;
use uuid::Uuid;

#[tokio::test]
async fn inbox_store_makes_order_event_consumption_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let inbox = InMemoryInboxStore::new();
    let message_id = Uuid::now_v7();
    let consumer = "billing-projection";

    assert_eq!(
        inbox.begin_processing(message_id, consumer).await?,
        IdempotencyDecision::StartProcessing
    );
    assert_eq!(
        inbox.begin_processing(message_id, consumer).await?,
        IdempotencyDecision::AlreadyProcessing
    );

    inbox
        .mark_failed(
            message_id,
            consumer,
            "payment service unavailable".to_string(),
        )
        .await?;
    assert_eq!(
        inbox.begin_processing(message_id, consumer).await?,
        IdempotencyDecision::RetryPreviousFailure
    );

    inbox.mark_completed(message_id, consumer).await?;
    assert_eq!(
        inbox.begin_processing(message_id, consumer).await?,
        IdempotencyDecision::AlreadyCompleted
    );

    let record = inbox
        .get(message_id, consumer)
        .await?
        .ok_or("expected inbox record")?;
    assert_eq!(record.status, InboxStatus::Completed);
    assert_eq!(record.last_error, None);

    Ok(())
}
