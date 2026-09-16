//! Conformance suite for [`InboxStore`].
use pharos_app::{IdempotencyDecision, InboxStatus, InboxStore};
use uuid::Uuid;

/// Runs the [`InboxStore`] conformance suite against `inbox`.
///
/// Covers the full [`IdempotencyDecision`] lifecycle: a fresh message
/// starts processing, a concurrent/duplicate delivery of the same message
/// sees `AlreadyProcessing`, completion makes every later delivery see
/// `AlreadyCompleted`, and a failure makes the next delivery see
/// `RetryPreviousFailure`. Also checks that the same message id under a
/// different consumer is tracked independently — the whole point of the
/// `(message_id, consumer)` key.
///
/// ```ignore
/// #[tokio::test]
/// async fn inbox_store_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::inbox::run(&inbox).await
/// }
/// ```
pub async fn run<I>(inbox: &I) -> Result<(), Box<dyn std::error::Error>>
where
    I: InboxStore,
{
    let message_id = Uuid::now_v7();
    let consumer = "billing";

    assert_eq!(
        inbox.begin_processing(message_id, consumer).await?,
        IdempotencyDecision::StartProcessing,
        "a never-seen message must start processing"
    );
    assert_eq!(
        inbox.begin_processing(message_id, consumer).await?,
        IdempotencyDecision::AlreadyProcessing,
        "a message already being processed must not start again"
    );

    let record = inbox
        .get(message_id, consumer)
        .await?
        .ok_or("a processing record must be gettable")?;
    assert_eq!(record.message_id, message_id);
    assert_eq!(record.status, InboxStatus::Processing);

    inbox.mark_completed(message_id, consumer).await?;
    assert_eq!(
        inbox.begin_processing(message_id, consumer).await?,
        IdempotencyDecision::AlreadyCompleted,
        "a completed message must never be reprocessed"
    );
    let completed = inbox
        .get(message_id, consumer)
        .await?
        .ok_or("a completed record must be gettable")?;
    assert_eq!(completed.status, InboxStatus::Completed);

    let failing_id = Uuid::now_v7();
    inbox.begin_processing(failing_id, consumer).await?;
    inbox
        .mark_failed(failing_id, consumer, "handler panicked".to_string())
        .await?;
    assert_eq!(
        inbox.begin_processing(failing_id, consumer).await?,
        IdempotencyDecision::RetryPreviousFailure,
        "a failed message must be retryable on the next delivery"
    );
    let failed = inbox
        .get(failing_id, consumer)
        .await?
        .ok_or("a failed record must be gettable")?;
    assert_eq!(failed.last_error.as_deref(), Some("handler panicked"));

    // The retry actually succeeding must clear the failure state.
    inbox.mark_completed(failing_id, consumer).await?;
    assert_eq!(
        inbox.begin_processing(failing_id, consumer).await?,
        IdempotencyDecision::AlreadyCompleted
    );

    // A different consumer sees its own independent record for the same
    // message id — the inbox key is (message_id, consumer), not just
    // message_id.
    assert_eq!(
        inbox.begin_processing(message_id, "shipping").await?,
        IdempotencyDecision::StartProcessing,
        "a different consumer must track the same message id independently"
    );

    Ok(())
}
