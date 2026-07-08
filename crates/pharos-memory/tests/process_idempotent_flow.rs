//! End-to-end idempotent-consumer flow over the in-memory adapters.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use pharos_app::{
    Delivery, InboxStore, Message, MessagePublisher, ProcessError, ProcessOutcome,
    process_idempotent,
};
use pharos_memory::{InMemoryInboxStore, InMemoryMessageBroker};

#[derive(Debug, thiserror::Error)]
#[error("business failure")]
struct BusinessFailure;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn deliver(broker: &InMemoryMessageBroker) -> Result<Delivery, Box<dyn std::error::Error>> {
    broker
        .publish(Message::new("orders", b"{}".to_vec(), "application/json"))
        .await?;
    use pharos_app::MessageConsumer;
    Ok(broker.next("orders").await?.ok_or("expected a delivery")?)
}

#[tokio::test]
async fn processes_once_and_skips_duplicates() -> TestResult {
    let inbox = InMemoryInboxStore::new();
    let broker = InMemoryMessageBroker::new();
    let delivery = deliver(&broker).await?;
    let runs = Arc::new(AtomicU32::new(0));

    let outcome = process_idempotent(&inbox, &broker, "billing", &delivery, |_d| async {
        runs.fetch_add(1, Ordering::SeqCst);
        Ok::<(), BusinessFailure>(())
    })
    .await?;
    assert_eq!(outcome, ProcessOutcome::Processed);

    // Redelivery of the same message id: handler must not run again.
    let outcome = process_idempotent(&inbox, &broker, "billing", &delivery, |_d| async {
        runs.fetch_add(1, Ordering::SeqCst);
        Ok::<(), BusinessFailure>(())
    })
    .await?;
    assert_eq!(outcome, ProcessOutcome::SkippedDuplicate);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn failure_is_marked_nacked_and_retryable() -> TestResult {
    let inbox = InMemoryInboxStore::new();
    let broker = InMemoryMessageBroker::new();
    let delivery = deliver(&broker).await?;

    let result = process_idempotent(&inbox, &broker, "billing", &delivery, |_d| async {
        Err::<(), _>(BusinessFailure)
    })
    .await;
    assert!(matches!(result, Err(ProcessError::Handler(_))));

    // The failure was recorded, so the retry path runs the handler again…
    let record = inbox
        .get(delivery.message.message_id, "billing")
        .await?
        .ok_or("expected an inbox record")?;
    assert_eq!(record.last_error.as_deref(), Some("business failure"));

    // …and a successful retry completes normally.
    let outcome = process_idempotent(&inbox, &broker, "billing", &delivery, |_d| async {
        Ok::<(), BusinessFailure>(())
    })
    .await?;
    assert_eq!(outcome, ProcessOutcome::Processed);
    Ok(())
}
