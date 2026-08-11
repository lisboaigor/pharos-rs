//! Regression coverage for the poison-message requeue loop:
//! `process_idempotent` alone nacks with `requeue: true` on every handler
//! failure, forever, because nothing in it ever chooses to stop.
//! `process_idempotent_with_retry` closes that loop by consulting a
//! `RetryPolicy` against `Delivery::attempt` and dead-lettering once the
//! budget is spent.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use pharos_app::{
    DeadLetterQueue, Delivery, Message, MessageConsumer, MessagePublisher, ProcessError,
    ProcessOutcome, RetryPolicy, process_idempotent_with_retry,
};
use pharos_memory::{InMemoryDeadLetterQueue, InMemoryInboxStore, InMemoryMessageBroker};

#[derive(Debug, thiserror::Error)]
#[error("handler always fails")]
struct AlwaysFails;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn deliver(broker: &InMemoryMessageBroker) -> Result<Delivery, Box<dyn std::error::Error>> {
    broker
        .publish(Message::new("orders", b"{}".to_vec(), "application/json"))
        .await?;
    Ok(broker.next("orders").await?.ok_or("expected a delivery")?)
}

/// The bug this exists to prevent: without a retry ceiling, a message whose
/// handler can never succeed is nacked with `requeue: true` on every single
/// attempt — an unbounded loop against the consumer's own infrastructure.
/// With a bounded `RetryPolicy`, the same scenario dead-letters and stops.
#[tokio::test]
async fn a_permanently_failing_handler_is_dead_lettered_instead_of_looping_forever() -> TestResult {
    let inbox = InMemoryInboxStore::new();
    let broker = InMemoryMessageBroker::new();
    let dlq = InMemoryDeadLetterQueue::new();
    let policy = RetryPolicy::new(3, Duration::from_millis(1));
    let runs = Arc::new(AtomicU32::new(0));

    let mut delivery = deliver(&broker).await?;
    let mut outcome = None;

    // Drive the same loop a real consumer would: process, and if it comes
    // back requeued, fetch the redelivery and process it again. This must
    // terminate — that is the entire point of the test.
    for _ in 0..10 {
        let result = process_idempotent_with_retry(
            &inbox,
            &broker,
            &dlq,
            "billing",
            &delivery,
            &policy,
            |_d| {
                runs.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(AlwaysFails) }
            },
        )
        .await;

        match result {
            Err(ProcessError::Handler(_)) => {}
            other => panic!("expected a handler failure, got {other:?}"),
        }

        match broker.next("orders").await? {
            Some(redelivery) => delivery = redelivery,
            None => {
                outcome = Some(());
                break;
            }
        }
    }

    assert!(
        outcome.is_some(),
        "the loop never stopped requeuing — the poison-message bug is back"
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        3,
        "the handler must run exactly `max_attempts` times, not more"
    );
    assert_eq!(
        broker.queued_len("orders").await,
        0,
        "the final failure must not have been requeued"
    );

    let dead = dlq.list(10).await?;
    assert_eq!(
        dead.len(),
        1,
        "the message must have been dead-lettered exactly once"
    );
    assert_eq!(dead[0].attempts, 3);
    assert_eq!(dead[0].reason, "handler always fails");
    Ok(())
}

/// A handler that succeeds within the retry budget must not be dead-lettered.
#[tokio::test]
async fn a_handler_that_eventually_succeeds_is_not_dead_lettered() -> TestResult {
    let inbox = InMemoryInboxStore::new();
    let broker = InMemoryMessageBroker::new();
    let dlq = InMemoryDeadLetterQueue::new();
    let policy = RetryPolicy::new(5, Duration::from_millis(1));

    let mut delivery = deliver(&broker).await?;

    // First attempt fails.
    let result = process_idempotent_with_retry(
        &inbox,
        &broker,
        &dlq,
        "billing",
        &delivery,
        &policy,
        |_d| async { Err::<(), _>(AlwaysFails) },
    )
    .await;
    assert!(matches!(result, Err(ProcessError::Handler(_))));

    delivery = broker
        .next("orders")
        .await?
        .ok_or("expected a redelivery")?;
    assert_eq!(delivery.attempt, 2);

    // Second attempt succeeds.
    let outcome = process_idempotent_with_retry(
        &inbox,
        &broker,
        &dlq,
        "billing",
        &delivery,
        &policy,
        |_d| async { Ok::<(), AlwaysFails>(()) },
    )
    .await?;
    assert_eq!(outcome, ProcessOutcome::Processed);
    assert!(dlq.list(10).await?.is_empty());
    Ok(())
}
