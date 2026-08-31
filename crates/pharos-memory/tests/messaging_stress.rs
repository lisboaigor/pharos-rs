//! Failure-mode regression tests for the idempotent-consumer flow, run
//! against the in-memory adapters.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pharos_app::{
    DispatchConfig, IdempotencyDecision, InboxStore, Message, MessageAcknowledger, MessageConsumer,
    MessagePublisher, MessagingError, OutboxDispatcher, OutboxMessage, OutboxRepository,
    ProcessOutcome, process_idempotent,
};
use pharos_memory::{InMemoryInboxStore, InMemoryMessageBroker, InMemoryOutboxRepository};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// The dispatcher takes its repository by value and `OutboxRepository` has no
/// blanket impl for `Arc<R>`, so sharing one across dispatchers needs this.
#[derive(Clone)]
struct SharedOutbox(Arc<InMemoryOutboxRepository>);

impl OutboxRepository for SharedOutbox {
    async fn insert(&self, message: OutboxMessage) -> Result<(), pharos_app::OutboxError> {
        self.0.insert(message).await
    }
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, pharos_app::OutboxError> {
        self.0.pending(limit).await
    }
    async fn schedule_retry(
        &self,
        id: uuid::Uuid,
        delay: Duration,
    ) -> Result<(), pharos_app::OutboxError> {
        self.0.schedule_retry(id, delay).await
    }
    async fn record_attempt(&self, id: uuid::Uuid) -> Result<(), pharos_app::OutboxError> {
        self.0.record_attempt(id).await
    }
    async fn mark_published(&self, id: uuid::Uuid) -> Result<(), pharos_app::OutboxError> {
        self.0.mark_published(id).await
    }
    async fn mark_failed(
        &self,
        id: uuid::Uuid,
        error: String,
    ) -> Result<(), pharos_app::OutboxError> {
        self.0.mark_failed(id, error).await
    }
    async fn failed(&self, limit: usize) -> Result<Vec<OutboxMessage>, pharos_app::OutboxError> {
        self.0.failed(limit).await
    }
    async fn mark_dead_lettered(&self, id: uuid::Uuid) -> Result<(), pharos_app::OutboxError> {
        self.0.mark_dead_lettered(id).await
    }
}

#[derive(Debug, thiserror::Error)]
#[error("handler failed")]
struct HandlerFailed;

fn message(topic: &str, key: Option<&str>, payload: &str) -> Message {
    let mut message = Message::new(topic, payload.as_bytes().to_vec(), "application/json");
    if let Some(key) = key {
        message = message.with_key(key);
    }
    message
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. A consumer that dies mid-message: the redelivery must not be acked away.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_crashed_consumers_message_is_requeued_not_dropped() -> TestResult {
    let broker = InMemoryMessageBroker::new();
    let inbox = InMemoryInboxStore::new(); // default 300s stale lease
    let consumer = "orders-worker"; // one consumer group, several processes

    let msg = message("orders", None, "place-order");
    let message_id = msg.message_id;
    broker.publish(msg).await?;

    // ── process A picks the message up and dies after begin_processing ──
    let Some(first) = broker.next("orders").await? else {
        panic!("the broker should have had a message");
    };
    let decision = inbox.begin_processing(message_id, consumer).await?;
    assert_eq!(decision, IdempotencyDecision::StartProcessing);
    // A is SIGKILLed here: no mark_completed, no mark_failed, no ack, no nack.
    // The broker's visibility timeout expires and it redelivers.
    broker.nack(&first, true).await?;

    // ── process B receives the redelivery and runs the framework's flow ──
    let handler_calls = Arc::new(AtomicU64::new(0));
    let Some(second) = broker.next("orders").await? else {
        panic!("the redelivery should be queued");
    };
    let calls = Arc::clone(&handler_calls);
    let outcome = process_idempotent(&inbox, &broker, consumer, &second, |_| {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<(), HandlerFailed>(())
        }
    })
    .await?;

    assert_eq!(outcome, ProcessOutcome::StillProcessing);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0, "handler never ran");
    assert!(
        !broker.was_acked(message_id),
        "the framework must not ack a delivery nobody has finished processing"
    );
    assert!(
        broker.was_nacked(message_id),
        "the framework returns it to the broker instead"
    );
    assert_eq!(
        broker.queued_len("orders").await,
        1,
        "the message is still there for whoever picks it up next"
    );

    let after = inbox.begin_processing(message_id, consumer).await?;
    assert_eq!(
        after,
        IdempotencyDecision::AlreadyProcessing,
        "the record is still `processing`, exactly as it should be while the lease has not expired"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Same shape, but the lease has already expired: recovery works.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_short_stale_lease_recovers_the_same_message() -> TestResult {
    let broker = InMemoryMessageBroker::new();
    let inbox = InMemoryInboxStore::new().with_stale_after(Duration::from_millis(50));
    let consumer = "orders-worker";

    let msg = message("orders", None, "place-order");
    let message_id = msg.message_id;
    broker.publish(msg).await?;

    let Some(first) = broker.next("orders").await? else {
        panic!("the broker should have had a message");
    };
    inbox.begin_processing(message_id, consumer).await?;
    broker.nack(&first, true).await?;

    tokio::time::sleep(Duration::from_millis(120)).await;

    let handler_calls = Arc::new(AtomicU64::new(0));
    let Some(second) = broker.next("orders").await? else {
        panic!("the redelivery should be queued");
    };
    let calls = Arc::clone(&handler_calls);
    let outcome = process_idempotent(&inbox, &broker, consumer, &second, |_| {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<(), HandlerFailed>(())
        }
    })
    .await?;

    assert_eq!(outcome, ProcessOutcome::Processed);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Per-key ordering under intra-batch concurrency.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct OrderRecorder {
    seen: Arc<tokio::sync::Mutex<Vec<(String, u64)>>>,
}

impl MessagePublisher for OrderRecorder {
    async fn publish(&self, message: Message) -> Result<(), MessagingError> {
        let key = message.key.clone().unwrap_or_default();
        let seq: u64 = String::from_utf8_lossy(&message.payload)
            .parse()
            .unwrap_or(0);
        // A jittered await inside the publish is what a real broker call is.
        tokio::time::sleep(Duration::from_micros(seq % 7 * 100)).await;
        self.seen.lock().await.push((key, seq));
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_key_order_survives_intra_batch_concurrency() -> TestResult {
    let outbox = Arc::new(InMemoryOutboxRepository::new());
    for seq in 0..300u64 {
        outbox
            .insert(OutboxMessage::new(message(
                "orders",
                Some(&format!("key-{}", seq % 10)),
                &seq.to_string(),
            )))
            .await?;
        // `pending` orders by created_at; make the ordering unambiguous.
        tokio::time::sleep(Duration::from_micros(50)).await;
    }

    let seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let dispatcher = OutboxDispatcher::with_config(
        SharedOutbox(Arc::clone(&outbox)),
        OrderRecorder {
            seen: Arc::clone(&seen),
        },
        DispatchConfig::default()
            .with_batch_size(300)
            .with_concurrency(16),
    );
    let result = dispatcher.dispatch_batch().await;
    assert_eq!(result.published, 300, "every message published");

    let seen = seen.lock().await;
    let mut last_by_key: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut inversions = 0;
    for (key, seq) in seen.iter() {
        if let Some(previous) = last_by_key.get(key)
            && previous > seq
        {
            inversions += 1;
        }
        last_by_key.insert(key.clone(), *seq);
    }
    println!("publishes={} per_key_inversions={inversions}", seen.len());
    assert_eq!(inversions, 0, "per-key ordering must survive concurrency");
    Ok(())
}
