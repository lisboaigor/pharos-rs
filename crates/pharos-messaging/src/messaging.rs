use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use thiserror::Error;
use uuid::Uuid;

/// Broker message representation used by publisher and consumer adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Unique message identifier. Generated as UUID v7.
    pub message_id: Uuid,
    /// Broker topic, queue, or subject.
    pub topic: String,
    /// Optional partition/routing key.
    pub key: Option<String>,
    /// Message headers propagated to the broker.
    pub headers: BTreeMap<String, String>,
    /// Serialized message body.
    pub payload: Vec<u8>,
    /// Payload content type.
    pub content_type: String,
}

impl Message {
    /// Creates a new message for a topic.
    pub fn new(
        topic: impl Into<String>,
        payload: Vec<u8>,
        content_type: impl Into<String>,
    ) -> Self {
        Self {
            message_id: Uuid::now_v7(),
            topic: topic.into(),
            key: None,
            headers: BTreeMap::new(),
            payload,
            content_type: content_type.into(),
        }
    }

    /// Sets the routing key.
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Adds a header.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// Message delivered to a consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// Delivered message.
    pub message: Message,
    /// Number of delivery attempts observed by the adapter.
    pub attempt: u32,
}

impl Delivery {
    /// Creates a first-attempt delivery.
    pub fn new(message: Message) -> Self {
        Self {
            message,
            attempt: 1,
        }
    }
}

/// Error returned by messaging adapters.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MessagingError {
    /// Publishing failed; the source carries the original broker error.
    #[error("publish failed: {0}")]
    Publish(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Consuming failed; the source carries the original broker error.
    #[error("consume failed: {0}")]
    Consume(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Acknowledgement failed; the source carries the original broker error.
    #[error("ack failed: {0}")]
    Ack(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Negative acknowledgement failed; the source carries the original broker error.
    #[error("nack failed: {0}")]
    Nack(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Publishing failed because the broker rejected the message itself —
    /// too large, malformed, an invalid key — rather than because the
    /// broker was unreachable. Unlike [`Self::Publish`], no number of
    /// retries makes this succeed; see [`Self::failure_kind`].
    #[error("publish rejected (poisoned message): {0}")]
    PublishPoisoned(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl MessagingError {
    /// Wraps any `Error + Send + Sync + 'static` as a publish failure.
    pub fn publish(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Publish(Box::new(e))
    }
    /// Wraps any `Error + Send + Sync + 'static` as a consume failure.
    pub fn consume(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Consume(Box::new(e))
    }
    /// Wraps any `Error + Send + Sync + 'static` as an ack failure.
    pub fn ack(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Ack(Box::new(e))
    }
    /// Wraps any `Error + Send + Sync + 'static` as a nack failure.
    pub fn nack(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Nack(Box::new(e))
    }
    /// Wraps any `Error + Send + Sync + 'static` as a publish failure the
    /// broker attributes to the message itself, not to reachability. Use
    /// this from an adapter that can tell the two apart (the broker's own
    /// "too large"/"malformed" response, as opposed to a connection error);
    /// [`RetryPolicy::decide_for`] dead-letters it immediately instead of
    /// spending the normal retry budget on a message that will never
    /// succeed.
    pub fn publish_poisoned(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::PublishPoisoned(Box::new(e))
    }

    /// Classifies this error for [`RetryPolicy::decide_for`].
    ///
    /// Every variant here defaults to [`FailureKind::Transient`] except
    /// [`Self::PublishPoisoned`] — most adapters currently have no way to
    /// distinguish a broker outage from a rejected payload, so treating an
    /// unclassified failure as transient (retry, then eventually
    /// dead-letter on attempt-count alone) preserves prior behavior exactly.
    /// Adapters that *can* tell the difference should use
    /// [`Self::publish_poisoned`] to opt into immediate dead-lettering.
    pub fn failure_kind(&self) -> FailureKind {
        match self {
            Self::PublishPoisoned(_) => FailureKind::Poison,
            _ => FailureKind::Transient,
        }
    }
}

/// Classification of a delivery failure, used by [`RetryPolicy::decide_for`]
/// to tell a broker outage from a message that can never be delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// A transport/infrastructure problem — connection refused, timeout,
    /// broker unavailable — likely to succeed on retry.
    Transient,
    /// The message itself cannot be delivered no matter how many times it
    /// is retried: a payload the broker rejects as too large or malformed,
    /// a routing key it refuses. Retrying wastes the attempt budget (and,
    /// with [`OutboxDispatcher`] lane ordering, blocks every other message
    /// sharing its key) on something that will never succeed.
    ///
    /// [`OutboxDispatcher`]: crate::outbox_dispatcher::OutboxDispatcher
    Poison,
}

/// Publishes messages to an external broker or broker-like adapter.
pub trait MessagePublisher: Send + Sync + 'static {
    /// Publishes one message.
    fn publish(&self, message: Message) -> impl Future<Output = Result<(), MessagingError>> + Send;
    /// Publishes several messages.
    ///
    /// The default implementation calls [`Self::publish`] once per message,
    /// in order, stopping at the first error — it exists so every publisher
    /// gets a working `publish_batch` for free, not because a loop over
    /// `publish` is fast. A broker client with a real batch/pipeline API
    /// (a Kafka producer's batching, a Redis pipeline) should override this
    /// to actually use it; [`OutboxDispatcher`] currently calls `publish`
    /// once per outbox message regardless, so overriding this alone does
    /// not yet change outbox throughput — see the dispatcher's own
    /// documentation for that gap.
    ///
    /// [`OutboxDispatcher`]: crate::outbox_dispatcher::OutboxDispatcher
    fn publish_batch(
        &self,
        messages: Vec<Message>,
    ) -> impl Future<Output = Result<(), MessagingError>> + Send {
        async move {
            for message in messages {
                self.publish(message).await?;
            }
            Ok(())
        }
    }
}

/// Consumes messages from an external broker or broker-like adapter.
pub trait MessageConsumer: Send + Sync + 'static {
    /// Gets the next available message for a topic.
    fn next(
        &self,
        topic: &str,
    ) -> impl Future<Output = Result<Option<Delivery>, MessagingError>> + Send;
}

/// Acknowledges or rejects a delivered message.
pub trait MessageAcknowledger: Send + Sync + 'static {
    /// Acknowledges successful processing.
    fn ack(&self, delivery: &Delivery) -> impl Future<Output = Result<(), MessagingError>> + Send;
    /// Rejects processing and indicates whether the message should be retried.
    fn nack(
        &self,
        delivery: &Delivery,
        requeue: bool,
    ) -> impl Future<Output = Result<(), MessagingError>> + Send;
}

// Shared handles delegate, so an `Arc<P>` can be cloned into dispatchers,
// consumers, and tests without wrapper types.
impl<P: MessagePublisher> MessagePublisher for std::sync::Arc<P> {
    fn publish(&self, message: Message) -> impl Future<Output = Result<(), MessagingError>> + Send {
        (**self).publish(message)
    }
    // Forwarded explicitly (rather than relying on the trait's default) so
    // an inner `P` that overrides `publish_batch` for real batching is
    // actually used through the `Arc` wrapper, instead of silently falling
    // back to a per-message loop.
    fn publish_batch(
        &self,
        messages: Vec<Message>,
    ) -> impl Future<Output = Result<(), MessagingError>> + Send {
        (**self).publish_batch(messages)
    }
}

impl<C: MessageConsumer> MessageConsumer for std::sync::Arc<C> {
    fn next(
        &self,
        topic: &str,
    ) -> impl Future<Output = Result<Option<Delivery>, MessagingError>> + Send {
        (**self).next(topic)
    }
}

impl<A: MessageAcknowledger> MessageAcknowledger for std::sync::Arc<A> {
    fn ack(&self, delivery: &Delivery) -> impl Future<Output = Result<(), MessagingError>> + Send {
        (**self).ack(delivery)
    }
    fn nack(
        &self,
        delivery: &Delivery,
        requeue: bool,
    ) -> impl Future<Output = Result<(), MessagingError>> + Send {
        (**self).nack(delivery, requeue)
    }
}

/// Decision produced by a retry policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Retry after the provided delay.
    RetryAfter(Duration),
    /// Stop retrying and send the message to a dead-letter path when available.
    DeadLetter,
}

/// Strategy used to compute the delay between retry attempts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BackoffStrategy {
    /// Wait a constant `delay` before every retry.
    Fixed {
        /// Delay applied before each retry.
        delay: Duration,
    },
    /// Grow the delay geometrically, capped at `max`, with optional jitter.
    ///
    /// The delay for attempt `n` (1-based) is
    /// `min(base * multiplier^(n - 1), max)`. With `jitter` enabled the result
    /// is scaled by a random factor in `[0.5, 1.0]` to avoid synchronized
    /// retries ("thundering herd") across many workers.
    Exponential {
        /// Delay before the first retry.
        base: Duration,
        /// Growth factor applied per attempt.
        multiplier: f64,
        /// Upper bound on the computed delay.
        max: Duration,
        /// Whether to apply randomized jitter.
        jitter: bool,
    },
}

impl BackoffStrategy {
    /// Computes the delay before the retry following `attempt` (1-based).
    fn delay_for(&self, attempt: u32) -> Duration {
        match *self {
            BackoffStrategy::Fixed { delay } => delay,
            BackoffStrategy::Exponential {
                base,
                multiplier,
                max,
                jitter,
            } => {
                let exponent = attempt.saturating_sub(1);
                let factor = multiplier.max(1.0).powi(exponent as i32);
                let raw = base.as_secs_f64() * factor;
                let capped = raw.min(max.as_secs_f64());
                let scaled = if jitter {
                    capped * jitter_factor()
                } else {
                    capped
                };
                Duration::from_secs_f64(scaled.max(0.0))
            }
        }
    }
}

/// Returns a random scaling factor in `[0.5, 1.0]`.
///
/// Uses a real (thread-local) RNG so workers that compute a delay at the same
/// instant still spread out, unlike a clock-derived factor.
fn jitter_factor() -> f64 {
    0.5 + 0.5 * fastrand::f64()
}

/// Bounded retry policy with a configurable backoff strategy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// Maximum number of attempts, including the first delivery.
    pub max_attempts: u32,
    /// Strategy used to compute the delay before each retry.
    pub backoff: BackoffStrategy,
}

impl RetryPolicy {
    /// Creates a retry policy with a fixed delay between attempts.
    pub fn new(max_attempts: u32, delay: Duration) -> Self {
        Self {
            max_attempts,
            backoff: BackoffStrategy::Fixed { delay },
        }
    }

    /// Creates a retry policy with exponential backoff and jitter.
    pub fn exponential(max_attempts: u32, base: Duration, multiplier: f64, max: Duration) -> Self {
        Self {
            max_attempts,
            backoff: BackoffStrategy::Exponential {
                base,
                multiplier,
                max,
                jitter: true,
            },
        }
    }

    /// Returns whether the next attempt should be retried or dead-lettered.
    ///
    /// Looks only at the attempt count — every failure is treated as
    /// possibly transient. Prefer [`Self::decide_for`] when a
    /// [`FailureKind`] is available, so a poisoned message dead-letters
    /// immediately instead of consuming the full retry budget.
    pub fn decide(&self, attempt: u32) -> RetryDecision {
        if attempt < self.max_attempts {
            RetryDecision::RetryAfter(self.backoff.delay_for(attempt))
        } else {
            RetryDecision::DeadLetter
        }
    }

    /// Like [`Self::decide`], but dead-letters immediately on
    /// [`FailureKind::Poison`] regardless of attempts remaining — no delay
    /// makes a poisoned message deliverable, so there is nothing to gain by
    /// spending the retry budget on it (and, in [`OutboxDispatcher`] lanes
    /// with head-of-line blocking, every retry it does spend holds up the
    /// rest of its key).
    ///
    /// [`OutboxDispatcher`]: crate::outbox_dispatcher::OutboxDispatcher
    pub fn decide_for(&self, attempt: u32, failure: FailureKind) -> RetryDecision {
        match failure {
            FailureKind::Poison => RetryDecision::DeadLetter,
            FailureKind::Transient => self.decide(attempt),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_uses_uuid_v7_and_preserves_headers() {
        let message = Message::new("orders", b"{}".to_vec(), "application/json")
            .with_key("order-1")
            .with_header("correlation_id", "corr-1");

        assert_eq!(message.message_id.get_version_num(), 7);
        assert_eq!(message.topic, "orders");
        assert_eq!(message.key.as_deref(), Some("order-1"));
        assert_eq!(
            message.headers.get("correlation_id").map(String::as_str),
            Some("corr-1")
        );
    }

    #[test]
    fn retry_policy_dead_letters_after_max_attempts() {
        let policy = RetryPolicy::new(3, Duration::from_secs(2));

        assert_eq!(
            policy.decide(1),
            RetryDecision::RetryAfter(Duration::from_secs(2))
        );
        assert_eq!(
            policy.decide(2),
            RetryDecision::RetryAfter(Duration::from_secs(2))
        );
        assert_eq!(policy.decide(3), RetryDecision::DeadLetter);
    }

    #[test]
    fn decide_for_dead_letters_a_poisoned_message_on_the_first_attempt() {
        let policy = RetryPolicy::new(5, Duration::from_secs(2));

        // Plenty of retry budget left (attempt 1 of 5) — a transient failure
        // would still retry here (as `decide` alone does).
        assert_eq!(
            policy.decide(1),
            RetryDecision::RetryAfter(Duration::from_secs(2))
        );
        // But classified as poison, no delay ever makes it deliverable:
        // dead-letter immediately instead of spending the retry budget.
        assert_eq!(
            policy.decide_for(1, FailureKind::Poison),
            RetryDecision::DeadLetter
        );
    }

    #[test]
    fn decide_for_treats_transient_failures_like_decide() {
        let policy = RetryPolicy::new(3, Duration::from_secs(2));

        assert_eq!(
            policy.decide_for(1, FailureKind::Transient),
            RetryDecision::RetryAfter(Duration::from_secs(2))
        );
        assert_eq!(
            policy.decide_for(3, FailureKind::Transient),
            RetryDecision::DeadLetter
        );
    }

    #[test]
    fn messaging_error_classifies_publish_poisoned_and_defaults_others_to_transient() {
        let poisoned = MessagingError::publish_poisoned(std::io::Error::other("payload too large"));
        assert_eq!(poisoned.failure_kind(), FailureKind::Poison);

        let transient = MessagingError::publish(std::io::Error::other("connection refused"));
        assert_eq!(transient.failure_kind(), FailureKind::Transient);
    }

    #[test]
    fn exponential_backoff_grows_and_caps() {
        let policy = RetryPolicy {
            max_attempts: 10,
            backoff: BackoffStrategy::Exponential {
                base: Duration::from_millis(100),
                multiplier: 2.0,
                max: Duration::from_secs(1),
                jitter: false,
            },
        };

        assert_eq!(
            policy.decide(1),
            RetryDecision::RetryAfter(Duration::from_millis(100))
        );
        assert_eq!(
            policy.decide(2),
            RetryDecision::RetryAfter(Duration::from_millis(200))
        );
        assert_eq!(
            policy.decide(3),
            RetryDecision::RetryAfter(Duration::from_millis(400))
        );
        // 100ms * 2^4 = 1600ms, capped at the 1s max.
        assert_eq!(
            policy.decide(5),
            RetryDecision::RetryAfter(Duration::from_secs(1))
        );
    }

    #[test]
    fn exponential_backoff_jitter_stays_within_bounds() {
        let policy =
            RetryPolicy::exponential(5, Duration::from_millis(200), 2.0, Duration::from_secs(10));
        // attempt 3 → 200ms * 2^2 = 800ms, scaled by jitter in [0.5, 1.0].
        match policy.decide(3) {
            RetryDecision::RetryAfter(delay) => {
                assert!(delay >= Duration::from_millis(400));
                assert!(delay <= Duration::from_millis(800));
            }
            other => panic!("expected RetryAfter, got {other:?}"),
        }
    }
}
