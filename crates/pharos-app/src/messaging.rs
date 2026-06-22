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
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MessagingError {
    /// Publishing failed.
    #[error("publish failed: {0}")]
    Publish(String),
    /// Consuming failed.
    #[error("consume failed: {0}")]
    Consume(String),
    /// Acknowledgement failed.
    #[error("ack failed: {0}")]
    Ack(String),
    /// Negative acknowledgement failed.
    #[error("nack failed: {0}")]
    Nack(String),
}

/// Publishes messages to an external broker or broker-like adapter.
pub trait MessagePublisher: Send + Sync + 'static {
    /// Publishes one message.
    fn publish(&self, message: Message) -> impl Future<Output = Result<(), MessagingError>> + Send;
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

/// Returns a pseudo-random scaling factor in `[0.5, 1.0]`.
///
/// Derived from the current time to avoid pulling in an RNG dependency; this is
/// sufficient to desynchronize retries across workers.
fn jitter_factor() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    0.5 + 0.5 * (nanos as f64 / 1_000_000_000.0)
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
    pub fn decide(&self, attempt: u32) -> RetryDecision {
        if attempt < self.max_attempts {
            RetryDecision::RetryAfter(self.backoff.delay_for(attempt))
        } else {
            RetryDecision::DeadLetter
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
