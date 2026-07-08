//! Resilience decorators for [`EventHandler`]s.
//!
//! Cross-cutting failure handling composes around handlers the same way Tower
//! layers compose around command handlers — by wrapping, never by editing the
//! handler:
//!
//! - `Retrying` (feature `retry`) re-runs a failing handler under a
//!   [`RetryPolicy`](pharos_messaging::RetryPolicy), honoring the backoff
//!   between attempts.
//! - [`DeadLettering`] parks the event on a [`DeadLetterQueue`] once the inner
//!   handler fails, so publishing continues and no event is ever lost.
//!
//! Compose them dead-letter-outermost, so retries are exhausted before the
//! event is parked:
//!
//! ```ignore
//! bus.register::<OrderEvent, _>(DeadLettering::new(
//!     Retrying::new(UpdateInventory, RetryPolicy::exponential(3, base, 2.0, max)),
//!     dlq,
//!     "order-events",
//!     map_event, // Fn(&OrderEvent) -> Message
//! ));
//! ```

use pharos_core::DomainEvent;
use pharos_messaging::dead_letter::{DeadLetterMessage, DeadLetterQueue};
use pharos_messaging::messaging::Message;
#[cfg(feature = "retry")]
use pharos_messaging::messaging::{RetryDecision, RetryPolicy};

use crate::event_handler::EventHandler;

/// Retries the inner handler under a [`RetryPolicy`], sleeping the policy's
/// backoff between attempts. Returns the last error once the budget is spent.
///
/// Requires the `retry` feature (pulls in Tokio's timer).
#[cfg(feature = "retry")]
#[derive(Debug, Clone)]
pub struct Retrying<H> {
    inner: H,
    policy: RetryPolicy,
}

#[cfg(feature = "retry")]
impl<H> Retrying<H> {
    /// Wraps a handler with a retry policy.
    pub fn new(inner: H, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }
}

#[cfg(feature = "retry")]
impl<E, H> EventHandler<E> for Retrying<H>
where
    E: DomainEvent,
    H: EventHandler<E>,
{
    type Error = H::Error;

    async fn handle(&self, event: &E) -> Result<(), Self::Error> {
        let mut attempt: u32 = 1;
        loop {
            match self.inner.handle(event).await {
                Ok(()) => return Ok(()),
                Err(error) => match self.policy.decide(attempt) {
                    RetryDecision::RetryAfter(delay) => {
                        metrics::counter!(
                            "pharos.event_handler.retried",
                            "event_type" => event.event_type()
                        )
                        .increment(1);
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    }
                    RetryDecision::DeadLetter => return Err(error),
                },
            }
        }
    }
}

/// Error returned by [`DeadLettering`] when parking the event itself fails.
#[derive(Debug, thiserror::Error)]
#[error("handler failed ({handler_error}) and dead-lettering also failed: {dead_letter_error}")]
pub struct DeadLetteringError {
    /// The inner handler's failure, preserved as context.
    pub handler_error: String,
    /// The dead-letter queue failure.
    #[source]
    pub dead_letter_error: pharos_messaging::dead_letter::DeadLetterError,
}

/// Parks the event on a [`DeadLetterQueue`] when the inner handler fails, then
/// reports success so the remaining handlers (and the publisher) continue.
///
/// The event is never lost: it becomes a [`DeadLetterMessage`] carrying the
/// handler's error as the reason, ready for offline inspection and replay.
/// Only a failure of the dead-letter queue itself is propagated.
pub struct DeadLettering<H, Q, F> {
    inner: H,
    dlq: Q,
    topic: String,
    map_event: F,
}

impl<H, Q, F> DeadLettering<H, Q, F> {
    /// Wraps a handler with a dead-letter fallback.
    ///
    /// `map_event` serializes the domain event into the durable [`Message`]
    /// stored on the queue (same shape as the outbox mapping).
    pub fn new(inner: H, dlq: Q, topic: impl Into<String>, map_event: F) -> Self {
        Self {
            inner,
            dlq,
            topic: topic.into(),
            map_event,
        }
    }
}

impl<E, H, Q, F> EventHandler<E> for DeadLettering<H, Q, F>
where
    E: DomainEvent,
    H: EventHandler<E>,
    Q: DeadLetterQueue,
    F: Fn(&E) -> Message + Send + Sync + 'static,
{
    type Error = DeadLetteringError;

    async fn handle(&self, event: &E) -> Result<(), Self::Error> {
        let Err(error) = self.inner.handle(event).await else {
            return Ok(());
        };

        let mut message = (self.map_event)(event);
        message.topic = self.topic.clone();
        let dead = DeadLetterMessage::new(message, error.to_string(), 1);

        self.dlq
            .dead_letter(dead)
            .await
            .map_err(|dead_letter_error| DeadLetteringError {
                handler_error: error.to_string(),
                dead_letter_error,
            })?;

        metrics::counter!(
            "pharos.event_handler.dead_lettered",
            "event_type" => event.event_type()
        )
        .increment(1);
        tracing::warn!(
            event_type = event.event_type(),
            aggregate_id = event.aggregate_id(),
            error = %error,
            "event handler failed; event parked on the dead-letter queue"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use chrono::{DateTime, Utc};
    use pharos_messaging::dead_letter::DeadLetterError;
    use tokio::sync::Mutex;

    use super::*;

    #[derive(Debug)]
    struct Ping {
        occurred_at: DateTime<Utc>,
    }

    impl DomainEvent for Ping {
        fn event_type(&self) -> &'static str {
            "Ping"
        }
        fn occurred_at(&self) -> DateTime<Utc> {
            self.occurred_at
        }
        fn aggregate_id(&self) -> &str {
            "ping-1"
        }
    }

    fn ping() -> Ping {
        Ping {
            occurred_at: Utc::now(),
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("boom")]
    struct Boom;

    /// Fails the first `failures` attempts, then succeeds.
    struct Flaky {
        failures: u32,
        attempts: Arc<AtomicU32>,
    }

    impl EventHandler<Ping> for Flaky {
        type Error = Boom;
        async fn handle(&self, _event: &Ping) -> Result<(), Self::Error> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= self.failures {
                return Err(Boom);
            }
            Ok(())
        }
    }

    #[derive(Default, Clone)]
    struct MemoryDlq {
        parked: Arc<Mutex<Vec<DeadLetterMessage>>>,
    }

    impl DeadLetterQueue for MemoryDlq {
        async fn dead_letter(&self, message: DeadLetterMessage) -> Result<(), DeadLetterError> {
            self.parked.lock().await.push(message);
            Ok(())
        }
        async fn list(&self, limit: usize) -> Result<Vec<DeadLetterMessage>, DeadLetterError> {
            let mut all = self.parked.lock().await.clone();
            all.truncate(limit);
            Ok(all)
        }
    }

    fn map_ping(event: &Ping) -> Message {
        Message::new(
            "pings",
            event.aggregate_id().as_bytes().to_vec(),
            "text/plain",
        )
    }

    #[cfg(feature = "retry")]
    #[tokio::test]
    async fn retrying_recovers_a_transient_failure() {
        use std::time::Duration;

        let attempts = Arc::new(AtomicU32::new(0));
        let handler = Retrying::new(
            Flaky {
                failures: 2,
                attempts: Arc::clone(&attempts),
            },
            RetryPolicy::new(3, Duration::from_millis(1)),
        );

        assert!(handler.handle(&ping()).await.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[cfg(feature = "retry")]
    #[tokio::test]
    async fn retrying_gives_up_after_the_budget() {
        use std::time::Duration;

        let attempts = Arc::new(AtomicU32::new(0));
        let handler = Retrying::new(
            Flaky {
                failures: 10,
                attempts: Arc::clone(&attempts),
            },
            RetryPolicy::new(2, Duration::from_millis(1)),
        );

        assert!(handler.handle(&ping()).await.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn dead_lettering_parks_the_event_and_reports_success()
    -> Result<(), Box<dyn std::error::Error>> {
        let dlq = MemoryDlq::default();
        let handler = DeadLettering::new(
            Flaky {
                failures: u32::MAX,
                attempts: Arc::new(AtomicU32::new(0)),
            },
            dlq.clone(),
            "order-events.dlq",
            map_ping,
        );

        // The failure is absorbed: the event is parked, publishing continues.
        handler.handle(&ping()).await?;

        let parked = dlq.list(10).await?;
        assert_eq!(parked.len(), 1);
        assert_eq!(parked[0].message.topic, "order-events.dlq");
        assert_eq!(parked[0].reason, "boom");
        Ok(())
    }
}
