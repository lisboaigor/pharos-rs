use std::time::Duration;

use tracing::{Instrument, info_span};

use crate::messaging::{
    BackoffStrategy, MessagePublisher, MessagingError, RetryDecision, RetryPolicy,
};
use crate::outbox::{OutboxError, OutboxRepository};

/// Tuning for an [`OutboxDispatcher`] run.
///
/// `batch_size` bounds how many pending messages a single [`OutboxDispatcher::dispatch_batch`]
/// run claims. `retry` decides what happens to a message that fails to publish:
/// while the message still has attempts left it is kept `pending` so a later run
/// retries it; once the attempts are exhausted it is marked `failed` (a
/// terminal state suitable for a dead-letter sweep).
#[derive(Debug, Clone, Copy)]
pub struct DispatchConfig {
    /// Maximum number of pending messages claimed per [`OutboxDispatcher::dispatch_batch`] run.
    pub batch_size: usize,
    /// Retry policy applied to publish failures.
    pub retry: RetryPolicy,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            retry: RetryPolicy {
                max_attempts: 5,
                backoff: BackoffStrategy::Exponential {
                    base: Duration::from_millis(200),
                    multiplier: 2.0,
                    max: Duration::from_secs(30),
                    jitter: true,
                },
            },
        }
    }
}

impl DispatchConfig {
    /// Creates a config with an explicit batch size and retry policy.
    pub fn new(batch_size: usize, retry: RetryPolicy) -> Self {
        Self { batch_size, retry }
    }

    /// Sets the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets the retry policy.
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }
}

/// Errors returned while dispatching an individual outbox message.
#[derive(Debug, thiserror::Error)]
pub enum OutboxDispatchError {
    /// Outbox storage failed.
    #[error(transparent)]
    Outbox(#[from] OutboxError),
    /// Message publishing failed.
    #[error(transparent)]
    Messaging(#[from] MessagingError),
}

/// Outcome of a [`OutboxDispatcher::dispatch_pending`] run.
///
/// A dispatch run never aborts on the first failure: a single poisoned message
/// must not block delivery of the rest of the batch. The run processes every
/// fetched message, then reports how many were published and which ones failed.
#[derive(Debug, Default)]
pub struct DispatchResult {
    /// Number of messages published successfully in this run.
    pub published: usize,
    /// Errors collected for messages that could not be published or marked.
    pub errors: Vec<OutboxDispatchError>,
}

impl DispatchResult {
    /// Returns `true` when the whole batch was dispatched without errors.
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns the number of failures collected during the run.
    pub fn failure_count(&self) -> usize {
        self.errors.len()
    }
}

/// Publishes pending outbox messages through a [`MessagePublisher`].
///
/// This is intentionally small and adapter-agnostic. Production systems can run
/// it from a background worker, scheduler, or transactional polling loop, and
/// can run several dispatchers concurrently when the outbox repository claims
/// rows with `FOR UPDATE SKIP LOCKED`.
pub struct OutboxDispatcher<R, P> {
    repo: R,
    publisher: P,
    config: DispatchConfig,
}

impl<R, P> OutboxDispatcher<R, P> {
    /// Creates a dispatcher with the default [`DispatchConfig`].
    pub fn new(repo: R, publisher: P) -> Self {
        Self::with_config(repo, publisher, DispatchConfig::default())
    }

    /// Creates a dispatcher with an explicit [`DispatchConfig`].
    pub fn with_config(repo: R, publisher: P, config: DispatchConfig) -> Self {
        Self {
            repo,
            publisher,
            config,
        }
    }

    /// Returns the configured outbox repository.
    pub fn repo(&self) -> &R {
        &self.repo
    }

    /// Returns the configured publisher.
    pub fn publisher(&self) -> &P {
        &self.publisher
    }

    /// Returns the dispatch configuration.
    pub fn config(&self) -> &DispatchConfig {
        &self.config
    }
}

impl<R, P> OutboxDispatcher<R, P>
where
    R: OutboxRepository,
    P: MessagePublisher,
{
    /// Dispatches a batch sized by the configured [`DispatchConfig::batch_size`].
    ///
    /// Run this from a background loop; the loop's interval is your polling
    /// cadence. Messages that fail but still have retry budget are left
    /// `pending` for the next run, so no external scheduler is required.
    pub async fn dispatch_batch(&self) -> DispatchResult {
        self.dispatch_pending(self.config.batch_size).await
    }

    /// Dispatches up to `limit` pending messages.
    ///
    /// Every fetched message is attempted. A failure is recorded and dispatching
    /// continues with the next message, so one undeliverable message cannot stall
    /// the outbox. On a publish failure the configured [`RetryPolicy`] decides
    /// whether the message keeps its `pending` status for a later retry or is
    /// marked `failed` once its attempts are exhausted. The returned
    /// [`DispatchResult`] reports successes and the collected per-message errors.
    pub async fn dispatch_pending(&self, limit: usize) -> DispatchResult {
        async move {
            let pending = match self.repo.pending(limit).await {
                Ok(pending) => pending,
                Err(error) => {
                    return DispatchResult {
                        published: 0,
                        errors: vec![error.into()],
                    };
                }
            };

            let mut result = DispatchResult::default();

            for outbox_message in pending {
                // `attempts` reflects prior tries; the attempt we are about to
                // make is `attempts + 1`. Recording it is best-effort and must
                // not prevent the publish below.
                let attempt = outbox_message.attempts + 1;
                let _ = self.repo.record_attempt(outbox_message.id).await;

                match self.publisher.publish(outbox_message.message).await {
                    Ok(()) => match self.repo.mark_published(outbox_message.id).await {
                        Ok(()) => {
                            metrics::counter!("pharos.outbox.published").increment(1);
                            result.published += 1;
                        }
                        // The message was published; failing to mark it only
                        // risks a future duplicate, which consumers dedupe.
                        Err(error) => result.errors.push(error.into()),
                    },
                    Err(error) => {
                        match self.config.retry.decide(attempt) {
                            // Out of retry budget: move to the terminal `failed`
                            // state so a dead-letter sweep can pick it up.
                            RetryDecision::DeadLetter => {
                                let _ = self
                                    .repo
                                    .mark_failed(outbox_message.id, error.to_string())
                                    .await;
                                metrics::counter!("pharos.outbox.dead_lettered").increment(1);
                            }
                            // Still retriable: leave the row `pending` so the
                            // next run retries it after the caller's interval.
                            RetryDecision::RetryAfter(_) => {
                                metrics::counter!("pharos.outbox.retry_scheduled").increment(1);
                            }
                        }
                        metrics::counter!("pharos.outbox.failed").increment(1);
                        result.errors.push(OutboxDispatchError::Messaging(error));
                    }
                }
            }

            result
        }
        .instrument(info_span!("outbox.dispatch_pending", limit))
        .await
    }
}
