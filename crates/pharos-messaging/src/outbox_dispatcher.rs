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
    /// Maximum number of publish lanes running concurrently within one run.
    ///
    /// The default (`1`) publishes sequentially, preserving the claim order.
    /// With higher values the batch is partitioned by [`Message::key`]:
    /// messages sharing a key stay in one lane and publish **in claim order**,
    /// while different keys (and key-less messages) publish in parallel — the
    /// same per-key ordering contract brokers like Kafka give consumers.
    ///
    /// [`Message::key`]: crate::messaging::Message::key
    pub concurrency: usize,
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
            concurrency: 1,
        }
    }
}

impl DispatchConfig {
    /// Creates a config with an explicit batch size and retry policy.
    pub fn new(batch_size: usize, retry: RetryPolicy) -> Self {
        Self {
            batch_size,
            retry,
            ..Self::default()
        }
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

    /// Sets the intra-batch publish concurrency (see
    /// [`concurrency`](Self::concurrency) for the ordering trade-off).
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
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
    /// marked `failed` once its attempts are exhausted. Up to
    /// [`DispatchConfig::concurrency`] messages are published concurrently.
    /// The returned [`DispatchResult`] reports successes and the collected
    /// per-message errors.
    pub async fn dispatch_pending(&self, limit: usize) -> DispatchResult {
        use futures::StreamExt;

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

            let concurrency = self.config.concurrency.max(1);

            // Partition the batch into lanes: one lane per routing key (claim
            // order preserved inside it), one lane per key-less message. Lanes
            // run concurrently; messages within a lane run sequentially, so
            // per-key ordering survives any concurrency level.
            let mut lanes: Vec<Vec<crate::outbox::OutboxMessage>> = Vec::new();
            let mut lane_by_key: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for message in pending {
                match message.message.key.clone() {
                    Some(key) => match lane_by_key.get(&key) {
                        Some(&index) => lanes[index].push(message),
                        None => {
                            lane_by_key.insert(key, lanes.len());
                            lanes.push(vec![message]);
                        }
                    },
                    None => lanes.push(vec![message]),
                }
            }

            let outcomes: Vec<(usize, Vec<OutboxDispatchError>)> =
                futures::stream::iter(lanes.into_iter().map(|lane| async move {
                    let mut published = 0;
                    let mut errors = Vec::new();
                    for message in lane {
                        let (p, e) = self.dispatch_one(message).await;
                        published += p;
                        errors.extend(e);
                    }
                    (published, errors)
                }))
                .buffer_unordered(concurrency)
                .collect()
                .await;

            let mut result = DispatchResult::default();
            for (published, errors) in outcomes {
                result.published += published;
                result.errors.extend(errors);
            }
            result
        }
        .instrument(info_span!("outbox.dispatch_pending", limit))
        .await
    }

    /// Attempts one claimed message end-to-end and reports `(published, errors)`.
    async fn dispatch_one(
        &self,
        outbox_message: crate::outbox::OutboxMessage,
    ) -> (usize, Vec<OutboxDispatchError>) {
        let mut errors = Vec::new();

        // `attempts` reflects prior tries; the attempt we are about to make is
        // `attempts + 1`. If it cannot be recorded, skip the publish:
        // publishing without counting the attempt would let a poisoned message
        // retry forever and never reach the dead-letter path.
        let attempt = outbox_message.attempts + 1;
        if let Err(error) = self.repo.record_attempt(outbox_message.id).await {
            errors.push(error.into());
            return (0, errors);
        }

        match self.publisher.publish(outbox_message.message).await {
            Ok(()) => match self.repo.mark_published(outbox_message.id).await {
                Ok(()) => {
                    metrics::counter!("pharos.outbox.published").increment(1);
                    (1, errors)
                }
                // The message was published; failing to mark it only risks a
                // future duplicate, which consumers dedupe.
                Err(error) => {
                    errors.push(error.into());
                    (0, errors)
                }
            },
            Err(error) => {
                match self.config.retry.decide(attempt) {
                    // Out of retry budget: move to the terminal `failed` state
                    // so a dead-letter sweep can pick it up.
                    RetryDecision::DeadLetter => {
                        if let Err(mark_error) = self
                            .repo
                            .mark_failed(outbox_message.id, error.to_string())
                            .await
                        {
                            errors.push(mark_error.into());
                        }
                        metrics::counter!("pharos.outbox.dead_lettered").increment(1);
                    }
                    // Still retriable: leave the row `pending` and push its
                    // next attempt out by the computed backoff, so the policy's
                    // delay is honored across polls. A scheduling failure is
                    // recorded but non-fatal: the message is retried on a later
                    // poll regardless.
                    RetryDecision::RetryAfter(delay) => {
                        if let Err(error) = self.repo.schedule_retry(outbox_message.id, delay).await
                        {
                            errors.push(error.into());
                        }
                        metrics::counter!("pharos.outbox.retry_scheduled").increment(1);
                    }
                }
                metrics::counter!("pharos.outbox.failed").increment(1);
                errors.push(OutboxDispatchError::Messaging(error));
                (0, errors)
            }
        }
    }
}
