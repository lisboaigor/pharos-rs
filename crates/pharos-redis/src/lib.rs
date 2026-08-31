//! Redis messaging adapter for Pharos RS.
//!
//! [`RedisMessageBroker`] implements the `pharos-app` messaging traits
//! ([`MessagePublisher`], [`MessageConsumer`], [`MessageAcknowledger`]) on top
//! of Redis lists using the reliable-queue pattern:
//!
//! - publishing appends to the topic list with `RPUSH`;
//! - consuming moves the head entry to a per-topic processing list with
//!   `LMOVE` — the message is never removed before it is handled, so a
//!   consumer crash leaves it parked in `{topic}:processing` instead of
//!   losing it;
//! - `ack` removes the entry from the processing list;
//! - `nack(..., true)` removes it from the processing list and requeues a
//!   redelivery with an incremented attempt counter;
//! - [`RedisMessageBroker::recover_processing`] moves parked entries back to
//!   the topic list, e.g. on worker startup after a crash.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use pharos_messaging::{
    Delivery, Message, MessageAcknowledger, MessageConsumer, MessagePublisher, MessagingError,
};
use serde::{Deserialize, Serialize};
use tracing::{Instrument, info_span};
use uuid::Uuid;

/// Suffix appended to a topic name to form its processing list key.
pub const PROCESSING_SUFFIX: &str = ":processing";

/// Default ceiling on [`RedisMessageBroker`]'s in-process `in_flight` cache.
/// See [`RedisMessageBroker::with_max_in_flight`].
const DEFAULT_MAX_IN_FLIGHT: usize = 100_000;

/// Default age past which an `in_flight` entry is treated as stale.
/// See [`RedisMessageBroker::with_in_flight_ttl`].
const DEFAULT_IN_FLIGHT_TTL: Duration = Duration::from_secs(3600);

/// Redis list-backed implementation of the messaging traits.
///
/// In-flight deliveries are tracked in process memory (message id → raw list
/// entry) so `ack`/`nack` can remove the exact entry from the processing
/// list. Acknowledge deliveries from the same broker instance that consumed
/// them; entries orphaned by a crash are reclaimed with
/// [`recover_processing`](Self::recover_processing).
///
/// The cache is bounded ([`DEFAULT_MAX_IN_FLIGHT`] entries,
/// [`DEFAULT_IN_FLIGHT_TTL`] age — override with
/// [`with_max_in_flight`](Self::with_max_in_flight) /
/// [`with_in_flight_ttl`](Self::with_in_flight_ttl)): a delivery whose
/// handler never calls `ack`/`nack` (a bug, a panic, a message a consumer
/// silently drops) would otherwise sit in this map for the life of the
/// process, growing it without bound. An entry that ages out or is evicted
/// to stay under the cap is not lost — `remove_in_flight` already falls back
/// to re-encoding the delivery when its cache entry is missing — so bounding
/// this cache only ever costs a re-encode on a rare, already-abnormal path,
/// never correctness.
#[derive(Debug, Clone)]
pub struct RedisMessageBroker {
    client: redis::Client,
    in_flight: Arc<DashMap<Uuid, (String, Instant)>>,
    max_in_flight: usize,
    in_flight_ttl: Duration,
}

impl RedisMessageBroker {
    /// Creates a Redis broker from an existing Redis client.
    pub fn new(client: redis::Client) -> Self {
        Self {
            client,
            in_flight: Arc::new(DashMap::new()),
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            in_flight_ttl: DEFAULT_IN_FLIGHT_TTL,
        }
    }

    /// Creates a Redis broker from a Redis URL.
    pub fn from_url(url: &str) -> Result<Self, redis::RedisError> {
        Ok(Self::new(redis::Client::open(url)?))
    }

    /// Overrides the entry ceiling on the `in_flight` cache
    /// ([`DEFAULT_MAX_IN_FLIGHT`] by default).
    pub fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = max_in_flight;
        self
    }

    /// Overrides the age past which an `in_flight` entry is swept as stale
    /// ([`DEFAULT_IN_FLIGHT_TTL`] by default).
    pub fn with_in_flight_ttl(mut self, in_flight_ttl: Duration) -> Self {
        self.in_flight_ttl = in_flight_ttl;
        self
    }

    /// Number of entries currently cached in `in_flight`. Exposed for tests
    /// and diagnostics.
    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    /// Returns the underlying Redis client.
    pub fn client(&self) -> &redis::Client {
        &self.client
    }

    /// Returns the processing list key for a topic.
    pub fn processing_list(topic: &str) -> String {
        format!("{topic}{PROCESSING_SUFFIX}")
    }

    /// Moves every entry parked in the topic's processing list back to the
    /// topic list, making it consumable again.
    ///
    /// Call this on worker startup: entries only stay in the processing list
    /// when a previous consumer crashed between `next` and `ack`/`nack`.
    /// Returns the number of recovered entries.
    pub async fn recover_processing(&self, topic: &str) -> Result<u64, MessagingError> {
        async move {
            let mut connection = self.connection().await.map_err(MessagingError::consume)?;
            let processing = Self::processing_list(topic);
            let backlog: u64 = redis::cmd("LLEN")
                .arg(&processing)
                .query_async(&mut connection)
                .await
                .map_err(MessagingError::consume)?;

            // Bounded by the observed backlog so entries parked by consumers
            // that are still alive and concurrently processing do not make
            // this loop spin forever.
            let mut recovered = 0;
            for _ in 0..backlog {
                let moved: Option<String> = redis::cmd("LMOVE")
                    .arg(&processing)
                    .arg(topic)
                    .arg("LEFT")
                    .arg("RIGHT")
                    .query_async(&mut connection)
                    .await
                    .map_err(MessagingError::consume)?;
                if moved.is_none() {
                    break;
                }
                recovered += 1;
            }
            if recovered > 0 {
                metrics::counter!("pharos.redis.messages.recovered", "topic" => topic.to_string())
                    .increment(recovered);
            }
            Ok(recovered)
        }
        .instrument(info_span!("redis.message.recover_processing", topic))
        .await
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, redis::RedisError> {
        self.client.get_multiplexed_async_connection().await
    }

    /// Caches a delivery's raw entry, keeping `in_flight` bounded.
    ///
    /// Sweeps entries older than `in_flight_ttl` first; if the cache is
    /// still at `max_in_flight` afterward, the new entry is simply not
    /// cached rather than growing the map further — `remove_in_flight`'s
    /// re-encode fallback covers an uncached entry, so this never loses
    /// correctness, only a small amount of work on an already-rare path.
    fn cache_in_flight(&self, message_id: Uuid, raw: String) {
        if self.in_flight.len() >= self.max_in_flight {
            let now = Instant::now();
            self.in_flight.retain(|_, (_, inserted_at)| {
                now.duration_since(*inserted_at) < self.in_flight_ttl
            });
        }
        if self.in_flight.len() < self.max_in_flight {
            self.in_flight.insert(message_id, (raw, Instant::now()));
        } else {
            tracing::warn!(
                max_in_flight = self.max_in_flight,
                "in_flight cache at capacity even after sweeping stale entries; \
                 not caching this delivery (ack/nack will re-encode it instead)"
            );
        }
    }

    /// Removes a delivery's raw entry from its topic's processing list.
    async fn remove_in_flight(
        &self,
        delivery: &Delivery,
        map_err: impl Fn(Box<dyn std::error::Error + Send + Sync + 'static>) -> MessagingError,
    ) -> Result<(), MessagingError> {
        // Prefer the raw entry captured at consume time; fall back to
        // re-encoding for deliveries consumed by another broker instance
        // (works as long as the entry was written by this adapter, whose
        // encoding is deterministic).
        let raw = match self.in_flight.remove(&delivery.message.message_id) {
            Some((_, (raw, _))) => raw,
            None => serde_json::to_string(&WireDelivery::from(delivery.clone()))
                .map_err(|error| map_err(Box::new(error)))?,
        };

        let mut connection = self
            .connection()
            .await
            .map_err(|error| map_err(Box::new(error)))?;
        redis::cmd("LREM")
            .arg(Self::processing_list(&delivery.message.topic))
            .arg(1)
            .arg(raw)
            .query_async::<()>(&mut connection)
            .await
            .map_err(|error| map_err(Box::new(error)))?;
        Ok(())
    }
}

impl MessagePublisher for RedisMessageBroker {
    async fn publish(&self, message: Message) -> Result<(), MessagingError> {
        async move {
            let mut connection = self.connection().await.map_err(MessagingError::publish)?;
            let topic = message.topic.clone();
            let delivery = WireDelivery::from(Delivery::new(message));
            let encoded = serde_json::to_string(&delivery).map_err(MessagingError::publish)?;

            redis::cmd("RPUSH")
                .arg(topic)
                .arg(encoded)
                .query_async::<()>(&mut connection)
                .await
                .map_err(MessagingError::publish)?;

            metrics::counter!("pharos.redis.messages.published").increment(1);
            Ok(())
        }
        .instrument(info_span!("redis.message.publish"))
        .await
    }
}

impl MessageConsumer for RedisMessageBroker {
    async fn next(&self, topic: &str) -> Result<Option<Delivery>, MessagingError> {
        async move {
            let mut connection = self.connection().await.map_err(MessagingError::consume)?;
            // Reliable-queue pattern: move the entry to the processing list
            // instead of popping it, so a crash between here and ack/nack
            // parks the message instead of dropping it.
            let encoded: Option<String> = redis::cmd("LMOVE")
                .arg(topic)
                .arg(Self::processing_list(topic))
                .arg("LEFT")
                .arg("RIGHT")
                .query_async(&mut connection)
                .await
                .map_err(MessagingError::consume)?;

            let delivery = encoded
                .map(|raw| {
                    let delivery = serde_json::from_str::<WireDelivery>(&raw)
                        .map(Delivery::from)
                        .map_err(MessagingError::consume)?;
                    self.cache_in_flight(delivery.message.message_id, raw);
                    Ok::<_, MessagingError>(delivery)
                })
                .transpose()?;

            if delivery.is_some() {
                metrics::counter!("pharos.redis.messages.consumed").increment(1);
            }

            Ok(delivery)
        }
        .instrument(info_span!("redis.message.next", topic))
        .await
    }
}

impl MessageAcknowledger for RedisMessageBroker {
    async fn ack(&self, delivery: &Delivery) -> Result<(), MessagingError> {
        async move {
            self.remove_in_flight(delivery, MessagingError::Ack).await?;
            metrics::counter!("pharos.redis.messages.acked").increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "redis.message.ack",
            message_id = %delivery.message.message_id,
        ))
        .await
    }

    async fn nack(&self, delivery: &Delivery, requeue: bool) -> Result<(), MessagingError> {
        async move {
            self.remove_in_flight(delivery, MessagingError::Nack)
                .await?;

            if requeue {
                let mut connection = self.connection().await.map_err(MessagingError::nack)?;
                let mut redelivery = delivery.clone();
                redelivery.attempt += 1;
                let topic = redelivery.message.topic.clone();
                let encoded = serde_json::to_string(&WireDelivery::from(redelivery))
                    .map_err(MessagingError::nack)?;

                redis::cmd("RPUSH")
                    .arg(topic)
                    .arg(encoded)
                    .query_async::<()>(&mut connection)
                    .await
                    .map_err(MessagingError::nack)?;
            }

            metrics::counter!("pharos.redis.messages.nacked", "requeue" => requeue.to_string())
                .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "redis.message.nack",
            message_id = %delivery.message.message_id,
            requeue,
        ))
        .await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireDelivery {
    message: WireMessage,
    attempt: u32,
}

impl From<Delivery> for WireDelivery {
    fn from(value: Delivery) -> Self {
        Self {
            message: WireMessage::from(value.message),
            attempt: value.attempt,
        }
    }
}

impl From<WireDelivery> for Delivery {
    fn from(value: WireDelivery) -> Self {
        Self {
            message: Message::from(value.message),
            attempt: value.attempt,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireMessage {
    message_id: Uuid,
    topic: String,
    key: Option<String>,
    headers: BTreeMap<String, String>,
    payload: Vec<u8>,
    content_type: String,
}

impl From<Message> for WireMessage {
    fn from(value: Message) -> Self {
        Self {
            message_id: value.message_id,
            topic: value.topic,
            key: value.key,
            headers: value.headers,
            payload: value.payload,
            content_type: value.content_type,
        }
    }
}

impl From<WireMessage> for Message {
    fn from(value: WireMessage) -> Self {
        Message {
            message_id: value.message_id,
            topic: value.topic,
            key: value.key,
            headers: value.headers,
            payload: value.payload,
            content_type: value.content_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `redis::Client::open` only parses/validates the URL — it does not
    // connect — so these tests exercise the `in_flight` cache's bounding
    // logic without a live Redis server.
    fn broker() -> RedisMessageBroker {
        RedisMessageBroker::new(redis::Client::open("redis://127.0.0.1:0").expect("URL parses"))
    }

    #[test]
    fn caches_entries_up_to_the_configured_cap() {
        let broker = broker().with_max_in_flight(3);
        for _ in 0..3 {
            broker.cache_in_flight(Uuid::now_v7(), "raw".to_string());
        }
        assert_eq!(broker.in_flight_len(), 3);

        // A fourth entry, with no stale entries to sweep, is not cached —
        // the cache stays bounded instead of growing past the cap.
        broker.cache_in_flight(Uuid::now_v7(), "raw".to_string());
        assert_eq!(broker.in_flight_len(), 3);
    }

    #[test]
    fn sweeps_ttl_expired_entries_to_make_room() {
        let broker = broker()
            .with_max_in_flight(2)
            .with_in_flight_ttl(Duration::from_millis(1));
        broker.cache_in_flight(Uuid::now_v7(), "old-1".to_string());
        broker.cache_in_flight(Uuid::now_v7(), "old-2".to_string());
        assert_eq!(broker.in_flight_len(), 2);

        std::thread::sleep(Duration::from_millis(5));

        // At capacity, but both entries are past the 1ms TTL: the sweep
        // clears them, and the new entry is cached.
        broker.cache_in_flight(Uuid::now_v7(), "new".to_string());
        assert_eq!(broker.in_flight_len(), 1);
    }

    #[test]
    fn a_zero_cap_never_caches_anything() {
        // The precondition `remove_in_flight`'s re-encode fallback exists
        // for: with the cap at zero, nothing is ever cached, so every
        // ack/nack must take the fallback path — exercised end-to-end
        // against a live server in `tests/docker_integration.rs`.
        let broker = broker().with_max_in_flight(0);
        broker.cache_in_flight(Uuid::now_v7(), "raw".to_string());
        assert_eq!(broker.in_flight_len(), 0);
    }
}
