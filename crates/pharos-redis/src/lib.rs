//! Redis messaging adapter for Pharos RS.
//!
//! [`RedisMessageBroker`] implements the `pharos-app` messaging traits
//! ([`MessagePublisher`], [`MessageConsumer`], [`MessageAcknowledger`]) on top
//! of Redis lists. Topics map directly to Redis list keys: publishing appends
//! with `RPUSH`, consuming reads with `LPOP`, and `nack(..., true)` requeues a
//! redelivery with an incremented attempt counter.

use std::collections::BTreeMap;

use pharos_app::{
    Delivery, Message, MessageAcknowledger, MessageConsumer, MessagePublisher, MessagingError,
};
use serde::{Deserialize, Serialize};
use tracing::{Instrument, info_span};
use uuid::Uuid;

/// Default Redis set used to record acknowledged message ids.
pub const DEFAULT_REDIS_ACK_SET: &str = "pharos:acked";
/// Default Redis set used to record negatively acknowledged message ids.
pub const DEFAULT_REDIS_NACK_SET: &str = "pharos:nacked";

/// Redis list-backed implementation of the messaging traits.
///
/// Topics are mapped directly to Redis list keys. Publishing appends to the
/// topic list with `RPUSH`; consuming reads with `LPOP`; `nack(..., true)`
/// requeues a redelivery with an incremented attempt counter.
#[derive(Debug, Clone)]
pub struct RedisMessageBroker {
    client: redis::Client,
    ack_set: String,
    nack_set: String,
}

impl RedisMessageBroker {
    /// Creates a Redis broker from an existing Redis client.
    pub fn new(client: redis::Client) -> Self {
        Self::with_tracking_sets(client, DEFAULT_REDIS_ACK_SET, DEFAULT_REDIS_NACK_SET)
    }

    /// Creates a Redis broker from a Redis URL.
    pub fn from_url(url: &str) -> Result<Self, redis::RedisError> {
        Ok(Self::new(redis::Client::open(url)?))
    }

    /// Creates a Redis broker with custom ack/nack tracking set names.
    pub fn with_tracking_sets(
        client: redis::Client,
        ack_set: impl Into<String>,
        nack_set: impl Into<String>,
    ) -> Self {
        Self {
            client,
            ack_set: ack_set.into(),
            nack_set: nack_set.into(),
        }
    }

    /// Returns the underlying Redis client.
    pub fn client(&self) -> &redis::Client {
        &self.client
    }

    /// Returns the Redis set used to track acknowledged message ids.
    pub fn ack_set(&self) -> &str {
        &self.ack_set
    }

    /// Returns the Redis set used to track negatively acknowledged message ids.
    pub fn nack_set(&self) -> &str {
        &self.nack_set
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, redis::RedisError> {
        self.client.get_multiplexed_async_connection().await
    }
}

impl MessagePublisher for RedisMessageBroker {
    async fn publish(&self, message: Message) -> Result<(), MessagingError> {
        async move {
            let mut connection = self
                .connection()
                .await
                .map_err(|error| MessagingError::Publish(error.to_string()))?;
            let topic = message.topic.clone();
            let delivery = WireDelivery::from(Delivery::new(message));
            let encoded = serde_json::to_string(&delivery)
                .map_err(|error| MessagingError::Publish(error.to_string()))?;

            redis::cmd("RPUSH")
                .arg(topic)
                .arg(encoded)
                .query_async::<()>(&mut connection)
                .await
                .map_err(|error| MessagingError::Publish(error.to_string()))?;

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
            let mut connection = self
                .connection()
                .await
                .map_err(|error| MessagingError::Consume(error.to_string()))?;
            let encoded: Option<String> = redis::cmd("LPOP")
                .arg(topic)
                .query_async(&mut connection)
                .await
                .map_err(|error| MessagingError::Consume(error.to_string()))?;

            let delivery = encoded
                .map(|value| {
                    serde_json::from_str::<WireDelivery>(&value)
                        .map(Delivery::from)
                        .map_err(|error| MessagingError::Consume(error.to_string()))
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
            let mut connection = self
                .connection()
                .await
                .map_err(|error| MessagingError::Ack(error.to_string()))?;
            redis::cmd("SADD")
                .arg(&self.ack_set)
                .arg(delivery.message.message_id.to_string())
                .query_async::<()>(&mut connection)
                .await
                .map_err(|error| MessagingError::Ack(error.to_string()))?;

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
            let mut connection = self
                .connection()
                .await
                .map_err(|error| MessagingError::Nack(error.to_string()))?;
            redis::cmd("SADD")
                .arg(&self.nack_set)
                .arg(delivery.message.message_id.to_string())
                .query_async::<()>(&mut connection)
                .await
                .map_err(|error| MessagingError::Nack(error.to_string()))?;

            if requeue {
                let mut redelivery = delivery.clone();
                redelivery.attempt += 1;
                let topic = redelivery.message.topic.clone();
                let encoded = serde_json::to_string(&WireDelivery::from(redelivery))
                    .map_err(|error| MessagingError::Nack(error.to_string()))?;

                redis::cmd("RPUSH")
                    .arg(topic)
                    .arg(encoded)
                    .query_async::<()>(&mut connection)
                    .await
                    .map_err(|error| MessagingError::Nack(error.to_string()))?;
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
