//! NATS adapters for Pharos.
//!
//! This crate targets core NATS subjects. Because core NATS does not have a
//! server-side offset/ack protocol like Kafka, `ack` is a no-op and
//! `nack(..., true)` re-publishes the message to the same subject.

use std::collections::BTreeMap;
use std::str::FromStr;

use async_nats::{HeaderMap, HeaderValue, Subscriber};
use futures::StreamExt;
use pharos_app::{
    Delivery, Message, MessageAcknowledger, MessageConsumer, MessagePublisher, MessagingError,
};
use tracing::{Instrument, info_span};

/// NATS publisher over an existing client.
#[derive(Debug, Clone)]
pub struct NatsPublisher {
    client: async_nats::Client,
}

impl NatsPublisher {
    /// Creates a publisher wrapper.
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }
}

impl MessagePublisher for NatsPublisher {
    async fn publish(&self, message: Message) -> Result<(), MessagingError> {
        let span_topic = message.topic.clone();
        async move {
            let topic = message.topic.clone();
            let payload = message.payload.clone();
            if message.headers.is_empty() {
                self.client
                    .publish(topic.clone(), payload.into())
                    .await
                    .map_err(MessagingError::publish)?;
            } else {
                self.client
                    .publish_with_headers(
                        topic.clone(),
                        header_map_from_message(&message),
                        payload.into(),
                    )
                    .await
                    .map_err(MessagingError::publish)?;
            }

            metrics::counter!("pharos.nats.messages.published", "topic" => topic).increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "nats.message.publish",
            topic = span_topic.as_str()
        ))
        .await
    }
}

/// Payload size ceiling applied when no explicit limit is configured.
///
/// Nothing in this crate sets a max-payload option on the `async_nats`
/// client — that client is built and configured by the caller. This is a
/// second, independent ceiling enforced in-process: without it, `message
/// .payload.to_vec()` copies whatever core NATS delivered with no cap at all,
/// and this crate has no way to tell that a producer sent something huge.
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// NATS consumer bound to a concrete subject subscription.
pub struct NatsConsumer {
    subscriber: tokio::sync::Mutex<Subscriber>,
    max_payload_bytes: usize,
}

impl NatsConsumer {
    /// Creates a consumer from an existing subscription, with the default
    /// payload size ceiling ([`DEFAULT_MAX_PAYLOAD_BYTES`]).
    pub fn new(subscriber: Subscriber) -> Self {
        Self {
            subscriber: tokio::sync::Mutex::new(subscriber),
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        }
    }

    /// Overrides the payload size ceiling.
    ///
    /// A message over this size is never handed to the caller: `next()`
    /// drops it and keeps pulling from the subscription. Core NATS has no
    /// offset to skip past — unlike Kafka there is nothing to commit — so
    /// dropping is simply not yielding the message; no acknowledgment is
    /// needed either way (see the module doc).
    pub fn with_max_payload_size(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes;
        self
    }
}

impl MessageConsumer for NatsConsumer {
    /// `topic` is unused: a [`NatsConsumer`] already wraps one live
    /// [`Subscriber`], whose subject (or wildcard pattern) was fixed when the
    /// subscription was created — there is no second subject to select
    /// between here. The parameter exists only to satisfy
    /// [`MessageConsumer`]'s shared signature.
    async fn next(&self, _topic: &str) -> Result<Option<Delivery>, MessagingError> {
        async move {
            let mut subscriber = self.subscriber.lock().await;

            loop {
                let Some(message) = subscriber.next().await else {
                    return Ok(None);
                };

                if message.payload.len() > self.max_payload_bytes {
                    tracing::warn!(
                        subject = message.subject.as_str(),
                        payload_len = message.payload.len(),
                        max = self.max_payload_bytes,
                        "dropping oversized NATS message without delivering it"
                    );
                    metrics::counter!(
                        "pharos.nats.messages.oversized",
                        "topic" => message.subject.to_string()
                    )
                    .increment(1);
                    continue;
                }

                let headers = header_map_to_btree(message.headers.as_ref());
                let attempt = extract_retry_attempt(&headers);
                let inner = Message {
                    message_id: extract_message_id(message.headers.as_ref())
                        .unwrap_or_else(uuid::Uuid::now_v7),
                    // The *delivery* subject, not the subscription pattern —
                    // for a wildcard subscription (`orders.*`) these differ,
                    // and the delivery subject is the one downstream code
                    // needs (e.g. to route by exact topic).
                    topic: message.subject.to_string(),
                    key: None,
                    headers,
                    payload: message.payload.to_vec(),
                    content_type: message
                        .headers
                        .as_ref()
                        .and_then(|headers| headers.get("content-type"))
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_else(|| "application/octet-stream".to_string()),
                };
                metrics::counter!(
                    "pharos.nats.messages.consumed",
                    "topic" => inner.topic.clone()
                )
                .increment(1);
                // `attempt` reflects this consumer's own prior
                // `nack(_, true)` (via the `pharos.retry.attempt` header it
                // wrote), not a fresh `Delivery::new`'s hardcoded `1`.
                return Ok(Some(Delivery {
                    message: inner,
                    attempt,
                }));
            }
        }
        .instrument(info_span!("nats.message.next"))
        .await
    }
}

/// NATS acknowledger. Core NATS has no durable offset ack, so `ack` is a no-op.
#[derive(Debug, Clone)]
pub struct NatsAcknowledger {
    client: async_nats::Client,
}

impl NatsAcknowledger {
    /// Creates an acknowledger wrapper.
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }
}

impl MessageAcknowledger for NatsAcknowledger {
    async fn ack(&self, delivery: &Delivery) -> Result<(), MessagingError> {
        metrics::counter!("pharos.nats.messages.acked", "topic" => delivery.message.topic.clone())
            .increment(1);
        Ok(())
    }

    async fn nack(&self, delivery: &Delivery, requeue: bool) -> Result<(), MessagingError> {
        async move {
            if requeue {
                // Without this, `Delivery::attempt` would stay pinned at `1`
                // forever on every subsequent delivery — `NatsConsumer::next`
                // reads this same header back — and a `RetryPolicy` consulted
                // against it would never see anything but a first attempt,
                // never dead-lettering a message whose handler can never
                // succeed. See `pharos_app::process_idempotent_with_retry`.
                let redelivery = delivery
                    .message
                    .clone()
                    .with_header("pharos.retry.attempt", (delivery.attempt + 1).to_string());

                let publisher = NatsPublisher::new(self.client.clone());
                publisher
                    .publish(redelivery)
                    .await
                    .map_err(MessagingError::nack)?;
            }

            metrics::counter!("pharos.nats.messages.nacked", "requeue" => requeue.to_string())
                .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "nats.message.nack",
            topic = delivery.message.topic.as_str(),
            requeue
        ))
        .await
    }
}

fn header_map_from_message(message: &Message) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (key, value) in &message.headers {
        if let Ok(header_value) = HeaderValue::from_str(value) {
            headers.insert(key.as_str(), header_value);
        }
    }
    if !message.headers.contains_key("message_id")
        && let Ok(header_value) = HeaderValue::from_str(&message.message_id.to_string())
    {
        headers.insert("message_id", header_value);
    }
    headers
}

fn header_map_to_btree(headers: Option<&HeaderMap>) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    let Some(headers) = headers else {
        return result;
    };

    for (key, values) in headers.iter() {
        if let Some(value) = values.first() {
            result.insert(key.to_string(), value.as_str().to_string());
        }
    }
    result
}

fn extract_message_id(headers: Option<&HeaderMap>) -> Option<uuid::Uuid> {
    headers
        .and_then(|headers| headers.get("message_id"))
        .and_then(|value| uuid::Uuid::parse_str(value.as_str()).ok())
}

/// Reads back the retry attempt this consumer's own `nack(_, true)` wrote,
/// defaulting to `1` (a first delivery) when absent.
fn extract_retry_attempt(headers: &BTreeMap<String, String>) -> u32 {
    headers
        .get("pharos.retry.attempt")
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip_preserves_message_id() {
        let message = Message::new("orders", br#"{}"#.to_vec(), "application/json")
            .with_header("x-correlation-id", "corr-1");

        let headers = header_map_from_message(&message);
        let mapped = header_map_to_btree(Some(&headers));

        assert_eq!(
            mapped.get("x-correlation-id").map(String::as_str),
            Some("corr-1")
        );
        assert_eq!(extract_message_id(Some(&headers)), Some(message.message_id));
    }

    #[test]
    fn retry_attempt_defaults_to_one_when_the_header_is_absent() {
        assert_eq!(extract_retry_attempt(&BTreeMap::new()), 1);
    }

    #[test]
    fn retry_attempt_reads_back_the_header_this_crate_writes_on_nack() {
        let mut headers = BTreeMap::new();
        headers.insert("pharos.retry.attempt".to_string(), "4".to_string());
        assert_eq!(extract_retry_attempt(&headers), 4);
    }

    #[test]
    fn retry_attempt_falls_back_to_one_on_a_garbled_header() {
        let mut headers = BTreeMap::new();
        headers.insert(
            "pharos.retry.attempt".to_string(),
            "not-a-number".to_string(),
        );
        assert_eq!(extract_retry_attempt(&headers), 1);
    }
}
