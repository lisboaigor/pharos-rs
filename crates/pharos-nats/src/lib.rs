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

/// NATS consumer bound to a concrete subject subscription.
pub struct NatsConsumer {
    subscriber: tokio::sync::Mutex<Subscriber>,
}

impl NatsConsumer {
    /// Creates a consumer from an existing subscription.
    pub fn new(subscriber: Subscriber) -> Self {
        Self {
            subscriber: tokio::sync::Mutex::new(subscriber),
        }
    }
}

impl MessageConsumer for NatsConsumer {
    async fn next(&self, _topic: &str) -> Result<Option<Delivery>, MessagingError> {
        async move {
			let mut subscriber = self.subscriber.lock().await;
			let Some(message) = subscriber.next().await else {
				return Ok(None);
			};

			let delivery = Delivery::new(Message {
				message_id: extract_message_id(message.headers.as_ref()).unwrap_or_else(uuid::Uuid::now_v7),
				topic: message.subject.to_string(),
				key: None,
				headers: header_map_to_btree(message.headers.as_ref()),
				payload: message.payload.to_vec(),
				content_type: message
					.headers
					.as_ref()
					.and_then(|headers| headers.get("content-type"))
					.map(|value| value.as_str().to_string())
					.unwrap_or_else(|| "application/octet-stream".to_string()),
			});
			metrics::counter!("pharos.nats.messages.consumed", "topic" => delivery.message.topic.clone())
				.increment(1);
			Ok(Some(delivery))
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
                let publisher = NatsPublisher::new(self.client.clone());
                publisher
                    .publish(delivery.message.clone())
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
}
