//! Kafka adapters for Pharos.
//!
//! This crate provides:
//! - `KafkaPublisher`, `KafkaConsumer`, and `KafkaAcknowledger` for the
//!   `pharos-app` messaging traits.
//! - `ConfluentSchemaRegistry` and `ApicurioSchemaRegistry` for remote schema
//!   registration and lookup.

use std::collections::BTreeMap;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;

use pharos_app::{
    Delivery, EventSchema, Message, MessageAcknowledger, MessageConsumer, MessagePublisher,
    MessagingError, SchemaRegistry, SchemaRegistryError,
};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, Message as KafkaMessage, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::{Instrument, info_span};

const HEADER_PARTITION: &str = "pharos.kafka.partition";
const HEADER_OFFSET: &str = "pharos.kafka.offset";

/// Kafka publisher over an existing `FutureProducer`.
#[derive(Clone)]
pub struct KafkaPublisher {
    producer: FutureProducer,
    queue_timeout: Duration,
}

impl KafkaPublisher {
    /// Creates a publisher with a 5-second queue timeout.
    pub fn new(producer: FutureProducer) -> Self {
        Self {
            producer,
            queue_timeout: Duration::from_secs(5),
        }
    }

    /// Uses a custom queue timeout for `FutureProducer::send`.
    pub fn with_queue_timeout(mut self, queue_timeout: Duration) -> Self {
        self.queue_timeout = queue_timeout;
        self
    }
}

impl MessagePublisher for KafkaPublisher {
    async fn publish(&self, message: Message) -> Result<(), MessagingError> {
        let span_topic = message.topic.clone();
        async move {
            let topic = message.topic.clone();
            let payload = message.payload.clone();
            let mut record = FutureRecord::to(&topic).payload(&payload);
            if let Some(key) = &message.key {
                record = record.key(key);
            }
            if !message.headers.is_empty() {
                record = record.headers(kafka_headers_from_map(&message.headers));
            }

            self.producer
                .send(record, self.queue_timeout)
                .await
                .map_err(|(error, _owned_message)| MessagingError::Publish(error.to_string()))?;
            metrics::counter!("pharos.kafka.messages.published", "topic" => topic).increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "kafka.message.publish",
            topic = span_topic.as_str()
        ))
        .await
    }
}

/// Kafka consumer over an existing `StreamConsumer`.
#[derive(Clone)]
pub struct KafkaConsumer {
    consumer: Arc<StreamConsumer>,
}

impl KafkaConsumer {
    /// Creates a consumer wrapper.
    pub fn new(consumer: StreamConsumer) -> Self {
        Self {
            consumer: Arc::new(consumer),
        }
    }
}

impl MessageConsumer for KafkaConsumer {
    async fn next(&self, topic: &str) -> Result<Option<Delivery>, MessagingError> {
        async move {
            self.consumer
                .subscribe(&[topic])
                .map_err(|error| MessagingError::Consume(error.to_string()))?;
            let borrowed = self
                .consumer
                .recv()
                .await
                .map_err(|error| MessagingError::Consume(error.to_string()))?;

            let mut headers = headers_to_map(borrowed.headers());
            headers.insert(
                HEADER_PARTITION.to_string(),
                borrowed.partition().to_string(),
            );
            headers.insert(HEADER_OFFSET.to_string(), borrowed.offset().to_string());

            let message = Message {
                message_id: extract_message_id(&headers).unwrap_or_else(uuid::Uuid::now_v7),
                topic: borrowed.topic().to_string(),
                key: borrowed
                    .key()
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned()),
                headers,
                payload: borrowed.payload().unwrap_or_default().to_vec(),
                content_type: borrowed
                    .headers()
                    .and_then(|headers| header_value(headers, "content-type"))
                    .unwrap_or_else(|| "application/octet-stream".to_string()),
            };
            metrics::counter!("pharos.kafka.messages.consumed", "topic" => topic.to_string())
                .increment(1);
            Ok(Some(Delivery::new(message)))
        }
        .instrument(info_span!("kafka.message.next", topic))
        .await
    }
}

/// Kafka acknowledger that commits consumed offsets and can optionally requeue
/// by republishing the message before the commit.
#[derive(Clone)]
pub struct KafkaAcknowledger {
    consumer: Arc<StreamConsumer>,
    producer: Option<FutureProducer>,
    queue_timeout: Duration,
}

impl KafkaAcknowledger {
    /// Creates an acknowledger that can only commit or drop offsets.
    pub fn new(consumer: StreamConsumer) -> Self {
        Self {
            consumer: Arc::new(consumer),
            producer: None,
            queue_timeout: Duration::from_secs(5),
        }
    }

    /// Creates an acknowledger that can republish on `nack(..., true)`.
    pub fn with_requeue_producer(consumer: StreamConsumer, producer: FutureProducer) -> Self {
        Self {
            consumer: Arc::new(consumer),
            producer: Some(producer),
            queue_timeout: Duration::from_secs(5),
        }
    }
}

impl MessageAcknowledger for KafkaAcknowledger {
    async fn ack(&self, delivery: &Delivery) -> Result<(), MessagingError> {
        commit_delivery(&self.consumer, delivery)
            .instrument(info_span!(
                "kafka.message.ack",
                topic = delivery.message.topic.as_str()
            ))
            .await
    }

    async fn nack(&self, delivery: &Delivery, requeue: bool) -> Result<(), MessagingError> {
        async move {
            if requeue {
                let Some(producer) = &self.producer else {
                    return Err(MessagingError::Nack(
                        "KafkaAcknowledger requires a producer to requeue messages".to_string(),
                    ));
                };

                let mut redelivery = delivery.message.clone();
                redelivery.headers.insert(
                    "pharos.retry.attempt".to_string(),
                    (delivery.attempt + 1).to_string(),
                );

                let publisher =
                    KafkaPublisher::new(producer.clone()).with_queue_timeout(self.queue_timeout);
                publisher.publish(redelivery).await.map_err(|error| {
                    MessagingError::Nack(format!("failed to requeue message in Kafka: {error}"))
                })?;
            }

            commit_delivery(&self.consumer, delivery).await?;
            metrics::counter!("pharos.kafka.messages.nacked", "requeue" => requeue.to_string())
                .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "kafka.message.nack",
            topic = delivery.message.topic.as_str(),
            requeue
        ))
        .await
    }
}

async fn commit_delivery(
    consumer: &Arc<StreamConsumer>,
    delivery: &Delivery,
) -> Result<(), MessagingError> {
    let partition = delivery
        .message
        .headers
        .get(HEADER_PARTITION)
        .ok_or_else(|| {
            MessagingError::Ack("Kafka delivery is missing partition metadata".to_string())
        })?
        .parse::<i32>()
        .map_err(|error| MessagingError::Ack(error.to_string()))?;
    let offset = delivery
        .message
        .headers
        .get(HEADER_OFFSET)
        .ok_or_else(|| {
            MessagingError::Ack("Kafka delivery is missing offset metadata".to_string())
        })?
        .parse::<i64>()
        .map_err(|error| MessagingError::Ack(error.to_string()))?;

    let mut topic_partition = rdkafka::TopicPartitionList::new();
    topic_partition
        .add_partition_offset(
            &delivery.message.topic,
            partition,
            rdkafka::Offset::Offset(offset + 1),
        )
        .map_err(|error| MessagingError::Ack(error.to_string()))?;
    consumer
        .commit(&topic_partition, CommitMode::Async)
        .map_err(|error| MessagingError::Ack(error.to_string()))?;
    metrics::counter!("pharos.kafka.messages.acked", "topic" => delivery.message.topic.clone())
        .increment(1);
    Ok(())
}

/// Confluent-compatible schema registry client.
#[derive(Debug, Clone)]
pub struct ConfluentSchemaRegistry {
    client: reqwest::Client,
    base_url: String,
}

/// Apicurio Registry client.
#[derive(Debug, Clone)]
pub struct ApicurioSchemaRegistry {
    client: reqwest::Client,
    base_url: String,
    group: String,
}

impl ApicurioSchemaRegistry {
    /// Creates an Apicurio registry client targeting the `default` group.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_group(base_url, "default")
    }

    /// Creates an Apicurio registry client for a specific group.
    pub fn with_group(base_url: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            group: group.into(),
        }
    }

    fn artifact_url(&self, event_type: &str) -> String {
        format!(
            "{}/apis/registry/v2/groups/{}/artifacts/{}",
            self.base_url, self.group, event_type
        )
    }
}

impl ConfluentSchemaRegistry {
    /// Creates a schema registry client from a base URL.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    fn subject_for(&self, event_type: &str) -> String {
        format!("{event_type}-value")
    }

    fn subject_url(&self, event_type: &str) -> String {
        format!(
            "{}/subjects/{}",
            self.base_url,
            self.subject_for(event_type)
        )
    }
}

impl SchemaRegistry for ConfluentSchemaRegistry {
    async fn register(&self, schema: EventSchema) -> Result<(), SchemaRegistryError> {
        let span_event_type = schema.event_type.clone();
        async move {
            let body = RegisterSchemaRequest {
                schema: schema.schema,
                schema_type: Some(schema.format.to_uppercase()),
            };
            let response = self
                .client
                .post(format!("{}/versions", self.subject_url(&schema.event_type)))
                .json(&body)
                .send()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;
            ensure_success(response).await?;
            Ok(())
        }
        .instrument(info_span!(
            "kafka.schema_registry.register",
            event_type = span_event_type.as_str()
        ))
        .await
    }

    async fn get(
        &self,
        event_type: &str,
        version: u32,
    ) -> Result<Option<EventSchema>, SchemaRegistryError> {
        async move {
            let response = self
                .client
                .get(format!(
                    "{}/versions/{version}",
                    self.subject_url(event_type)
                ))
                .send()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;

            if response.status() == StatusCode::NOT_FOUND {
                return Ok(None);
            }

            let payload = ensure_success(response)
                .await?
                .json::<ConfluentSchemaResponse>()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;
            Ok(Some(EventSchema::new(
                payload.subject.trim_end_matches("-value"),
                payload.version,
                payload
                    .schema_type
                    .unwrap_or_else(|| "AVRO".to_string())
                    .to_lowercase(),
                payload.schema,
            )))
        }
        .instrument(info_span!("kafka.schema_registry.get", event_type, version))
        .await
    }

    async fn latest(&self, event_type: &str) -> Result<Option<EventSchema>, SchemaRegistryError> {
        async move {
            let response = self
                .client
                .get(format!("{}/versions/latest", self.subject_url(event_type)))
                .send()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;

            if response.status() == StatusCode::NOT_FOUND {
                return Ok(None);
            }

            let payload = ensure_success(response)
                .await?
                .json::<ConfluentSchemaResponse>()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;
            Ok(Some(EventSchema::new(
                payload.subject.trim_end_matches("-value"),
                payload.version,
                payload
                    .schema_type
                    .unwrap_or_else(|| "AVRO".to_string())
                    .to_lowercase(),
                payload.schema,
            )))
        }
        .instrument(info_span!("kafka.schema_registry.latest", event_type))
        .await
    }
}

impl SchemaRegistry for ApicurioSchemaRegistry {
    async fn register(&self, schema: EventSchema) -> Result<(), SchemaRegistryError> {
        let span_event_type = schema.event_type.clone();
        async move {
            let response = self
                .client
                .post(format!(
                    "{}/apis/registry/v2/groups/{}/artifacts",
                    self.base_url, self.group
                ))
                .header("X-Registry-ArtifactId", schema.event_type)
                .header("X-Registry-ArtifactType", schema.format.to_uppercase())
                .body(schema.schema)
                .send()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;
            ensure_success(response).await?;
            Ok(())
        }
        .instrument(info_span!(
            "apicurio.schema_registry.register",
            event_type = span_event_type.as_str()
        ))
        .await
    }

    async fn get(
        &self,
        event_type: &str,
        version: u32,
    ) -> Result<Option<EventSchema>, SchemaRegistryError> {
        async move {
            let response = self
                .client
                .get(format!(
                    "{}/versions/{version}",
                    self.artifact_url(event_type)
                ))
                .send()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;
            if response.status() == StatusCode::NOT_FOUND {
                return Ok(None);
            }

            let response = ensure_success(response).await?;
            let schema = response
                .text()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;
            Ok(Some(EventSchema::new(
                event_type,
                version,
                "json-schema",
                schema,
            )))
        }
        .instrument(info_span!(
            "apicurio.schema_registry.get",
            event_type,
            version
        ))
        .await
    }

    async fn latest(&self, event_type: &str) -> Result<Option<EventSchema>, SchemaRegistryError> {
        async move {
            let meta_response = self
                .client
                .get(format!("{}/meta", self.artifact_url(event_type)))
                .send()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;
            if meta_response.status() == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let meta = ensure_success(meta_response)
                .await?
                .json::<ApicurioMetaResponse>()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;

            let content_response = self
                .client
                .get(self.artifact_url(event_type))
                .send()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;
            let schema = ensure_success(content_response)
                .await?
                .text()
                .await
                .map_err(|error| SchemaRegistryError::Storage(error.to_string()))?;

            Ok(Some(EventSchema::new(
                event_type,
                meta.version.parse().unwrap_or(1),
                meta.artifact_type.to_lowercase(),
                schema,
            )))
        }
        .instrument(info_span!("apicurio.schema_registry.latest", event_type))
        .await
    }
}

fn kafka_headers_from_map(headers: &BTreeMap<String, String>) -> OwnedHeaders {
    let mut owned = OwnedHeaders::new_with_capacity(headers.len());
    for (key, value) in headers {
        owned = owned.insert(Header {
            key,
            value: Some(value.as_bytes()),
        });
    }
    owned
}

fn headers_to_map<H: Headers>(headers: Option<&H>) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if let Some(headers) = headers {
        for idx in 0..headers.count() {
            if let Some(header) = headers.try_get(idx) {
                let value = header
                    .value
                    .and_then(|bytes| from_utf8(bytes).ok())
                    .unwrap_or_default()
                    .to_string();
                map.insert(header.key.to_string(), value);
            }
        }
    }
    map
}

fn header_value<H: Headers>(headers: &H, key: &str) -> Option<String> {
    for idx in 0..headers.count() {
        let header = headers.try_get(idx)?;
        if header.key == key {
            return header
                .value
                .and_then(|bytes| from_utf8(bytes).ok())
                .map(ToOwned::to_owned);
        }
    }
    None
}

fn extract_message_id(headers: &BTreeMap<String, String>) -> Option<uuid::Uuid> {
    headers
        .get("message_id")
        .or_else(|| headers.get("message-id"))
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
}

async fn ensure_success(
    response: reqwest::Response,
) -> Result<reqwest::Response, SchemaRegistryError> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(SchemaRegistryError::Storage(format!(
        "schema registry request failed with {status}: {body}"
    )))
}

#[derive(Debug, Serialize)]
struct RegisterSchemaRequest {
    schema: String,
    #[serde(rename = "schemaType", skip_serializing_if = "Option::is_none")]
    schema_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConfluentSchemaResponse {
    subject: String,
    version: u32,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApicurioMetaResponse {
    version: String,
    #[serde(rename = "type")]
    artifact_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_header_roundtrip_preserves_values() {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        headers.insert("message_id".to_string(), uuid::Uuid::nil().to_string());

        let owned = kafka_headers_from_map(&headers);
        let roundtrip = headers_to_map(Some(&owned));

        assert_eq!(
            roundtrip.get("content-type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(extract_message_id(&roundtrip), Some(uuid::Uuid::nil()));
    }

    #[test]
    fn confluent_urls_use_subject_per_event_type() {
        let registry = ConfluentSchemaRegistry::new("http://localhost:8081/");

        assert_eq!(registry.subject_for("OrderPlaced"), "OrderPlaced-value");
        assert_eq!(
            registry.subject_url("OrderPlaced"),
            "http://localhost:8081/subjects/OrderPlaced-value"
        );
    }

    #[test]
    fn apicurio_urls_use_group_and_artifact() {
        let registry = ApicurioSchemaRegistry::with_group("http://localhost:8080/", "billing");

        assert_eq!(
            registry.artifact_url("OrderPlaced"),
            "http://localhost:8080/apis/registry/v2/groups/billing/artifacts/OrderPlaced"
        );
    }
}
