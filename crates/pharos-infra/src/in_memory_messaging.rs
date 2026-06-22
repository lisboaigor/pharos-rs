use std::collections::VecDeque;
use std::sync::Arc;

use dashmap::DashMap;
use pharos_app::{
    Delivery, Message, MessageAcknowledger, MessageConsumer, MessagePublisher, MessagingError,
};
use tokio::sync::Mutex;
use tracing::{Instrument, info_span};
use uuid::Uuid;

/// In-memory broker implementing publisher, consumer, ack and nack contracts.
#[derive(Debug, Default, Clone)]
pub struct InMemoryMessageBroker {
    topics: Arc<DashMap<String, Arc<Mutex<VecDeque<Delivery>>>>>,
    acked: Arc<DashMap<Uuid, Delivery>>,
    nacked: Arc<DashMap<Uuid, Delivery>>,
}

impl InMemoryMessageBroker {
    /// Creates an empty in-memory broker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns how many messages are queued for a topic.
    pub async fn queued_len(&self, topic: &str) -> usize {
        match self.topics.get(topic) {
            Some(queue) => queue.lock().await.len(),
            None => 0,
        }
    }

    /// Returns `true` when the message was acked.
    pub fn was_acked(&self, message_id: Uuid) -> bool {
        self.acked.contains_key(&message_id)
    }

    /// Returns `true` when the message was nacked.
    pub fn was_nacked(&self, message_id: Uuid) -> bool {
        self.nacked.contains_key(&message_id)
    }
}

impl MessagePublisher for InMemoryMessageBroker {
    async fn publish(&self, message: Message) -> Result<(), MessagingError> {
        async move {
            let topic = message.topic.clone();
            let queue = self
                .topics
                .entry(topic)
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                .clone();
            queue.lock().await.push_back(Delivery::new(message));
            metrics::counter!("pharos.messages.published").increment(1);
            Ok(())
        }
        .instrument(info_span!("message.publish"))
        .await
    }
}

impl MessageConsumer for InMemoryMessageBroker {
    async fn next(&self, topic: &str) -> Result<Option<Delivery>, MessagingError> {
        async move {
            let Some(queue) = self.topics.get(topic).map(|entry| entry.clone()) else {
                return Ok(None);
            };
            let delivery = queue.lock().await.pop_front();
            if delivery.is_some() {
                metrics::counter!("pharos.messages.consumed").increment(1);
            }
            Ok(delivery)
        }
        .instrument(info_span!("message.next", topic))
        .await
    }
}

impl MessageAcknowledger for InMemoryMessageBroker {
    async fn ack(&self, delivery: &Delivery) -> Result<(), MessagingError> {
        async move {
            self.acked
                .insert(delivery.message.message_id, delivery.clone());
            metrics::counter!("pharos.messages.acked").increment(1);
            Ok(())
        }
        .instrument(info_span!("message.ack", message_id = %delivery.message.message_id))
        .await
    }

    async fn nack(&self, delivery: &Delivery, requeue: bool) -> Result<(), MessagingError> {
        async move {
            self.nacked
                .insert(delivery.message.message_id, delivery.clone());
            if requeue {
                let mut redelivery = delivery.clone();
                redelivery.attempt += 1;
                let queue = self
                    .topics
                    .entry(redelivery.message.topic.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                    .clone();
                queue.lock().await.push_back(redelivery);
            }
            metrics::counter!("pharos.messages.nacked", "requeue" => requeue.to_string())
                .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "message.nack",
            message_id = %delivery.message.message_id,
            requeue
        ))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publishes_consumes_acks_and_requeues_messages() {
        let broker = InMemoryMessageBroker::new();
        let message = Message::new("orders", b"{}".to_vec(), "application/json");
        let message_id = message.message_id;

        broker.publish(message).await.unwrap();
        assert_eq!(broker.queued_len("orders").await, 1);

        let delivery = broker.next("orders").await.unwrap().unwrap();
        assert_eq!(delivery.attempt, 1);
        assert_eq!(delivery.message.message_id, message_id);

        broker.nack(&delivery, true).await.unwrap();
        assert!(broker.was_nacked(message_id));

        let redelivery = broker.next("orders").await.unwrap().unwrap();
        assert_eq!(redelivery.attempt, 2);

        broker.ack(&redelivery).await.unwrap();
        assert!(broker.was_acked(message_id));
    }
}
