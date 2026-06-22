#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pharos_app::{
        DispatchConfig, Message, MessagingError, OutboxDispatcher, OutboxMessage, OutboxRepository,
        OutboxStatus, RetryPolicy,
    };

    use crate::{InMemoryMessageBroker, InMemoryOutboxRepository};

    /// Publisher that always fails, to exercise the retry/dead-letter path.
    struct AlwaysFailingPublisher;

    impl pharos_app::MessagePublisher for AlwaysFailingPublisher {
        async fn publish(&self, _message: Message) -> Result<(), MessagingError> {
            Err(MessagingError::Publish("broker down".to_string()))
        }
    }

    #[tokio::test]
    async fn dispatcher_keeps_message_pending_until_retries_exhausted() {
        let outbox_repo = InMemoryOutboxRepository::new();
        let outbox_message =
            OutboxMessage::new(Message::new("orders", b"{}".to_vec(), "application/json"));
        let outbox_id = outbox_message.id;
        outbox_repo.insert(outbox_message).await.unwrap();

        // Two attempts allowed: first failure stays pending, second dead-letters.
        let config = DispatchConfig::new(10, RetryPolicy::new(2, Duration::from_millis(1)));
        let dispatcher = OutboxDispatcher::with_config(outbox_repo, AlwaysFailingPublisher, config);

        let first = dispatcher.dispatch_batch().await;
        assert_eq!(first.published, 0);
        assert_eq!(first.failure_count(), 1);
        let stored = dispatcher.repo().get(outbox_id).unwrap();
        assert_eq!(
            stored.status,
            OutboxStatus::Pending,
            "first failure with retry budget left must stay pending"
        );
        assert_eq!(stored.attempts, 1);

        let second = dispatcher.dispatch_batch().await;
        assert_eq!(second.failure_count(), 1);
        let stored = dispatcher.repo().get(outbox_id).unwrap();
        assert_eq!(
            stored.status,
            OutboxStatus::Failed,
            "exhausting attempts must mark the message failed"
        );
        assert_eq!(stored.attempts, 2);
    }

    #[tokio::test]
    async fn dispatcher_publishes_pending_outbox_messages() {
        let outbox_repo = InMemoryOutboxRepository::new();
        let broker = InMemoryMessageBroker::new();
        let outbox_message =
            OutboxMessage::new(Message::new("orders", b"{}".to_vec(), "application/json"));
        let outbox_id = outbox_message.id;

        outbox_repo.insert(outbox_message).await.unwrap();

        let dispatcher = OutboxDispatcher::new(outbox_repo, broker.clone());
        let result = dispatcher.dispatch_pending(10).await;

        assert!(result.is_ok());
        assert_eq!(result.published, 1);
        assert_eq!(broker.queued_len("orders").await, 1);

        let stored = dispatcher_repo_state(&dispatcher, outbox_id).await;
        assert_eq!(stored.status, OutboxStatus::Published);
        assert_eq!(stored.attempts, 1);
    }

    async fn dispatcher_repo_state(
        dispatcher: &OutboxDispatcher<InMemoryOutboxRepository, InMemoryMessageBroker>,
        id: uuid::Uuid,
    ) -> OutboxMessage {
        // Test helper relies on the repository getter exposed by the in-memory adapter.
        dispatcher.repo().get(id).unwrap()
    }
}
