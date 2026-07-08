#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pharos_app::{
        DispatchConfig, Message, MessagePublisher, MessagingError, OutboxDispatcher, OutboxMessage,
        OutboxRepository, OutboxStatus, RetryPolicy,
    };

    use crate::{InMemoryMessageBroker, InMemoryOutboxRepository};

    /// Publisher that always fails, to exercise the retry/dead-letter path.
    struct AlwaysFailingPublisher;

    impl pharos_app::MessagePublisher for AlwaysFailingPublisher {
        async fn publish(&self, _message: Message) -> Result<(), MessagingError> {
            Err(MessagingError::publish(std::io::Error::other(
                "broker down",
            )))
        }
    }

    #[tokio::test]
    async fn dispatcher_keeps_message_pending_until_retries_exhausted()
    -> Result<(), Box<dyn std::error::Error>> {
        let outbox_repo = InMemoryOutboxRepository::new();
        let outbox_message =
            OutboxMessage::new(Message::new("orders", b"{}".to_vec(), "application/json"));
        let outbox_id = outbox_message.id;
        outbox_repo.insert(outbox_message).await?;

        // Two attempts allowed: first failure stays pending, second dead-letters.
        let config = DispatchConfig::new(10, RetryPolicy::new(2, Duration::from_millis(1)));
        let dispatcher = OutboxDispatcher::with_config(outbox_repo, AlwaysFailingPublisher, config);

        let first = dispatcher.dispatch_batch().await;
        assert_eq!(first.published, 0);
        assert_eq!(first.failure_count(), 1);
        let stored = dispatcher
            .repo()
            .get(outbox_id)
            .ok_or("outbox message not found")?;
        assert_eq!(
            stored.status,
            OutboxStatus::Pending,
            "first failure with retry budget left must stay pending"
        );
        assert_eq!(stored.attempts, 1);

        // The failure scheduled the retry 1ms into the future; the message is
        // invisible to `pending` until that backoff elapses.
        tokio::time::sleep(Duration::from_millis(10)).await;

        let second = dispatcher.dispatch_batch().await;
        assert_eq!(second.failure_count(), 1);
        let stored = dispatcher
            .repo()
            .get(outbox_id)
            .ok_or("outbox message not found")?;
        assert_eq!(
            stored.status,
            OutboxStatus::Failed,
            "exhausting attempts must mark the message failed"
        );
        assert_eq!(stored.attempts, 2);
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_publishes_pending_outbox_messages() -> Result<(), Box<dyn std::error::Error>>
    {
        let outbox_repo = InMemoryOutboxRepository::new();
        let broker = InMemoryMessageBroker::new();
        let outbox_message =
            OutboxMessage::new(Message::new("orders", b"{}".to_vec(), "application/json"));
        let outbox_id = outbox_message.id;

        outbox_repo.insert(outbox_message).await?;

        let dispatcher = OutboxDispatcher::new(outbox_repo, broker.clone());
        let result = dispatcher.dispatch_pending(10).await;

        assert!(result.is_ok());
        assert_eq!(result.published, 1);
        assert_eq!(broker.queued_len("orders").await, 1);

        let stored = dispatcher_repo_state(&dispatcher, outbox_id)?;
        assert_eq!(stored.status, OutboxStatus::Published);
        assert_eq!(stored.attempts, 1);
        Ok(())
    }

    fn dispatcher_repo_state(
        dispatcher: &OutboxDispatcher<InMemoryOutboxRepository, InMemoryMessageBroker>,
        id: uuid::Uuid,
    ) -> Result<OutboxMessage, Box<dyn std::error::Error>> {
        dispatcher
            .repo()
            .get(id)
            .ok_or_else(|| "outbox message not found".into())
    }

    /// A recording publisher that notes the order in which payloads arrive.
    struct RecordingPublisher {
        seen: std::sync::Mutex<Vec<(Option<String>, Vec<u8>)>>,
    }

    impl MessagePublisher for RecordingPublisher {
        async fn publish(&self, message: Message) -> Result<(), MessagingError> {
            // Yield so concurrent lanes interleave if ordering were broken.
            tokio::task::yield_now().await;
            let Ok(mut seen) = self.seen.lock() else {
                panic!("recording publisher mutex poisoned");
            };
            seen.push((message.key.clone(), message.payload.clone()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn concurrent_dispatch_preserves_per_key_ordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let repo = InMemoryOutboxRepository::new();
        for (key, payload) in [
            ("order-1", b"a".to_vec()),
            ("order-2", b"x".to_vec()),
            ("order-1", b"b".to_vec()),
            ("order-1", b"c".to_vec()),
            ("order-2", b"y".to_vec()),
        ] {
            repo.insert(OutboxMessage::new(
                Message::new("orders", payload, "text/plain").with_key(key),
            ))
            .await?;
        }

        let publisher = std::sync::Arc::new(RecordingPublisher {
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let dispatcher = OutboxDispatcher::with_config(
            repo,
            std::sync::Arc::clone(&publisher),
            DispatchConfig::default().with_concurrency(8),
        );

        let result = dispatcher.dispatch_batch().await;
        assert!(result.is_ok());
        assert_eq!(result.published, 5);

        let Ok(seen) = publisher.seen.lock() else {
            panic!("recording publisher mutex poisoned");
        };
        let per_key = |k: &str| -> Vec<Vec<u8>> {
            seen.iter()
                .filter(|(key, _)| key.as_deref() == Some(k))
                .map(|(_, p)| p.clone())
                .collect()
        };
        // Messages sharing a key arrive in claim (insertion) order.
        assert_eq!(
            per_key("order-1"),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(per_key("order-2"), vec![b"x".to_vec(), b"y".to_vec()]);
        Ok(())
    }
}
