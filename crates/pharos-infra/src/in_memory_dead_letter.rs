use pharos_app::{DeadLetterError, DeadLetterMessage, DeadLetterQueue};
use tokio::sync::Mutex;
use tracing::{Instrument, info_span};

/// In-memory dead-letter queue for tests and local development.
#[derive(Debug, Default)]
pub struct InMemoryDeadLetterQueue {
    messages: Mutex<Vec<DeadLetterMessage>>,
}

impl InMemoryDeadLetterQueue {
    /// Creates an empty dead-letter queue.
    pub fn new() -> Self {
        Self::default()
    }
}

impl DeadLetterQueue for InMemoryDeadLetterQueue {
    async fn dead_letter(&self, message: DeadLetterMessage) -> Result<(), DeadLetterError> {
        async move {
            self.messages.lock().await.push(message);
            metrics::counter!("pharos.dead_letter.in_memory.enqueued").increment(1);
            Ok(())
        }
        .instrument(info_span!("dead_letter.in_memory.enqueue"))
        .await
    }

    async fn list(&self, limit: usize) -> Result<Vec<DeadLetterMessage>, DeadLetterError> {
        async move {
            let messages = self.messages.lock().await;
            Ok(messages.iter().take(limit).cloned().collect())
        }
        .instrument(info_span!("dead_letter.in_memory.list", limit))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pharos_app::Message;

    #[tokio::test]
    async fn stores_dead_letter_messages() {
        let queue = InMemoryDeadLetterQueue::new();
        let message = DeadLetterMessage::new(
            Message::new("orders", b"payload".to_vec(), "text/plain"),
            "failed",
            3,
        );

        queue.dead_letter(message).await.unwrap();
        let messages = queue.list(10).await.unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].reason, "failed");
    }
}
