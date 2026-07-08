use std::time::Duration;

use pharos_app::{DeadLetterMessage, DeadLetterQueue, Message, RetryDecision, RetryPolicy};
use pharos_memory::InMemoryDeadLetterQueue;

#[tokio::test]
async fn retry_policy_dead_letters_order_message_after_max_attempts()
-> Result<(), Box<dyn std::error::Error>> {
    let policy = RetryPolicy::new(3, Duration::from_millis(50));
    let dlq = InMemoryDeadLetterQueue::new();
    let message = Message::new("order-events", b"OrderConfirmed".to_vec(), "text/plain")
        .with_key("order-123")
        .with_header("event_type", "OrderConfirmed");

    assert_eq!(
        policy.decide(1),
        RetryDecision::RetryAfter(Duration::from_millis(50))
    );
    assert_eq!(
        policy.decide(2),
        RetryDecision::RetryAfter(Duration::from_millis(50))
    );

    if policy.decide(3) == RetryDecision::DeadLetter {
        dlq.dead_letter(DeadLetterMessage::new(
            message,
            "projection handler failed after retries",
            3,
        ))
        .await?;
    }

    let messages = dlq.list(10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id.get_version_num(), 7);
    assert_eq!(messages[0].attempts, 3);
    assert_eq!(messages[0].message.key.as_deref(), Some("order-123"));
    assert_eq!(
        messages[0]
            .message
            .headers
            .get("event_type")
            .map(String::as_str),
        Some("OrderConfirmed")
    );

    Ok(())
}
