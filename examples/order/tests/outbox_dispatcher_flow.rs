use pharos_app::{
    Message, MessageAcknowledger, MessageConsumer, OutboxDispatcher, OutboxMessage,
    OutboxRepository, OutboxStatus,
};
use pharos_infra::{InMemoryMessageBroker, InMemoryOutboxRepository};

#[tokio::test]
async fn outbox_dispatcher_publishes_pending_order_messages()
-> Result<(), Box<dyn std::error::Error>> {
    let outbox = InMemoryOutboxRepository::new();
    let broker = InMemoryMessageBroker::new();

    let outbox_message = OutboxMessage::new(
        Message::new(
            "order-events",
            br#"{"event_type":"OrderConfirmed"}"#.to_vec(),
            "application/json",
        )
        .with_key("order-123")
        .with_header("event_type", "OrderConfirmed"),
    );
    let outbox_id = outbox_message.id;

    outbox.insert(outbox_message).await?;

    let dispatcher = OutboxDispatcher::new(outbox, broker.clone());
    let result = dispatcher.dispatch_pending(10).await;

    assert!(result.is_ok());
    assert_eq!(result.published, 1);

    let stored = dispatcher
        .repo()
        .get(outbox_id)
        .ok_or("expected outbox message to remain stored")?;
    assert_eq!(stored.status, OutboxStatus::Published);
    assert_eq!(stored.attempts, 1);

    let delivery = broker
        .next("order-events")
        .await?
        .ok_or("expected published order message")?;
    assert_eq!(delivery.message.key.as_deref(), Some("order-123"));
    assert_eq!(
        delivery
            .message
            .headers
            .get("event_type")
            .map(String::as_str),
        Some("OrderConfirmed")
    );

    broker.ack(&delivery).await?;
    assert!(broker.was_acked(delivery.message.message_id));

    Ok(())
}
