//! Conformance suite for [`MessagePublisher`]/[`MessageConsumer`]/
//! [`MessageAcknowledger`], and for [`ConsumerGroupCoordinator`].
use pharos_app::{
    ConsumerGroupCoordinator, Message, MessageAcknowledger, MessageConsumer, MessagePublisher,
};

/// Runs the publish/consume conformance suite against a `publisher`/
/// `consumer` pair.
///
/// A published message must be consumable exactly once through `next` — a
/// delivery is only handed out again after an explicit `nack(requeue:
/// true)` (see [`acknowledger`]), never on its own.
///
/// ```ignore
/// #[tokio::test]
/// async fn message_publisher_consumer_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::messaging::publish_and_consume(&broker, &broker).await
/// }
/// ```
pub async fn publish_and_consume<P, C>(
    publisher: &P,
    consumer: &C,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: MessagePublisher,
    C: MessageConsumer,
{
    let topic = "pharos-contract-topic";
    assert!(
        consumer.next(topic).await?.is_none(),
        "a topic with nothing published must yield no delivery"
    );

    let message = Message::new(topic, b"payload".to_vec(), "text/plain");
    let id = message.message_id;
    publisher.publish(message).await?;

    let delivery = consumer
        .next(topic)
        .await?
        .ok_or("a published message must be consumable")?;
    assert_eq!(delivery.message.message_id, id);
    assert_eq!(
        delivery.attempt, 1,
        "a first delivery must report attempt 1"
    );

    assert!(
        consumer.next(topic).await?.is_none(),
        "a message must not be delivered twice without an explicit requeue"
    );

    Ok(())
}

/// Runs the ack/nack conformance suite against a `publisher`/`consumer`/
/// `ack` triple (usually the same value implementing all three traits).
///
/// `nack(requeue: false)` drops the message for good; `nack(requeue: true)`
/// redelivers it with an incremented `attempt` count.
///
/// ```ignore
/// #[tokio::test]
/// async fn message_acknowledger_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::messaging::acknowledger(&broker, &broker, &broker).await
/// }
/// ```
pub async fn acknowledger<P, C, A>(
    publisher: &P,
    consumer: &C,
    ack: &A,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: MessagePublisher,
    C: MessageConsumer,
    A: MessageAcknowledger,
{
    let topic = "pharos-contract-ack-topic";

    publisher
        .publish(Message::new(topic, b"dropped".to_vec(), "text/plain"))
        .await?;
    let delivery = consumer
        .next(topic)
        .await?
        .ok_or("delivery expected for the dropped message")?;
    ack.nack(&delivery, false).await?;
    assert!(
        consumer.next(topic).await?.is_none(),
        "nack without requeue must not redeliver the message"
    );

    let requeued = Message::new(topic, b"requeued".to_vec(), "text/plain");
    let requeued_id = requeued.message_id;
    publisher.publish(requeued).await?;
    let first_delivery = consumer
        .next(topic)
        .await?
        .ok_or("delivery expected for the requeued message")?;
    ack.nack(&first_delivery, true).await?;

    let redelivered = consumer
        .next(topic)
        .await?
        .ok_or("nack(requeue: true) must redeliver the message")?;
    assert_eq!(redelivered.message.message_id, requeued_id);
    assert_eq!(
        redelivered.attempt,
        first_delivery.attempt + 1,
        "a redelivery must report a higher attempt count than the one before it"
    );

    ack.ack(&redelivered).await?;

    Ok(())
}

/// Runs the [`ConsumerGroupCoordinator`] conformance suite against
/// `coordinator`.
///
/// Only checks what every coordinator must guarantee regardless of its
/// rebalancing strategy: `join` assigns at least one partition per
/// requested topic to the joining consumer, `assignments` reflects what
/// `join` returned, and `leave` removes that consumer's assignments. Exact
/// partition numbers are backend-specific and deliberately not asserted.
///
/// ```ignore
/// #[tokio::test]
/// async fn consumer_group_coordinator_contract() -> Result<(), Box<dyn std::error::Error>> {
///     pharos_testing::contract::messaging::consumer_group(&coordinator).await
/// }
/// ```
pub async fn consumer_group<G>(coordinator: &G) -> Result<(), Box<dyn std::error::Error>>
where
    G: ConsumerGroupCoordinator,
{
    let group = "pharos-contract-group";
    let topics = vec!["orders".to_string(), "payments".to_string()];

    let assigned = coordinator.join(group, "consumer-1", &topics).await?;
    assert!(
        !assigned.is_empty(),
        "join must return at least one partition assignment"
    );
    for topic in &topics {
        assert!(
            assigned.iter().any(|a| &a.topic == topic),
            "every requested topic must get at least one assignment"
        );
    }
    for assignment in &assigned {
        assert_eq!(assignment.group, group);
        assert_eq!(assignment.consumer_id, "consumer-1");
    }

    let current = coordinator.assignments(group).await?;
    assert!(!current.is_empty());

    coordinator.leave(group, "consumer-1").await?;
    let after_leave = coordinator.assignments(group).await?;
    assert!(
        !after_leave.iter().any(|a| a.consumer_id == "consumer-1"),
        "leave must remove that consumer's assignments"
    );

    Ok(())
}
