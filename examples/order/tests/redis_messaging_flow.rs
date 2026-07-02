use pharos_app::{Message, MessageAcknowledger, MessageConsumer, MessagePublisher};
use pharos_redis::RedisMessageBroker;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, runners::AsyncRunner};

const REDIS_IMAGE: &str = "redis";
const REDIS_TAG: &str = "7-alpine";

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn redis_broker_handles_order_event_delivery_against_real_container() -> TestResult {
    let (_container, broker) = start_redis().await?;
    let message = Message::new(
        "order-events",
        br#"{"event_type":"OrderConfirmed","order_id":"order-123"}"#.to_vec(),
        "application/json",
    )
    .with_key("order-123")
    .with_header("event_type", "OrderConfirmed")
    .with_header("correlation_id", "order-123");
    let message_id = message.message_id;

    broker.publish(message).await?;

    let delivery = broker
        .next("order-events")
        .await?
        .ok_or("expected order event delivery from Redis")?;
    assert_eq!(delivery.attempt, 1);
    assert_eq!(delivery.message.message_id, message_id);
    assert_eq!(delivery.message.key.as_deref(), Some("order-123"));

    broker.nack(&delivery, true).await?;

    let redelivery = broker
        .next("order-events")
        .await?
        .ok_or("expected order event redelivery from Redis")?;
    assert_eq!(redelivery.attempt, 2);
    assert_eq!(redelivery.message.message_id, message_id);

    broker.ack(&redelivery).await?;
    assert!(broker.next("order-events").await?.is_none());

    Ok(())
}

async fn start_redis() -> Result<
    (ContainerAsync<GenericImage>, RedisMessageBroker),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let container = GenericImage::new(REDIS_IMAGE, REDIS_TAG)
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await?;

    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    let broker = RedisMessageBroker::from_url(&format!("redis://{host}:{port}/"))?;

    Ok((container, broker))
}
