use pharos_app::{Message, MessageAcknowledger, MessageConsumer, MessagePublisher};
use pharos_redis::RedisMessageBroker;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, runners::AsyncRunner};

const REDIS_IMAGE: &str = "redis";
const REDIS_TAG: &str = "7-alpine";

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn redis_broker_publishes_consumes_requeues_and_acks_against_real_container() -> TestResult {
    let (_container, broker) = start_redis().await?;
    let message = Message::new("orders", b"order-1".to_vec(), "text/plain")
        .with_key("order-1")
        .with_header("correlation_id", "corr-1");
    let message_id = message.message_id;

    broker.publish(message).await?;

    let delivery = broker
        .next("orders")
        .await?
        .ok_or("expected a Redis delivery")?;
    assert_eq!(delivery.attempt, 1);
    assert_eq!(delivery.message.message_id, message_id);
    assert_eq!(delivery.message.key.as_deref(), Some("order-1"));

    broker.nack(&delivery, true).await?;

    let redelivery = broker
        .next("orders")
        .await?
        .ok_or("expected a Redis redelivery")?;
    assert_eq!(redelivery.attempt, 2);
    assert_eq!(redelivery.message.message_id, message_id);

    broker.ack(&redelivery).await?;
    assert!(broker.next("orders").await?.is_none());

    Ok(())
}

#[tokio::test]
async fn redis_broker_parks_unacked_messages_and_recovers_them() -> TestResult {
    let (_container, broker) = start_redis().await?;
    let message = Message::new("payments", b"payment-1".to_vec(), "text/plain");
    let message_id = message.message_id;

    broker.publish(message).await?;

    // Consume but never ack — as if the worker crashed mid-processing.
    let delivery = broker
        .next("payments")
        .await?
        .ok_or("expected a Redis delivery")?;
    assert_eq!(delivery.message.message_id, message_id);

    // The message is parked in the processing list, not lost and not
    // consumable again until it is recovered.
    assert!(broker.next("payments").await?.is_none());

    // A fresh broker instance (new process) reclaims the parked entry.
    let restarted = RedisMessageBroker::new(broker.client().clone());
    assert_eq!(restarted.recover_processing("payments").await?, 1);

    let redelivery = restarted
        .next("payments")
        .await?
        .ok_or("expected a recovered Redis delivery")?;
    assert_eq!(redelivery.message.message_id, message_id);

    restarted.ack(&redelivery).await?;
    assert!(restarted.next("payments").await?.is_none());
    assert_eq!(restarted.recover_processing("payments").await?, 0);

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
