//! Proves the conformance kit itself is correct by running every suite
//! against `pharos-memory`'s adapters — the same adapters an application
//! reaches for first, and the reference every other adapter (a SeaORM one,
//! a hand-rolled one) is expected to match.

use std::sync::Arc;

use pharos_memory::{
    InMemoryConsumerGroupCoordinator, InMemoryDeadLetterQueue, InMemoryEventStore,
    InMemoryInboxStore, InMemoryMessageBroker, InMemoryOutboxRepository, InMemorySagaStore,
    InMemorySchemaRegistry, InMemorySnapshotStore, InMemoryUnitOfWork,
};
use pharos_testing::contract::{self, ContractAggregate, ContractEvent};
use uuid::Uuid;

#[tokio::test]
async fn repository() -> Result<(), Box<dyn std::error::Error>> {
    let uow =
        InMemoryUnitOfWork::<ContractAggregate>::new(Arc::new(InMemoryOutboxRepository::new()));
    contract::repository::run(&uow).await
}

#[tokio::test]
async fn transactional_repository_and_save_and_enqueue_atomicity()
-> Result<(), Box<dyn std::error::Error>> {
    let uow =
        InMemoryUnitOfWork::<ContractAggregate>::new(Arc::new(InMemoryOutboxRepository::new()));
    contract::transactional::transactional_repository(&uow, &uow).await?;

    // A fresh unit of work per suite: the atomicity suite's assertions are
    // about what's (not) visible immediately after each call, which a
    // shared store from the suite above would muddy.
    let uow =
        InMemoryUnitOfWork::<ContractAggregate>::new(Arc::new(InMemoryOutboxRepository::new()));
    contract::transactional::save_and_enqueue_atomicity(&uow, &uow, uow.outbox().as_ref()).await
}

#[tokio::test]
async fn outbox_repository() -> Result<(), Box<dyn std::error::Error>> {
    contract::outbox::run(&InMemoryOutboxRepository::new()).await
}

#[tokio::test]
async fn outbox_repository_no_double_claim() -> Result<(), Box<dyn std::error::Error>> {
    contract::outbox::no_double_claim(&InMemoryOutboxRepository::new()).await
}

#[tokio::test]
async fn inbox_store() -> Result<(), Box<dyn std::error::Error>> {
    contract::inbox::run(&InMemoryInboxStore::new()).await
}

#[tokio::test]
async fn dead_letter_queue() -> Result<(), Box<dyn std::error::Error>> {
    contract::dead_letter::run(&InMemoryDeadLetterQueue::new()).await
}

#[tokio::test]
async fn saga_store_and_timeout() -> Result<(), Box<dyn std::error::Error>> {
    let store = InMemorySagaStore::<String, u32>::new();
    contract::saga::run(&store).await?;

    let store = InMemorySagaStore::<String, u32>::new();
    contract::saga::timeout(&store).await
}

#[tokio::test]
async fn event_store_and_snapshot_store() -> Result<(), Box<dyn std::error::Error>> {
    contract::event_store::run(&InMemoryEventStore::<Uuid, ContractEvent>::new()).await?;
    contract::event_store::snapshot_store(&InMemorySnapshotStore::<Uuid, u64>::new()).await
}

#[tokio::test]
async fn message_publisher_consumer_and_acknowledger() -> Result<(), Box<dyn std::error::Error>> {
    let broker = InMemoryMessageBroker::new();
    contract::messaging::publish_and_consume(&broker, &broker).await?;

    let broker = InMemoryMessageBroker::new();
    contract::messaging::acknowledger(&broker, &broker, &broker).await
}

#[tokio::test]
async fn consumer_group_coordinator() -> Result<(), Box<dyn std::error::Error>> {
    contract::messaging::consumer_group(&InMemoryConsumerGroupCoordinator::new()).await
}

#[tokio::test]
async fn schema_registry() -> Result<(), Box<dyn std::error::Error>> {
    contract::schema_registry::run(&InMemorySchemaRegistry::new()).await
}
