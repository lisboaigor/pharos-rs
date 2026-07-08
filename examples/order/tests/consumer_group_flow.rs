use pharos_app::ConsumerGroupCoordinator;
use pharos_memory::InMemoryConsumerGroupCoordinator;

#[tokio::test]
async fn consumer_group_coordinator_assigns_order_event_partitions()
-> Result<(), Box<dyn std::error::Error>> {
    let coordinator = InMemoryConsumerGroupCoordinator::new();
    let topics = vec!["order-events".to_string(), "payment-events".to_string()];

    let assignments = coordinator
        .join("order-projections", "consumer-1", &topics)
        .await?;

    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].group, "order-projections");
    assert_eq!(assignments[0].consumer_id, "consumer-1");
    assert_eq!(assignments[0].topic, "order-events");
    assert_eq!(assignments[0].partition, 0);
    assert_eq!(assignments[1].topic, "payment-events");
    assert_eq!(assignments[1].partition, 1);

    let current = coordinator.assignments("order-projections").await?;
    assert_eq!(current, assignments);

    coordinator.leave("order-projections", "consumer-1").await?;
    assert!(
        coordinator
            .assignments("order-projections")
            .await?
            .is_empty()
    );

    Ok(())
}
