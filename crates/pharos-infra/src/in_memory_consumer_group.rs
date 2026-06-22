use dashmap::DashMap;
use pharos_app::{ConsumerGroupCoordinator, ConsumerGroupError, PartitionAssignment};
use tracing::{Instrument, info_span};

/// In-memory consumer group coordinator for tests and local development.
#[derive(Debug, Default)]
pub struct InMemoryConsumerGroupCoordinator {
    assignments: DashMap<String, Vec<PartitionAssignment>>,
}

impl InMemoryConsumerGroupCoordinator {
    /// Creates an empty coordinator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ConsumerGroupCoordinator for InMemoryConsumerGroupCoordinator {
    async fn join(
        &self,
        group: &str,
        consumer_id: &str,
        topics: &[String],
    ) -> Result<Vec<PartitionAssignment>, ConsumerGroupError> {
        async move {
            let assignments: Vec<_> = topics
                .iter()
                .enumerate()
                .map(|(partition, topic)| {
                    PartitionAssignment::new(group, consumer_id, topic.clone(), partition as i32)
                })
                .collect();
            self.assignments
                .insert(group.to_string(), assignments.clone());
            Ok(assignments)
        }
        .instrument(info_span!(
            "consumer_group.in_memory.join",
            group,
            consumer_id
        ))
        .await
    }

    async fn leave(&self, group: &str, consumer_id: &str) -> Result<(), ConsumerGroupError> {
        async move {
            if let Some(mut assignments) = self.assignments.get_mut(group) {
                assignments.retain(|assignment| assignment.consumer_id != consumer_id);
            }
            Ok(())
        }
        .instrument(info_span!(
            "consumer_group.in_memory.leave",
            group,
            consumer_id
        ))
        .await
    }

    async fn assignments(
        &self,
        group: &str,
    ) -> Result<Vec<PartitionAssignment>, ConsumerGroupError> {
        async move {
            Ok(self
                .assignments
                .get(group)
                .map(|entry| entry.value().clone())
                .unwrap_or_default())
        }
        .instrument(info_span!("consumer_group.in_memory.assignments", group))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn coordinates_assignments() {
        let coordinator = InMemoryConsumerGroupCoordinator::new();
        let topics = vec!["orders".to_string(), "payments".to_string()];

        let assignments = coordinator
            .join("workers", "consumer-1", &topics)
            .await
            .unwrap();

        assert_eq!(assignments.len(), 2);
        assert_eq!(coordinator.assignments("workers").await.unwrap().len(), 2);

        coordinator.leave("workers", "consumer-1").await.unwrap();
        assert!(coordinator.assignments("workers").await.unwrap().is_empty());
    }
}
