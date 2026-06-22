use std::future::Future;

use thiserror::Error;

/// Assignment of a topic partition to a consumer inside a consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignment {
    /// Consumer group name.
    pub group: String,
    /// Consumer instance id.
    pub consumer_id: String,
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
}

impl PartitionAssignment {
    /// Creates a partition assignment.
    pub fn new(
        group: impl Into<String>,
        consumer_id: impl Into<String>,
        topic: impl Into<String>,
        partition: i32,
    ) -> Self {
        Self {
            group: group.into(),
            consumer_id: consumer_id.into(),
            topic: topic.into(),
            partition,
        }
    }
}

/// Errors produced by consumer group coordinators.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConsumerGroupError {
    /// Coordination backend failed.
    #[error("consumer group coordination failed: {0}")]
    Coordination(String),
}

/// Coordinates consumer group membership and partition assignments.
pub trait ConsumerGroupCoordinator: Send + Sync + 'static {
    /// Joins a consumer group and returns the assigned partitions.
    fn join(
        &self,
        group: &str,
        consumer_id: &str,
        topics: &[String],
    ) -> impl Future<Output = Result<Vec<PartitionAssignment>, ConsumerGroupError>> + Send;

    /// Leaves a consumer group.
    fn leave(
        &self,
        group: &str,
        consumer_id: &str,
    ) -> impl Future<Output = Result<(), ConsumerGroupError>> + Send;

    /// Returns current assignments for a consumer group.
    fn assignments(
        &self,
        group: &str,
    ) -> impl Future<Output = Result<Vec<PartitionAssignment>, ConsumerGroupError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_partition_assignment() {
        let assignment = PartitionAssignment::new("orders", "consumer-1", "order-events", 0);

        assert_eq!(assignment.group, "orders");
        assert_eq!(assignment.consumer_id, "consumer-1");
        assert_eq!(assignment.topic, "order-events");
        assert_eq!(assignment.partition, 0);
    }
}
