//! Broker-facing messaging contracts for Pharos RS.
//!
//! `pharos-messaging` holds everything a message flows through between the
//! application layer and a concrete broker adapter, with no dependency on the
//! rest of the framework:
//!
//! - [`Message`], [`Delivery`], and the [`MessagePublisher`] /
//!   [`MessageConsumer`] / [`MessageAcknowledger`] traits;
//! - [`RetryPolicy`] and [`BackoffStrategy`];
//! - the outbox seam: [`OutboxMessage`], [`OutboxRepository`], and
//!   [`OutboxDispatcher`];
//! - the inbox/idempotency seam: [`InboxMessage`] and [`InboxStore`];
//! - [`DeadLetterQueue`] and [`ConsumerGroupCoordinator`];
//! - [`SchemaRegistry`].
//!
//! It exists as its own crate so messaging contracts can evolve and be
//! versioned independently of the CQRS surface in `pharos-app` (which
//! re-exports everything here for convenience).

pub mod consume;
pub mod consumer_group;
pub mod dead_letter;
pub mod inbox;
pub mod messaging;
pub mod outbox;
pub mod outbox_dispatcher;
pub mod schema_registry;

pub use consume::{ProcessError, ProcessOutcome, process_idempotent};
pub use consumer_group::{ConsumerGroupCoordinator, ConsumerGroupError, PartitionAssignment};
pub use dead_letter::{DeadLetterError, DeadLetterMessage, DeadLetterQueue};
pub use inbox::{IdempotencyDecision, InboxError, InboxMessage, InboxStatus, InboxStore};
pub use messaging::{
    BackoffStrategy, Delivery, Message, MessageAcknowledger, MessageConsumer, MessagePublisher,
    MessagingError, RetryDecision, RetryPolicy,
};
pub use outbox::{
    OutboxError, OutboxMessage, OutboxRepository, OutboxStatus, SweepError,
    sweep_failed_to_dead_letter,
};
pub use outbox_dispatcher::{
    DispatchConfig, DispatchResult, OutboxDispatchError, OutboxDispatcher,
};
pub use schema_registry::{EventSchema, SchemaRegistry, SchemaRegistryError};
