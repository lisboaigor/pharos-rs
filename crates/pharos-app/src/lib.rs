//! Application-layer contracts and orchestration helpers for Pharos RS.
//!
//! `pharos-app` connects domain models from `pharos-core` to infrastructure
//! adapters without choosing a database, broker, or runtime architecture. It is
//! the framework's boundary for use cases, event dispatching, integration event
//! contracts, outbox/inbox patterns, retry policies, and serialization.
//!
//! # Main areas
//!
//! - Commands and queries: [`CommandHandler`] and [`QueryHandler`], driven
//!   through [`dispatch`] and [`query_dispatch`] — the instrumentation seam
//!   that wraps each call in the command's/query's tracing span, so handlers
//!   stay pure business logic.
//! - In-process domain events: [`EventBus`], [`EventHandler`], and
//!   [`save_and_publish`]. The bus is itself the instrumentation seam for event
//!   handlers, wrapping each in an `event_handler` span.
//! - Distributed event-driven seam: [`save_and_enqueue`], [`OutboxRepository`],
//!   and [`OutboxDispatcher`].
//! - Consumers and idempotency: [`InboxStore`].
//! - Broker-facing contracts: [`MessagePublisher`], [`MessageConsumer`], and
//!   [`MessageAcknowledger`].
//! - External event contracts: [`IntegrationEvent`] and [`EventSerializer`].
//! - Production seams: [`SchemaRegistry`], [`DeadLetterQueue`], and
//!   [`ConsumerGroupCoordinator`].
//!
//! # Event-driven flow
//!
//! ```mermaid
//! flowchart TD
//!     Command[Command Handler]
//!     Aggregate[Aggregate Root]
//!     Repo[Repository]
//!     Events[Domain Events]
//!     Bus[EventBus]
//!     Handler[EventHandler]
//!     Mapper[Map to Message]
//!     Outbox[OutboxRepository]
//!     Dispatcher[OutboxDispatcher]
//!     Publisher[MessagePublisher]
//!
//!     Command --> Aggregate
//!     Aggregate --> Events
//!     Command --> Repo
//!     Events --> Bus
//!     Bus --> Handler
//!     Events --> Mapper
//!     Mapper --> Outbox
//!     Outbox --> Dispatcher
//!     Dispatcher --> Publisher
//! ```
//!
//! # Choosing the right seam
//!
//! Use [`save_and_publish`] when all event handlers execute in the same process.
//! Use [`save_and_enqueue`] when events must be persisted to an outbox before an
//! external worker publishes them to a broker.
//!
//! # Idempotent consumer flow
//!
//! ```mermaid
//! flowchart TD
//!     Delivery[Message Delivery]
//!     Begin[InboxStore.begin_processing]
//!     Decision{IdempotencyDecision}
//!     Process[Process message]
//!     Completed[mark_completed]
//!     Failed[mark_failed]
//!     Skip[Skip duplicate]
//!
//!     Delivery --> Begin
//!     Begin --> Decision
//!     Decision -->|StartProcessing| Process
//!     Decision -->|RetryPreviousFailure| Process
//!     Decision -->|AlreadyProcessing| Skip
//!     Decision -->|AlreadyCompleted| Skip
//!     Process -->|ok| Completed
//!     Process -->|error| Failed
//! ```

pub mod command;
pub mod error;
pub mod event_bus;
pub mod event_handler;
pub mod integration_event;
pub mod query;
pub mod resilience;
pub mod serialization;
pub mod service;
pub mod tenant;
#[cfg(feature = "tenant-task-local")]
pub mod tenant_local;
#[cfg(feature = "tower")]
pub mod tower_service;
pub mod unit_of_work;
pub mod upcast;

/// Re-exports used by the code generated in `pharos-macros`, so deriving
/// crates don't need these as direct dependencies. Not public API.
#[doc(hidden)]
pub mod __private {
    pub use tracing;
}

// The broker-facing messaging contracts live in `pharos-messaging` so they can
// evolve independently of the CQRS surface; everything is re-exported here (as
// both modules and names) so downstream `pharos_app::...` paths keep working.
pub use pharos_messaging::{
    consume, consumer_group, dead_letter, inbox, messaging, outbox, outbox_dispatcher,
    schema_registry,
};

pub use command::{
    Command, CommandHandler, DispatchError, FieldViolation, ValidationError, dispatch,
};
pub use error::ApplicationError;
pub use event_bus::{EventBus, EventBusError, PublishErrorPolicy};
pub use event_handler::EventHandler;
pub use integration_event::{CausationId, CorrelationId, IntegrationEvent};
pub use pharos_messaging::{
    BackoffStrategy, ConsumerGroupCoordinator, ConsumerGroupError, DeadLetterError,
    DeadLetterMessage, DeadLetterQueue, Delivery, DispatchConfig, DispatchResult, EventSchema,
    IdempotencyDecision, InboxError, InboxMessage, InboxStatus, InboxStore, Message,
    MessageAcknowledger, MessageConsumer, MessagePublisher, MessagingError, OutboxDispatchError,
    OutboxDispatcher, OutboxError, OutboxMessage, OutboxRepository, OutboxStatus,
    PartitionAssignment, ProcessError, ProcessOutcome, RetryDecision, RetryPolicy, SchemaRegistry,
    SchemaRegistryError, SweepError, process_idempotent, sweep_failed_to_dead_letter,
};
pub use query::{Query, QueryHandler, dispatch as query_dispatch};
#[cfg(feature = "retry")]
pub use resilience::Retrying;
pub use resilience::{DeadLettering, DeadLetteringError};
pub use serialization::{
    EventSerializationError, EventSerializer, JsonEventSerializer, MessageCodec, SerializedEvent,
};
pub use service::{republish_pending, save_and_enqueue, save_and_publish};
pub use tenant::{InvalidTenantId, TenantContext, TenantId};
#[cfg(feature = "tenant-task-local")]
pub use tenant_local::CURRENT_TENANT;
#[cfg(feature = "tower")]
pub use tower_service::{CommandHandlerService, QueryHandlerService};
pub use unit_of_work::UnitOfWorkError;
pub use upcast::{JsonUpcasterRegistry, UpcastError, VersionedJsonCodec};
