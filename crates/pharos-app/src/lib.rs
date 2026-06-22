//! Application-layer contracts and orchestration helpers for Pharos RS.
//!
//! `pharos-app` connects domain models from `pharos-core` to infrastructure
//! adapters without choosing a database, broker, or runtime architecture. It is
//! the framework's boundary for use cases, event dispatching, integration event
//! contracts, outbox/inbox patterns, retry policies, and serialization.
//!
//! # Main areas
//!
//! - Commands and queries: [`CommandHandler`] and [`QueryHandler`].
//! - In-process domain events: [`EventBus`], [`EventHandler`], and
//!   [`save_and_publish`].
//! - Distributed event-driven seam: [`save_and_enqueue`], [`OutboxRepository`],
//!   and [`OutboxDispatcher`].
//! - Consumers and idempotency: [`InboxStore`] and [`IdempotencyStore`].
//! - Broker-facing contracts: [`MessagePublisher`], [`MessageConsumer`], and
//!   [`MessageAcknowledger`].
//! - External event contracts: [`IntegrationEvent`] and [`EventSerializer`].
//! - Production seams: [`UnitOfWork`], [`SchemaRegistry`], [`DeadLetterQueue`],
//!   [`ConsumerGroupCoordinator`], [`TransportAdapter`], and observability
//!   configuration descriptors.
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
pub mod consumer_group;
pub mod dead_letter;
pub mod error;
pub mod event_bus;
pub mod event_handler;
pub mod inbox;
pub mod integration_event;
pub mod messaging;
pub mod observability;
pub mod outbox;
pub mod outbox_dispatcher;
pub mod query;
pub mod schema_registry;
pub mod serialization;
pub mod service;
pub mod tenant;
pub mod tenant_local;
#[cfg(feature = "tower")]
pub mod tower_service;
pub mod transport;
pub mod unit_of_work;

pub use command::{Command, CommandHandler};
pub use consumer_group::{ConsumerGroupCoordinator, ConsumerGroupError, PartitionAssignment};
pub use dead_letter::{DeadLetterError, DeadLetterMessage, DeadLetterQueue};
pub use error::ApplicationError;
pub use event_bus::{EventBus, EventBusError};
pub use event_handler::EventHandler;
pub use inbox::{
    IdempotencyDecision, IdempotencyStore, InboxError, InboxMessage, InboxStatus, InboxStore,
};
pub use integration_event::IntegrationEvent;
pub use messaging::{
    BackoffStrategy, Delivery, Message, MessageAcknowledger, MessageConsumer, MessagePublisher,
    MessagingError, RetryDecision, RetryPolicy,
};
pub use observability::{MetricsBackendConfig, OpenTelemetryConfig};
pub use outbox::{OutboxError, OutboxMessage, OutboxRepository, OutboxStatus};
pub use outbox_dispatcher::{
    DispatchConfig, DispatchResult, OutboxDispatchError, OutboxDispatcher,
};
pub use query::{Query, QueryHandler};
pub use schema_registry::{EventSchema, SchemaRegistry, SchemaRegistryError};
pub use serialization::{
    EventSerializationError, EventSerializer, JsonEventSerializer, SerializedEvent,
};
pub use service::{save_and_enqueue, save_and_publish};
pub use tenant::TenantContext;
pub use tenant_local::CURRENT_TENANT;
#[cfg(feature = "tower")]
pub use tower_service::{CommandHandlerService, QueryHandlerService};
pub use transport::{
    TransportAdapter, TransportEndpoint, TransportError, TransportProtocol, TransportRequest,
    TransportResponse,
};
pub use unit_of_work::{NoopUnitOfWork, UnitOfWork, UnitOfWorkError};
