//! Guards the promise that the facade alone is enough to use the derives:
//! the macros detect (via the calling crate's `Cargo.toml`) that only the
//! `pharos` facade is available and route the generated code through its
//! `core`/`app` re-exports — no direct dependency on
//! `pharos-core`/`pharos-app` and no per-type attribute required.
//!
//! Note: this crate *does* have those crates as (transitive) dependencies, but
//! nothing here names `pharos_core`/`pharos_app` paths — exactly what a
//! facade-only user would write.

use chrono::{DateTime, Utc};
use pharos::prelude::*;

#[derive(Debug, Clone, DomainEvent)]
enum CustomerEvent {
    Registered {
        #[aggregate_id]
        customer_id: String,
        #[occurred_at]
        occurred_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Entity, AggregateRoot)]
struct Customer {
    #[id]
    id: u64,
    #[version]
    version: u64,
    #[events]
    events: AggregateEvents<CustomerEvent>,
}

#[derive(Command)]
struct RegisterCustomer {
    id: u64,
}

#[derive(Query)]
#[query(result = Option<u64>)]
struct GetCustomer {
    #[trace]
    id: u64,
}

struct RegisterHandler;
impl CommandHandler<RegisterCustomer> for RegisterHandler {
    type Output = u64;
    type Error = std::convert::Infallible;

    async fn handle(&self, cmd: RegisterCustomer) -> Result<u64, Self::Error> {
        Ok(cmd.id)
    }
}

struct GetHandler;
impl QueryHandler<GetCustomer> for GetHandler {
    type Error = std::convert::Infallible;

    async fn handle(&self, q: GetCustomer) -> Result<Option<u64>, Self::Error> {
        Ok(Some(q.id))
    }
}

#[tokio::test]
async fn facade_only_derives_compile_and_dispatch() -> Result<(), Box<dyn std::error::Error>> {
    let mut customer = Customer {
        id: 7,
        version: 0,
        events: AggregateEvents::default(),
    };
    customer.events.raise(CustomerEvent::Registered {
        customer_id: "7".to_string(),
        occurred_at: Utc::now(),
    });

    assert_eq!(*customer.id(), 7);
    assert_eq!(customer.version(), 0);
    assert_eq!(customer.pending_events().len(), 1);
    assert_eq!(customer.pending_events()[0].event_type(), "Registered");
    assert_eq!(customer.pending_events()[0].aggregate_id(), "7");

    let out = dispatch(&RegisterHandler, RegisterCustomer { id: 7 }).await?;
    assert_eq!(out, 7);

    let found = query_dispatch(&GetHandler, GetCustomer { id: 7 }).await?;
    assert_eq!(found, Some(7));
    Ok(())
}
