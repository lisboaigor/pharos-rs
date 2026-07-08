//! The `billing` bounded context.
//!
//! It owns the [`Invoice`] aggregate. Billing does not import the orders domain
//! model; the composition root (see `lib.rs`) translates an order event into a
//! call to [`issue_invoice`].

use chrono::{DateTime, Utc};
use pharos_core::{AggregateEvents, AggregateRoot, DomainEvent, Entity, Repository};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// An invoice issued for an order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invoice {
    id: String,
    order_id: String,
    amount_cents: i64,
    #[serde(default)]
    version: u64,
    #[serde(skip)]
    events: AggregateEvents<InvoiceIssued>,
}

impl Invoice {
    /// Issues an invoice for an order, raising an [`InvoiceIssued`] event.
    pub fn issue(id: impl Into<String>, order_id: impl Into<String>, amount_cents: i64) -> Self {
        let id = id.into();
        let order_id = order_id.into();
        let mut events = AggregateEvents::default();
        events.raise(InvoiceIssued {
            invoice_id: id.clone(),
            order_id: order_id.clone(),
            amount_cents,
            occurred_at: Utc::now(),
        });
        Self {
            id,
            order_id,
            amount_cents,
            version: 0,
            events,
        }
    }

    /// Returns the order this invoice bills.
    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    /// Returns the invoice amount in cents.
    pub fn amount_cents(&self) -> i64 {
        self.amount_cents
    }
}

impl Entity for Invoice {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl AggregateRoot for Invoice {
    type Event = InvoiceIssued;

    fn pending_events(&self) -> &[Self::Event] {
        self.events.pending()
    }

    fn drain_events(&mut self) -> Vec<Self::Event> {
        self.events.drain()
    }

    fn version(&self) -> u64 {
        self.version
    }

    fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

/// Raised when an invoice is issued.
#[derive(Debug, Clone)]
pub struct InvoiceIssued {
    /// Invoice identifier.
    pub invoice_id: String,
    /// Order the invoice bills.
    pub order_id: String,
    /// Invoice amount in cents.
    pub amount_cents: i64,
    /// When the invoice was issued.
    pub occurred_at: DateTime<Utc>,
}

impl DomainEvent for InvoiceIssued {
    fn event_type(&self) -> &'static str {
        "InvoiceIssued"
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    fn aggregate_id(&self) -> &str {
        &self.invoice_id
    }
}

/// Errors produced by the billing context.
#[derive(Debug, Error)]
pub enum BillingError {
    /// Persisting the invoice failed.
    #[error("failed to persist invoice: {0}")]
    Persist(String),
}

/// Issues and persists an invoice for an order through the given repository.
pub async fn issue_invoice<R>(
    invoices: &R,
    order_id: &str,
    amount_cents: i64,
) -> Result<Invoice, BillingError>
where
    R: Repository<Invoice>,
{
    let mut invoice = Invoice::issue(format!("inv-{order_id}"), order_id, amount_cents);
    invoices
        .save(&mut invoice)
        .await
        .map_err(|error| BillingError::Persist(error.to_string()))?;
    Ok(invoice)
}
