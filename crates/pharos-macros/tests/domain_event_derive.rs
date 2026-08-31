//! Integration coverage for `#[derive(DomainEvent)]`, in particular the
//! `#[event(name = "...")]` override.
//!
//! `event_type()` is the event's identity on the wire: an `EventBus` topic
//! decoder and an `EventUpcasterRegistry` both key on it. Without an
//! explicit override it defaults to the variant's Rust identifier, so a
//! plain rename silently changes routing for events already written under
//! the old name. This test proves the override actually takes effect and
//! that the default still falls back to the identifier when no override is
//! given.

use chrono::{DateTime, Utc};
use pharos_core::DomainEvent;
use pharos_macros::DomainEvent;

#[derive(DomainEvent)]
enum OrderEvent {
    // No override: falls back to the variant identifier.
    Placed {
        #[aggregate_id]
        order_id: String,
        #[occurred_at]
        at: DateTime<Utc>,
    },
    // Renamed on the Rust side after events already existed on the wire
    // under "OrderConfirmed" — the override keeps routing stable.
    #[event(name = "OrderConfirmed")]
    Confirmed {
        #[aggregate_id]
        order_id: String,
        #[occurred_at]
        at: DateTime<Utc>,
    },
    // A payload shape bumped after events already existed under version 0.
    #[event(schema_version = 2)]
    Shipped {
        #[aggregate_id]
        order_id: String,
        #[occurred_at]
        at: DateTime<Utc>,
    },
}

#[test]
fn event_type_defaults_to_the_variant_identifier() {
    let event = OrderEvent::Placed {
        order_id: "order-1".into(),
        at: Utc::now(),
    };
    assert_eq!(event.event_type(), "Placed");
}

#[test]
fn event_type_uses_the_explicit_override_over_the_renamed_variant() {
    let event = OrderEvent::Confirmed {
        order_id: "order-1".into(),
        at: Utc::now(),
    };
    assert_eq!(event.event_type(), "OrderConfirmed");
}

#[test]
fn schema_version_defaults_to_zero_and_can_be_overridden_per_variant() {
    let placed = OrderEvent::Placed {
        order_id: "order-1".into(),
        at: Utc::now(),
    };
    assert_eq!(placed.schema_version(), 0);

    let shipped = OrderEvent::Shipped {
        order_id: "order-1".into(),
        at: Utc::now(),
    };
    assert_eq!(shipped.schema_version(), 2);
}
