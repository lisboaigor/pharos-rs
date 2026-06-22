//! Testing utilities for Pharos RS.
//!
//! This crate makes it pleasant to assert on the domain events an application
//! emits. Register an [`EventCapture`] on an [`EventBus`] (or build a pre-wired
//! pair with [`capturing_event_bus`]) and inspect what was published.
//!
//! It also provides [`TestSubscriber`], a `tracing` subscriber that captures
//! span and log records in memory so tests can assert on observability output
//! without depending on a real OTLP endpoint.
//!
//! ```
//! use chrono::{DateTime, Utc};
//! use pharos_core::DomainEvent;
//! use pharos_testing::{assert_event_published, capturing_event_bus};
//!
//! #[derive(Clone)]
//! struct Shipped { id: String, at: DateTime<Utc> }
//! impl DomainEvent for Shipped {
//!     fn event_type(&self) -> &'static str { "Shipped" }
//!     fn occurred_at(&self) -> DateTime<Utc> { self.at }
//!     fn aggregate_id(&self) -> &str { &self.id }
//! }
//!
//! # async fn run() {
//! let (bus, capture) = capturing_event_bus::<Shipped>();
//! bus.publish(&Shipped { id: "order-1".into(), at: Utc::now() }).await.unwrap();
//!
//! assert_event_published!(capture, 1);
//! assert_eq!(capture.events()[0].aggregate_id(), "order-1");
//! # }
//! ```

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use pharos_app::{EventBus, EventHandler};
use pharos_core::DomainEvent;

/// Captures domain events of type `E` published through an [`EventBus`].
///
/// `EventCapture` is cheap to clone; every clone observes the same captured
/// events. Register it on a bus with [`EventCapture::register_on`], or use
/// [`capturing_event_bus`] to build a bus that already has it wired in.
pub struct EventCapture<E> {
    events: Arc<Mutex<Vec<E>>>,
}

impl<E> Clone for EventCapture<E> {
    fn clone(&self) -> Self {
        Self {
            events: Arc::clone(&self.events),
        }
    }
}

impl<E> Default for EventCapture<E> {
    fn default() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<E> EventCapture<E>
where
    E: DomainEvent + Clone,
{
    /// Creates an empty capture.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers this capture as a handler for `E` on the given bus.
    pub fn register_on(&self, bus: &EventBus) {
        bus.register::<E, _>(CapturingHandler {
            events: Arc::clone(&self.events),
        });
    }

    /// Returns a clone of every captured event, in publication order.
    pub fn events(&self) -> Vec<E> {
        self.events.lock().expect("event capture poisoned").clone()
    }

    /// Returns the number of captured events.
    pub fn count(&self) -> usize {
        self.events.lock().expect("event capture poisoned").len()
    }

    /// Returns `true` when no events were captured.
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Clears all captured events.
    pub fn clear(&self) {
        self.events.lock().expect("event capture poisoned").clear();
    }
}

struct CapturingHandler<E> {
    events: Arc<Mutex<Vec<E>>>,
}

impl<E> EventHandler<E> for CapturingHandler<E>
where
    E: DomainEvent + Clone,
{
    type Error = Infallible;

    async fn handle(&self, event: &E) -> Result<(), Self::Error> {
        self.events
            .lock()
            .expect("event capture poisoned")
            .push(event.clone());
        Ok(())
    }
}

/// Builds an [`EventBus`] with an [`EventCapture`] for `E` already registered.
pub fn capturing_event_bus<E>() -> (EventBus, EventCapture<E>)
where
    E: DomainEvent + Clone,
{
    let bus = EventBus::new();
    let capture = EventCapture::<E>::new();
    capture.register_on(&bus);
    (bus, capture)
}

/// Asserts that exactly `$count` events were captured.
#[macro_export]
macro_rules! assert_event_published {
    ($capture:expr, $count:expr) => {
        assert_eq!(
            $capture.count(),
            $count,
            "expected {} captured event(s), got {}",
            $count,
            $capture.count(),
        );
    };
}

// ─── TestSubscriber ───────────────────────────────────────────────────────────

/// A `tracing` subscriber that captures formatted log lines in memory.
///
/// Install it at the start of a test to capture all tracing output without
/// sending it to a real OTLP endpoint. Useful for asserting on span names,
/// field values, or correlation IDs produced by the framework.
///
/// ```
/// use pharos_testing::TestSubscriber;
///
/// let sub = TestSubscriber::new();
/// let _guard = sub.install();
/// // … run code under test …
/// let lines = sub.lines();
/// assert!(lines.iter().any(|l| l.contains("postgres.outbox.insert")));
/// ```
pub struct TestSubscriber {
    lines: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl TestSubscriber {
    /// Creates a new subscriber with an empty capture buffer.
    pub fn new() -> Self {
        Self {
            lines: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Installs this subscriber as the default for the current thread, returning
    /// a guard that uninstalls it on drop.
    ///
    /// Only one subscriber can be the default at a time. If a subscriber is
    /// already installed (e.g. from another test in the same process), this
    /// falls back to a no-op and returns a guard that does nothing on drop.
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        use tracing_subscriber::fmt;

        let writer = MemWriter {
            lines: std::sync::Arc::clone(&self.lines),
        };
        let subscriber = fmt::Subscriber::builder()
            .with_writer(move || writer.clone())
            .finish();
        // `set_default` returns a guard; ignore errors from re-entrant installs.
        tracing::subscriber::set_default(subscriber)
    }

    /// Returns a snapshot of all captured log lines.
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().expect("test subscriber poisoned").clone()
    }

    /// Returns `true` if any captured line contains `needle`.
    pub fn contains(&self, needle: &str) -> bool {
        self.lines().iter().any(|l| l.contains(needle))
    }

    /// Clears the captured lines.
    pub fn clear(&self) {
        self.lines.lock().expect("test subscriber poisoned").clear();
    }
}

impl Default for TestSubscriber {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
struct MemWriter {
    lines: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl std::io::Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(s) = std::str::from_utf8(buf) {
            let trimmed = s.trim_end_matches('\n').to_string();
            if !trimmed.is_empty() {
                self.lines
                    .lock()
                    .expect("mem writer poisoned")
                    .push(trimmed);
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    #[derive(Clone)]
    struct Sample {
        id: String,
        at: DateTime<Utc>,
    }

    impl DomainEvent for Sample {
        fn event_type(&self) -> &'static str {
            "Sample"
        }
        fn occurred_at(&self) -> DateTime<Utc> {
            self.at
        }
        fn aggregate_id(&self) -> &str {
            &self.id
        }
    }

    #[tokio::test]
    async fn captures_published_events() {
        let (bus, capture) = capturing_event_bus::<Sample>();
        assert!(capture.is_empty());

        bus.publish(&Sample {
            id: "a-1".into(),
            at: Utc::now(),
        })
        .await
        .unwrap();
        bus.publish(&Sample {
            id: "a-2".into(),
            at: Utc::now(),
        })
        .await
        .unwrap();

        assert_event_published!(capture, 2);
        let ids: Vec<_> = capture
            .events()
            .iter()
            .map(|e| e.aggregate_id().to_string())
            .collect();
        assert_eq!(ids, vec!["a-1", "a-2"]);

        capture.clear();
        assert!(capture.is_empty());
    }
}
