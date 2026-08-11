//! Fan-out pub/sub contracts: [`RoomId`], [`RealtimeMessage`], and the
//! [`RealtimePublisher`]/[`RealtimeSubscriber`] traits.
//!
//! The shape deliberately mirrors `pharos_messaging::messaging` (plain data
//! struct + thin `Send + Sync + 'static` trait), but these are **new**
//! traits, not a reuse of the broker-facing ones: broadcasting to a room is
//! fan-out-to-many with no ack/nack, a different delivery contract than
//! point-to-point message consumption. A room with zero subscribers is not an
//! error — publishing is fire-and-forget, same as a chat room nobody is
//! currently looking at.

use std::future::Future;
use std::sync::Arc;

use futures::Stream;
use thiserror::Error;

/// The longest a [`RoomId`] may be. Rooms are map keys and span fields, not
/// payloads; anything longer is a mistake or an attempt to bloat them.
const MAX_ROOM_ID_LEN: usize = 128;

/// Rejected room id, with the reason.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidRoomId {
    /// The id was empty.
    #[error("room id must not be empty")]
    Empty,
    /// The id exceeded [`MAX_ROOM_ID_LEN`] bytes.
    #[error("room id is {len} bytes, over the {MAX_ROOM_ID_LEN}-byte limit")]
    TooLong {
        /// Length of the rejected id.
        len: usize,
    },
    /// The id contained a character outside the allowed set.
    #[error("room id contains the disallowed character {character:?}")]
    DisallowedCharacter {
        /// The first offending character.
        character: char,
    },
}

/// Identifies a fan-out room a [`RealtimeMessage`] is broadcast to.
///
/// Convention: `RoomId::parse(format!("game:{game_id}"))`. The *meaning* of
/// the string is up to the embedding app; its **shape** is not.
///
/// # Why parsing is fallible
///
/// A room id is not an opaque blob. It is a key in the hub's room map, a
/// tracing span field, and — for any backend other than the in-process one —
/// a broker subject. Accepting arbitrary strings hands each of those a
/// different problem: unbounded ids bloat the map, control characters forge
/// log lines, and NATS treats `*` and `>` as subject wildcards, so a room id
/// of `>` would subscribe to *every subject on the server*. Restricting the
/// character set once, here, closes all three at the boundary rather than in
/// each backend.
///
/// Allowed: ASCII letters, digits, and `: - _ . /`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoomId(String);

impl RoomId {
    /// Parses a room id, rejecting anything outside the allowed shape.
    pub fn parse(id: impl AsRef<str>) -> Result<Self, InvalidRoomId> {
        let id = id.as_ref();
        if id.is_empty() {
            return Err(InvalidRoomId::Empty);
        }
        if id.len() > MAX_ROOM_ID_LEN {
            return Err(InvalidRoomId::TooLong { len: id.len() });
        }
        if let Some(character) = id.chars().find(|c| !Self::is_allowed(*c)) {
            return Err(InvalidRoomId::DisallowedCharacter { character });
        }
        Ok(Self(id.to_owned()))
    }

    fn is_allowed(c: char) -> bool {
        c.is_ascii_alphanumeric() || matches!(c, ':' | '-' | '_' | '.' | '/')
    }

    /// Returns the room id as a plain string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RoomId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for RoomId {
    type Err = InvalidRoomId;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::parse(id)
    }
}

/// A message broadcast to every current subscriber of a [`RoomId`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealtimeMessage {
    /// The room this message fans out to.
    pub room: RoomId,
    /// Server-side discriminator for the payload (e.g. `"MoveMade"`).
    ///
    /// **This never reaches the client.** The outbound pump writes
    /// `payload` and nothing else, because the wire format belongs to the
    /// embedding app — a framing invented here would fight whatever the app
    /// already encodes. `kind` exists for the server's own telemetry: it is a
    /// `&'static str`, so it is the one field safe to use as a metric label
    /// (a `RoomId` comes from the request and would blow up label
    /// cardinality). If the client needs to discriminate message types, the
    /// app must encode that inside `payload`.
    pub kind: &'static str,
    /// Serialized payload. `pharos-realtime` never inspects or decodes this —
    /// the embedding app owns the wire format.
    pub payload: Vec<u8>,
}

impl RealtimeMessage {
    /// Creates a new fan-out message.
    pub fn new(room: RoomId, kind: &'static str, payload: Vec<u8>) -> Self {
        Self {
            room,
            kind,
            payload,
        }
    }
}

/// Error returned by a realtime hub, a [`super::auth::ConnectionAuthenticator`],
/// or the WebSocket glue in [`super::ws`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RealtimeError {
    /// Publishing to a room failed; the source carries the original backend
    /// error.
    #[error("realtime publish failed: {0}")]
    Publish(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Subscribing to a room failed; the source carries the original backend
    /// error.
    #[error("realtime subscribe failed: {0}")]
    Subscribe(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// A connection could not be authenticated and must not be upgraded.
    ///
    /// "We do not know who you are" — maps to `401`.
    #[error("realtime connection unauthorized: {0}")]
    Unauthorized(String),
    /// A known principal is not allowed to touch this room.
    ///
    /// "We know who you are and the answer is no" — maps to `403`. Kept
    /// distinct from [`Unauthorized`](Self::Unauthorized) so a client can tell
    /// "log in again" from "stop asking", and so revocation mid-connection is
    /// reported for what it is.
    #[error("realtime access forbidden for {identity} on room {room}")]
    Forbidden {
        /// The principal that was denied.
        identity: String,
        /// The room it was denied on.
        room: String,
    },
    /// A limit protecting the process was reached (rooms, connections).
    #[error("realtime capacity exceeded: {0}")]
    Capacity(String),
    /// A room id did not have the required shape.
    #[error("invalid room id: {0}")]
    InvalidRoom(#[from] InvalidRoomId),
    /// The WebSocket transport itself failed (upgrade rejection, send/recv
    /// error on an established connection).
    #[error("realtime transport failed: {0}")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl RealtimeError {
    /// Wraps any `Error + Send + Sync + 'static` as a publish failure.
    pub fn publish(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Publish(Box::new(e))
    }
    /// Wraps any `Error + Send + Sync + 'static` as a subscribe failure.
    pub fn subscribe(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Subscribe(Box::new(e))
    }
    /// Wraps any `Error + Send + Sync + 'static` as a transport failure.
    pub fn transport(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Transport(Box::new(e))
    }
}

/// Publishes fan-out messages into a room.
///
/// No ack/nack: a publish that reaches zero subscribers still returns `Ok`.
/// Delivery is best-effort to whoever is subscribed *right now* — there is no
/// replay buffer for a subscriber that joins later (out of scope for the MVP,
/// see the plan's "Fora de escopo").
pub trait RealtimePublisher: Send + Sync + 'static {
    /// Publishes one message to `msg.room`.
    fn publish(
        &self,
        msg: RealtimeMessage,
    ) -> impl Future<Output = Result<(), RealtimeError>> + Send;
}

/// Subscribes to a room's fan-out stream.
pub trait RealtimeSubscriber: Send + Sync + 'static {
    /// The stream of messages yielded to a subscriber of a room.
    type Stream: Stream<Item = RealtimeMessage> + Send + 'static;

    /// Joins `room`, returning a stream of every message published to it from
    /// this point on.
    fn subscribe(
        &self,
        room: &RoomId,
    ) -> impl Future<Output = Result<Self::Stream, RealtimeError>> + Send;
}

/// Convenience bound for anything that is both a publisher and a subscriber —
/// the shape `pharos_realtime::ws` glue needs. `InMemoryHub` implements both;
/// a future NATS-backed hub (Fase 7+ per the plan) would too.
pub trait RealtimeHub: RealtimePublisher + RealtimeSubscriber {}

impl<T: RealtimePublisher + RealtimeSubscriber> RealtimeHub for T {}

// Shared handles delegate, so an `Arc<H>` can be cloned into WS connection
// tasks, event handlers, and tests without a wrapper type — mirrors
// `pharos_messaging::messaging`'s `Arc<P>`/`Arc<C>`/`Arc<A>` impls.
impl<P: RealtimePublisher> RealtimePublisher for Arc<P> {
    fn publish(
        &self,
        msg: RealtimeMessage,
    ) -> impl Future<Output = Result<(), RealtimeError>> + Send {
        (**self).publish(msg)
    }
}

impl<S: RealtimeSubscriber> RealtimeSubscriber for Arc<S> {
    type Stream = S::Stream;

    fn subscribe(
        &self,
        room: &RoomId,
    ) -> impl Future<Output = Result<Self::Stream, RealtimeError>> + Send {
        (**self).subscribe(room)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room(id: &str) -> RoomId {
        let Ok(room) = RoomId::parse(id) else {
            panic!("`{id}` should be a valid room id");
        };
        room
    }

    #[test]
    fn room_id_displays_as_its_inner_string() {
        let room = room("game:abc-123");
        assert_eq!(room.as_str(), "game:abc-123");
        assert_eq!(room.to_string(), "game:abc-123");
    }

    #[test]
    fn room_id_accepts_the_conventional_shapes() {
        for id in ["game:1", "tenant/acme/orders", "a.b-c_d", "ABC123"] {
            assert!(RoomId::parse(id).is_ok(), "`{id}` should parse");
        }
    }

    #[test]
    fn room_id_rejects_nats_subject_wildcards() {
        // The reason this matters: a NATS-backed hub maps a room onto a
        // subject, where `>` matches every subject on the server. Letting one
        // through would turn a subscribe into a firehose across every tenant.
        for id in [">", "*", "game:*", "game:>"] {
            assert!(
                matches!(
                    RoomId::parse(id),
                    Err(InvalidRoomId::DisallowedCharacter { .. })
                ),
                "`{id}` must be rejected"
            );
        }
    }

    #[test]
    fn room_id_rejects_empty_oversized_and_control_characters() {
        assert_eq!(RoomId::parse(""), Err(InvalidRoomId::Empty));
        assert_eq!(
            RoomId::parse("x".repeat(MAX_ROOM_ID_LEN + 1)),
            Err(InvalidRoomId::TooLong {
                len: MAX_ROOM_ID_LEN + 1
            })
        );
        for id in ["game\n1", "game 1", "game\t1", "game\u{0}1"] {
            assert!(
                matches!(
                    RoomId::parse(id),
                    Err(InvalidRoomId::DisallowedCharacter { .. })
                ),
                "`{id:?}` must be rejected"
            );
        }
    }
}
