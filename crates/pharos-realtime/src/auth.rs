//! [`ConnectionAuthenticator`] — the seam an embedding app uses to reject an
//! unauthenticated WebSocket connection before the upgrade completes.
//!
//! Deliberately app-agnostic: `pharos-realtime` never knows what a principal
//! *is* (a player, a user, a service account). `bitchess` implements this
//! trait by resolving a session token — the same one `Login`'s REST handler
//! already mints — into a `PlayerId`, then wraps it as an opaque [`Identity`].

use std::future::Future;
use std::sync::Arc;

use axum::http::request::Parts;

use crate::hub::{RealtimeError, RoomId};

/// An opaque principal identifier established for a realtime connection.
///
/// A thin newtype over a string id. `pharos-realtime` only ever displays or
/// compares it (for tracing/metrics labels); it never parses or interprets
/// the value — that is entirely the embedding app's concern.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identity(String);

impl Identity {
    /// Wraps any string-like id as an opaque identity.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the identity as a plain string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Authenticates a connection before its WebSocket upgrade completes.
///
/// Implementations read a session token from `parts` and resolve it against
/// the app's own session store. Returning `Err` rejects the connection
/// outright — the upgrade never happens, so an unauthenticated socket is
/// never opened.
///
/// # Read the token from a header, not the query string
///
/// A query string is not a private channel: it lands in access logs, proxy
/// logs, and `Referer` headers, and `pharos_axum::observability::request_span`
/// records the full URI — query included — on a span built at `INFO`, so a
/// token in `?token=…` is written to the application's own logs on every
/// handshake. Prefer `Sec-WebSocket-Protocol` or an `Authorization` header.
/// If a browser API forces a query parameter, make that token single-use and
/// short-lived, and redact it before it reaches a subscriber.
pub trait ConnectionAuthenticator: Send + Sync + 'static {
    /// Authenticates the connection described by `parts`, returning the
    /// resolved [`Identity`] or a [`RealtimeError::Unauthorized`].
    ///
    /// This runs on every revalidation tick as well as at handshake time, so
    /// it must reflect the *current* state of the session — an implementation
    /// that caches a token's validity forever defeats
    /// [`RealtimeConfig::revalidate_every`](crate::ws::RealtimeConfig).
    fn authenticate(
        &self,
        parts: &Parts,
    ) -> impl Future<Output = Result<Identity, RealtimeError>> + Send;
}

impl<A: ConnectionAuthenticator> ConnectionAuthenticator for Arc<A> {
    fn authenticate(
        &self,
        parts: &Parts,
    ) -> impl Future<Output = Result<Identity, RealtimeError>> + Send {
        (**self).authenticate(parts)
    }
}

/// What a principal wants to do with a room.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Access {
    /// Receive the room's broadcasts.
    Subscribe,
    /// Send an inbound frame that the app will act on for this room.
    Publish,
}

impl Access {
    /// A stable, low-cardinality label for metrics and spans.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Subscribe => "subscribe",
            Self::Publish => "publish",
        }
    }
}

impl std::fmt::Display for Access {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Decides whether an [`Identity`] may [`Access`] a specific [`RoomId`].
///
/// # Why this is a separate, mandatory trait
///
/// Authenticating a connection answers "who is this?", which is not the same
/// question as "may this person watch *that* game?". A handshake-only check
/// authorizes the socket, not the resource, and the room usually comes
/// straight from the request path — so without a per-resource decision the
/// natural implementation of a WebSocket route is an IDOR:
/// `subscribe("tenant-b/orders")` succeeds for anyone who can log in.
///
/// [`upgrade`](crate::ws::upgrade) therefore requires an authorizer rather
/// than accepting an `Option`. Writing one that returns `Ok(())` is a
/// deliberate, visible decision; having nowhere to write one is not.
///
/// The split between [`Access::Subscribe`] and [`Access::Publish`] exists
/// because read and write permission on a room are rarely the same — a
/// spectator may watch a match without being able to move a piece in it.
/// Subscribe is checked once before the socket joins the room and again on
/// every revalidation tick; publish is checked on **every inbound frame**,
/// so a permission that is revoked mid-connection stops the next message
/// rather than the next reconnect.
pub trait RoomAuthorizer: Send + Sync + 'static {
    /// Authorizes `identity` for `access` on `room`.
    ///
    /// Return [`RealtimeError::Forbidden`] to deny. Any other error is
    /// treated as a denial too — an authorizer that cannot reach its policy
    /// store must fail closed.
    fn authorize(
        &self,
        identity: &Identity,
        room: &RoomId,
        access: Access,
    ) -> impl Future<Output = Result<(), RealtimeError>> + Send;
}

impl<A: RoomAuthorizer> RoomAuthorizer for Arc<A> {
    fn authorize(
        &self,
        identity: &Identity,
        room: &RoomId,
        access: Access,
    ) -> impl Future<Output = Result<(), RealtimeError>> + Send {
        (**self).authorize(identity, room, access)
    }
}

/// Builds a [`RealtimeError::Forbidden`] for `identity` on `room`.
///
/// A convenience so implementations do not have to stringify both sides by
/// hand on every deny path.
pub fn forbidden(identity: &Identity, room: &RoomId) -> RealtimeError {
    RealtimeError::Forbidden {
        identity: identity.to_string(),
        room: room.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AlwaysRejects;

    impl ConnectionAuthenticator for AlwaysRejects {
        async fn authenticate(&self, _parts: &Parts) -> Result<Identity, RealtimeError> {
            Err(RealtimeError::Unauthorized("no credentials".to_string()))
        }
    }

    #[tokio::test]
    async fn identity_display_matches_its_inner_string() {
        let identity = Identity::new("player-42");
        assert_eq!(identity.as_str(), "player-42");
        assert_eq!(identity.to_string(), "player-42");
    }

    #[tokio::test]
    async fn rejecting_authenticator_surfaces_unauthorized() {
        let parts = axum::http::Request::new(()).into_parts().0;
        let result = AlwaysRejects.authenticate(&parts).await;
        assert!(matches!(result, Err(RealtimeError::Unauthorized(_))));
    }
}
