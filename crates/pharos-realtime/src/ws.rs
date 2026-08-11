//! Generic axum WebSocket upgrade/pump glue — chess-agnostic, app-agnostic.
//!
//! [`Realtime`] is not itself an axum [`Handler`](axum::handler::Handler); it
//! is held in application state and called *from* one, after the app has
//! extracted [`WebSocketUpgrade`] and the request
//! [`Parts`](axum::http::request::Parts) its
//! [`ConnectionAuthenticator`] needs (both are ordinary, non-destructive axum
//! extractors — `axum_core` implements `FromRequestParts` for `Parts` itself
//! by cloning it, so extracting it alongside `WebSocketUpgrade` in the same
//! handler is always safe).
//!
//! # What a connection is subject to
//!
//! Authentication answers *who*, authorization answers *what*, and both are
//! rechecked while the socket is open rather than only at the handshake:
//!
//! - the [`ConnectionAuthenticator`] resolves an [`Identity`] before the
//!   upgrade, and again on every [`RealtimeConfig::revalidate_every`] tick;
//! - the [`RoomAuthorizer`] approves [`Access::Subscribe`] before the socket
//!   joins the room, again on each revalidation tick, and approves
//!   [`Access::Publish`] on **every inbound frame**;
//! - a revalidation that fails closes the connection, so logout, token
//!   expiry, a permission change, or a tenant move takes effect within one
//!   tick instead of lasting as long as the client keeps the socket open;
//! - a server-sent ping every [`RealtimeConfig::heartbeat_every`] with a
//!   [`RealtimeConfig::heartbeat_timeout`] deadline reaps half-open
//!   connections that TCP alone would hold forever;
//! - inbound frames are capped by [`RealtimeConfig::max_message_size`], well
//!   under the 64 MiB tungstenite would otherwise allow per message.
//!
//! ```ignore
//! use axum::extract::{Path, State, WebSocketUpgrade};
//! use axum::http::request::Parts;
//! use axum::response::{IntoResponse, Response};
//! use pharos_realtime::RoomId;
//!
//! async fn ws_games(
//!     ws: WebSocketUpgrade,
//!     parts: Parts,
//!     Path(game_id): Path<String>,
//!     State(state): State<AppState>,
//! ) -> Response {
//!     let room = match RoomId::parse(format!("game:{game_id}")) {
//!         Ok(room) => room,
//!         Err(error) => return RealtimeError::from(error).into_response(),
//!     };
//!     match state.realtime.upgrade(ws, &parts, room).await {
//!         Ok(response) => response,
//!         Err(error) => error.into_response(),
//!     }
//! }
//! ```

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use futures::{SinkExt, StreamExt};
use tokio::time::{Instant, MissedTickBehavior, interval};
use tracing::{Instrument, info_span};

use crate::auth::{Access, ConnectionAuthenticator, Identity, RoomAuthorizer};
use crate::hub::{RealtimeError, RealtimeHub, RealtimeSubscriber, RoomId};

/// Reacts to one inbound WebSocket frame's payload bytes.
///
/// The embedding app implements this to decode frames into its own commands —
/// deserializing into a command and dispatching it through the same
/// `pharos_app::dispatch` path its REST route uses, for instance. An `Err` is
/// logged and the frame is dropped; it does not close the connection — one
/// malformed frame should not tear down an otherwise-healthy socket.
///
/// `room` is supplied so the handler can scope what it does to the room the
/// frame arrived on. It is not a substitute for authorization: the
/// [`RoomAuthorizer`] has already approved [`Access::Publish`] for this
/// identity on this room before `on_message` is called. It *is* still the
/// handler's job to check that the fields inside `payload` refer to resources
/// this `identity` owns — the framework cannot know what the bytes mean.
pub trait OnMessage: Send + Sync + 'static {
    /// Handles one inbound frame's payload, from the connection identified by
    /// `identity`, on `room`.
    fn on_message(
        &self,
        identity: &Identity,
        room: &RoomId,
        payload: Bytes,
    ) -> impl Future<Output = Result<(), RealtimeError>> + Send;
}

impl<M: OnMessage> OnMessage for Arc<M> {
    fn on_message(
        &self,
        identity: &Identity,
        room: &RoomId,
        payload: Bytes,
    ) -> impl Future<Output = Result<(), RealtimeError>> + Send {
        (**self).on_message(identity, room, payload)
    }
}

impl IntoResponse for RealtimeError {
    fn into_response(self) -> Response {
        let status = match &self {
            RealtimeError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            RealtimeError::Forbidden { .. } => StatusCode::FORBIDDEN,
            RealtimeError::InvalidRoom(_) => StatusCode::BAD_REQUEST,
            RealtimeError::Capacity(_) => StatusCode::SERVICE_UNAVAILABLE,
            RealtimeError::Publish(_)
            | RealtimeError::Subscribe(_)
            | RealtimeError::Transport(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        // The body is deliberately the status' canonical reason rather than
        // the error text: `Forbidden` carries the identity and room, and
        // `Capacity` carries the hub's limits, neither of which belongs in a
        // response to the party that just got refused.
        let body = status.canonical_reason().unwrap_or("realtime error");
        tracing::warn!(error = %self, "realtime connection refused");
        (status, body).into_response()
    }
}

/// Limits and intervals applied to every connection.
///
/// The defaults are deliberately strict; loosen them explicitly rather than
/// discovering the ceiling in production.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealtimeConfig {
    /// Largest inbound message accepted, in bytes.
    ///
    /// tungstenite's default is 64 MiB per message and 16 MiB per frame,
    /// which for a control channel is an invitation: each in-flight message
    /// is buffered per connection, and nothing else here caps how many
    /// connections there are.
    pub max_message_size: usize,
    /// Largest inbound frame accepted, in bytes.
    pub max_frame_size: usize,
    /// How often the identity and its subscribe permission are rechecked.
    pub revalidate_every: Duration,
    /// How often a server ping is sent.
    pub heartbeat_every: Duration,
    /// How long the connection may go without any inbound frame before it is
    /// closed. Must exceed [`heartbeat_every`](Self::heartbeat_every) or every
    /// connection is reaped on the first tick.
    pub heartbeat_timeout: Duration,
}

impl Default for RealtimeConfig {
    fn default() -> Self {
        Self {
            max_message_size: 64 * 1024,
            max_frame_size: 64 * 1024,
            revalidate_every: Duration::from_secs(60),
            heartbeat_every: Duration::from_secs(20),
            heartbeat_timeout: Duration::from_secs(60),
        }
    }
}

/// Everything a realtime route needs, assembled once and held in app state.
///
/// The constructor takes the [`RoomAuthorizer`] as a required argument rather
/// than an option: per-resource authorization is the difference between "this
/// person may open a socket" and "this person may watch *that* match", and a
/// framework that lets the second question go unasked produces IDOR by
/// default. An authorizer that returns `Ok(())` is a visible decision; no
/// authorizer at all is an invisible one.
pub struct Realtime<H, A, Z, M> {
    hub: Arc<H>,
    authenticator: Arc<A>,
    authorizer: Arc<Z>,
    on_message: Arc<M>,
    config: RealtimeConfig,
}

impl<H, A, Z, M> Clone for Realtime<H, A, Z, M> {
    fn clone(&self) -> Self {
        Self {
            hub: Arc::clone(&self.hub),
            authenticator: Arc::clone(&self.authenticator),
            authorizer: Arc::clone(&self.authorizer),
            on_message: Arc::clone(&self.on_message),
            config: self.config,
        }
    }
}

impl<H, A, Z, M> Realtime<H, A, Z, M>
where
    H: RealtimeHub + 'static,
    A: ConnectionAuthenticator,
    Z: RoomAuthorizer,
    M: OnMessage,
{
    /// Assembles a realtime route with the default [`RealtimeConfig`].
    pub fn new(hub: Arc<H>, authenticator: Arc<A>, authorizer: Arc<Z>, on_message: Arc<M>) -> Self {
        Self {
            hub,
            authenticator,
            authorizer,
            on_message,
            config: RealtimeConfig::default(),
        }
    }

    /// Overrides the connection limits and intervals.
    pub fn with_config(mut self, config: RealtimeConfig) -> Self {
        self.config = config;
        self
    }

    /// Returns the configuration in force.
    pub fn config(&self) -> RealtimeConfig {
        self.config
    }

    /// Authenticates and authorizes a connection and, on success, upgrades it
    /// to a WebSocket pumping messages between the hub and the socket.
    ///
    /// `parts` must come from the same request `ws` was extracted from. On
    /// refusal the socket is never upgraded and the error maps to an HTTP
    /// status via [`RealtimeError`]'s `IntoResponse`.
    pub async fn upgrade(
        &self,
        ws: WebSocketUpgrade,
        parts: &Parts,
        room: RoomId,
    ) -> Result<Response, RealtimeError> {
        let identity = self.authenticator.authenticate(parts).await?;
        self.authorizer
            .authorize(&identity, &room, Access::Subscribe)
            .await?;

        let room_label = room.to_string();
        let identity_label = identity.to_string();

        let connection = Connection {
            hub: Arc::clone(&self.hub),
            authenticator: Arc::clone(&self.authenticator),
            authorizer: Arc::clone(&self.authorizer),
            on_message: Arc::clone(&self.on_message),
            config: self.config,
            // Revalidation has to re-run the authenticator, which needs the
            // request head; the socket outlives the borrow, so it is cloned.
            parts: parts.clone(),
            identity,
            room,
        };

        let ws = ws
            .max_message_size(self.config.max_message_size)
            .max_frame_size(self.config.max_frame_size);

        Ok(ws.on_upgrade(move |socket| {
            async move {
                metrics::gauge!("pharos.realtime.connections.active").increment(1.0);
                tracing::info!("realtime connection opened");

                let closed_because = connection.run(socket).await;

                metrics::gauge!("pharos.realtime.connections.active").decrement(1.0);
                metrics::counter!(
                    "pharos.realtime.connections.closed",
                    "reason" => closed_because.as_str()
                )
                .increment(1);
                tracing::info!(
                    reason = closed_because.as_str(),
                    "realtime connection closed"
                );
            }
            .instrument(info_span!(
                "realtime.ws",
                room = room_label.as_str(),
                identity = identity_label.as_str(),
            ))
        }))
    }
}

/// Why a connection ended. A bounded set, so it is safe as a metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseReason {
    /// The client closed, or the transport failed.
    ClientGone,
    /// The room's stream ended.
    RoomClosed,
    /// The hub refused the subscription.
    SubscribeFailed,
    /// Identity or permission no longer valid.
    Revoked,
    /// No inbound traffic within the heartbeat deadline.
    Timeout,
}

impl CloseReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::ClientGone => "client_gone",
            Self::RoomClosed => "room_closed",
            Self::SubscribeFailed => "subscribe_failed",
            Self::Revoked => "revoked",
            Self::Timeout => "timeout",
        }
    }
}

/// One live connection's state.
struct Connection<H, A, Z, M> {
    hub: Arc<H>,
    authenticator: Arc<A>,
    authorizer: Arc<Z>,
    on_message: Arc<M>,
    config: RealtimeConfig,
    parts: Parts,
    identity: Identity,
    room: RoomId,
}

impl<H, A, Z, M> Connection<H, A, Z, M>
where
    H: RealtimeHub + 'static,
    A: ConnectionAuthenticator,
    Z: RoomAuthorizer,
    M: OnMessage,
{
    /// Pumps the socket until one side ends, a revalidation fails, or the
    /// heartbeat deadline passes.
    async fn run(mut self, socket: WebSocket) -> CloseReason {
        let (mut sink, mut stream) = socket.split();

        let subscription = match self.hub.subscribe(&self.room).await {
            Ok(subscription) => subscription,
            Err(error) => {
                tracing::warn!(error = %error, "failed to subscribe realtime connection to its room");
                let _ = sink.send(WsMessage::Close(None)).await;
                return CloseReason::SubscribeFailed;
            }
        };
        tokio::pin!(subscription);

        let mut revalidate = interval(self.config.revalidate_every);
        let mut heartbeat = interval(self.config.heartbeat_every);
        // The first tick of a tokio interval fires immediately; skip it so a
        // connection is not revalidated and pinged the instant it opens.
        revalidate.reset();
        heartbeat.reset();
        revalidate.set_missed_tick_behavior(MissedTickBehavior::Delay);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut last_seen = Instant::now();

        let reason = loop {
            tokio::select! {
                outbound = subscription.next() => {
                    let Some(message) = outbound else {
                        break CloseReason::RoomClosed;
                    };
                    if sink.send(WsMessage::Binary(message.payload.into())).await.is_err() {
                        break CloseReason::ClientGone;
                    }
                    metrics::counter!(
                        "pharos.realtime.messages.delivered",
                        "kind" => message.kind
                    )
                    .increment(1);
                }

                inbound = stream.next() => {
                    let Some(frame) = inbound else {
                        break CloseReason::ClientGone;
                    };
                    // Any frame — including a pong — proves the peer is alive.
                    last_seen = Instant::now();

                    let payload = match frame {
                        Ok(WsMessage::Binary(bytes)) => bytes,
                        Ok(WsMessage::Text(text)) => Bytes::copy_from_slice(text.as_bytes()),
                        // Ping/Pong are answered by axum automatically; a Close
                        // ends the loop cleanly, and a transport error is
                        // treated as an ordinary disconnect, never a panic.
                        Ok(WsMessage::Ping(_) | WsMessage::Pong(_)) => continue,
                        Ok(WsMessage::Close(_)) | Err(_) => break CloseReason::ClientGone,
                    };

                    // Publish permission is checked per frame, not per
                    // connection: a right revoked mid-session has to stop the
                    // next message, not the next reconnect. Read access to a
                    // room is not write access to it either.
                    if let Err(error) = self
                        .authorizer
                        .authorize(&self.identity, &self.room, Access::Publish)
                        .await
                    {
                        metrics::counter!(
                            "pharos.realtime.frames.rejected",
                            "access" => Access::Publish.as_str()
                        )
                        .increment(1);
                        tracing::warn!(error = %error, "inbound realtime frame not authorized");
                        continue;
                    }

                    if let Err(error) =
                        self.on_message.on_message(&self.identity, &self.room, payload).await
                    {
                        tracing::warn!(error = %error, "on_message rejected an inbound realtime frame");
                    }
                }

                _ = revalidate.tick() => {
                    if !self.still_authorized().await {
                        let _ = sink.send(WsMessage::Close(None)).await;
                        break CloseReason::Revoked;
                    }
                }

                _ = heartbeat.tick() => {
                    if last_seen.elapsed() >= self.config.heartbeat_timeout {
                        tracing::info!("realtime connection exceeded its heartbeat deadline");
                        let _ = sink.send(WsMessage::Close(None)).await;
                        break CloseReason::Timeout;
                    }
                    if sink.send(WsMessage::Ping(Bytes::new())).await.is_err() {
                        break CloseReason::ClientGone;
                    }
                }
            }
        };

        let _ = sink.close().await;
        reason
    }

    /// Re-runs authentication and the subscribe authorization.
    ///
    /// A changed identity is a rejection, not a silent hand-over: the socket
    /// was authorized for whoever opened it, and letting a different principal
    /// inherit an established connection is exactly the session-fixation shape
    /// this is meant to prevent.
    async fn still_authorized(&mut self) -> bool {
        let identity = match self.authenticator.authenticate(&self.parts).await {
            Ok(identity) => identity,
            Err(error) => {
                tracing::info!(error = %error, "realtime connection failed revalidation");
                metrics::counter!("pharos.realtime.revalidations.failed", "cause" => "unauthenticated")
                    .increment(1);
                return false;
            }
        };

        if identity != self.identity {
            tracing::warn!("realtime connection changed identity mid-session; closing");
            metrics::counter!("pharos.realtime.revalidations.failed", "cause" => "identity_changed")
                .increment(1);
            return false;
        }

        if let Err(error) = self
            .authorizer
            .authorize(&self.identity, &self.room, Access::Subscribe)
            .await
        {
            tracing::info!(error = %error, "realtime connection lost access to its room");
            metrics::counter!("pharos.realtime.revalidations.failed", "cause" => "forbidden")
                .increment(1);
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{RealtimeMessage, RealtimePublisher};
    use crate::memory::InMemoryHub;

    // The refusal paths of `upgrade()` end to end (unauthenticated, forbidden
    // room, revoked mid-connection) are covered by `tests/hub_flow.rs`, which
    // drives a real `axum::serve` server — a `WebSocketUpgrade` can only be
    // constructed from a request that went through an actual hyper connection
    // upgrade, so it is not reproducible from a bare unit test.

    #[test]
    fn errors_map_to_the_status_that_tells_the_client_what_to_do() {
        let cases = [
            (
                RealtimeError::Unauthorized("no token".into()),
                StatusCode::UNAUTHORIZED,
            ),
            (
                RealtimeError::Forbidden {
                    identity: "player-1".into(),
                    room: "game:2".into(),
                },
                StatusCode::FORBIDDEN,
            ),
            (
                RealtimeError::Capacity("too many rooms".into()),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.into_response().status(), expected);
        }
    }

    #[test]
    fn a_refusal_never_echoes_the_identity_or_room_back_to_the_caller() {
        let response = RealtimeError::Forbidden {
            identity: "player-1".into(),
            room: "game:secret".into(),
        }
        .into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        // The body is the canonical reason; the detail stays in the log.
        assert!(response.headers().get("content-type").is_some());
    }

    #[tokio::test]
    async fn hub_behind_arc_still_implements_realtime_hub() -> Result<(), Box<dyn std::error::Error>>
    {
        let hub: Arc<InMemoryHub> = Arc::new(InMemoryHub::new());
        let room = RoomId::parse("game:2")?;
        RealtimePublisher::publish(&hub, RealtimeMessage::new(room, "kind", vec![1, 2, 3])).await?;
        Ok(())
    }
}
