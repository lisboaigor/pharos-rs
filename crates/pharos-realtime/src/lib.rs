//! Real-time WebSocket / pub-sub primitives for Pharos RS.
//!
//! The framework's primitive for fan-out-to-many-subscribers delivery (a
//! live game, a chat room, a dashboard with many viewers) — a different
//! delivery contract from `pharos-messaging`'s point-to-point broker
//! consumption, which is why it lives in its own crate rather than extending
//! `pharos_messaging::messaging`.
//!
//! # Modules
//!
//! - [`hub`] — [`RoomId`], [`RealtimeMessage`], and the
//!   [`RealtimePublisher`]/[`RealtimeSubscriber`] traits every backend
//!   implements.
//! - [`memory`] — [`InMemoryHub`], the single-node MVP backend built on
//!   `tokio::sync::broadcast`.
//! - [`auth`] — [`ConnectionAuthenticator`] (who is this?) and
//!   [`RoomAuthorizer`] (may they touch *this* room?), the two seams an
//!   embedding app fills in.
//! - [`ws`] — generic axum WebSocket upgrade/pump glue: authenticate,
//!   authorize, join a room, then pump messages both ways while rechecking
//!   both and keeping the connection honest with a heartbeat.
//!
//! # Flow
//!
//! ```mermaid
//! flowchart TD
//!     Client[WS client]
//!     Upgrade[Realtime::upgrade]
//!     Auth[ConnectionAuthenticator]
//!     Authz[RoomAuthorizer]
//!     Hub[RealtimeHub]
//!     Out[outbound: hub stream to WS frames]
//!     In[inbound: WS frames to OnMessage]
//!     Recheck[revalidation tick + heartbeat]
//!
//!     Client --> Upgrade
//!     Upgrade --> Auth
//!     Auth -->|Identity| Authz
//!     Authz -->|Access::Subscribe| Hub
//!     Hub --> Out
//!     Out --> Client
//!     Client --> In
//!     In -->|Access::Publish per frame| Authz
//!     In -->|payload| App[embedding app]
//!     Recheck -->|revoked| Client
//! ```
//!
//! # Authorization is not optional
//!
//! [`ws::Realtime::new`] takes a [`RoomAuthorizer`] as a required argument.
//! A room id normally comes straight off the request path, so a
//! handshake-only check authorizes the socket and not the resource, and
//! `subscribe("tenant-b/orders")` succeeds for anyone who can log in. Writing
//! an authorizer that returns `Ok(())` is a decision a reviewer can see;
//! having nowhere to write one is not.
//!
//! A future NATS-backed hub (Fase 7+ per the plan) implements the same
//! [`RealtimePublisher`]/[`RealtimeSubscriber`] traits for horizontal
//! scale-out; nothing in [`ws`] or an embedding app would need to change.
//! [`RoomId`] is validated with that in mind — NATS reads `*` and `>` as
//! subject wildcards, so they never make it into a room id in the first
//! place.

pub mod auth;
pub mod hub;
pub mod memory;
pub mod ws;

pub use auth::{Access, ConnectionAuthenticator, Identity, RoomAuthorizer, forbidden};
pub use hub::{
    InvalidRoomId, RealtimeError, RealtimeHub, RealtimeMessage, RealtimePublisher,
    RealtimeSubscriber, RoomId,
};
pub use memory::{InMemoryHub, RoomStream};
pub use ws::{OnMessage, Realtime, RealtimeConfig};
