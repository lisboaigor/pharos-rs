//! Stress / abuse tests against `pharos-realtime`'s WebSocket pump.
//!
//! Each test models one thing a hostile-but-authenticated client (or a
//! stalled, non-hostile one) can do to a server that embeds
//! `Realtime::upgrade`, and is a regression test for a fix that landed
//! because of it.
//!
//! See `memory_bounds.rs` for the two tests measuring what the default hub
//! limits cost in bytes — kept in a separate test binary since they read
//! process-wide RSS and would otherwise share a process with the
//! multi-hundred-megabyte allocations the tests here make.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::SinkExt;
use pharos_realtime::{
    Access, ConnectionAuthenticator, Identity, InMemoryHub, OnMessage, Realtime, RealtimeConfig,
    RealtimeError, RealtimeMessage, RealtimePublisher, Reply, RoomAuthorizer, RoomId, forbidden,
};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as ClientMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn room(id: &str) -> RoomId {
    let Ok(room) = RoomId::parse(id) else {
        panic!("`{id}` should be a valid room id");
    };
    room
}

struct HeaderAuthenticator {
    calls: AtomicU64,
}

impl ConnectionAuthenticator for HeaderAuthenticator {
    async fn authenticate(&self, parts: &Parts) -> Result<Identity, RealtimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        parts
            .headers
            .get("x-test-identity")
            .and_then(|value| value.to_str().ok())
            .map(Identity::new)
            .ok_or_else(|| RealtimeError::Unauthorized("no x-test-identity header".into()))
    }
}

/// Counts every authorization decision and can be revoked at runtime.
#[derive(Default)]
struct CountingAuthorizer {
    revoked: AtomicBool,
    subscribe_calls: AtomicU64,
    publish_calls: AtomicU64,
}

impl RoomAuthorizer for CountingAuthorizer {
    async fn authorize(
        &self,
        identity: &Identity,
        room: &RoomId,
        access: Access,
    ) -> Result<(), RealtimeError> {
        match access {
            Access::Subscribe => self.subscribe_calls.fetch_add(1, Ordering::SeqCst),
            Access::Publish => self.publish_calls.fetch_add(1, Ordering::SeqCst),
        };
        if self.revoked.load(Ordering::SeqCst) {
            return Err(forbidden(identity, room));
        }
        Ok(())
    }
}

/// An `OnMessage` that counts frames and can be told to block for a while,
/// standing in for a handler that dispatches a command into a slow database.
struct SlowHandler {
    frames: AtomicU64,
    delay: Duration,
}

impl OnMessage for SlowHandler {
    async fn on_message(
        &self,
        _identity: &Identity,
        _room: &RoomId,
        _payload: Bytes,
    ) -> Result<Option<Reply>, RealtimeError> {
        self.frames.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(None)
    }
}

type StressRealtime = Realtime<InMemoryHub, HeaderAuthenticator, CountingAuthorizer, SlowHandler>;

#[derive(Clone)]
struct AppState {
    realtime: StressRealtime,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    parts: Parts,
    Path(room_id): Path<String>,
    State(state): State<AppState>,
) -> Response {
    let room = match RoomId::parse(room_id) {
        Ok(room) => room,
        Err(error) => return RealtimeError::from(error).into_response(),
    };
    match state.realtime.upgrade(ws, &parts, room).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

struct Harness {
    base_url: String,
    hub: Arc<InMemoryHub>,
    authorizer: Arc<CountingAuthorizer>,
    handler: Arc<SlowHandler>,
    authenticator: Arc<HeaderAuthenticator>,
}

async fn start(
    config: RealtimeConfig,
    handler_delay: Duration,
) -> Result<Harness, Box<dyn std::error::Error + Send + Sync>> {
    let hub = Arc::new(InMemoryHub::new());
    let authenticator = Arc::new(HeaderAuthenticator {
        calls: AtomicU64::new(0),
    });
    let authorizer = Arc::new(CountingAuthorizer::default());
    let handler = Arc::new(SlowHandler {
        frames: AtomicU64::new(0),
        delay: handler_delay,
    });

    let realtime = Realtime::new(
        Arc::clone(&hub),
        Arc::clone(&authenticator),
        Arc::clone(&authorizer),
        Arc::clone(&handler),
    )
    .with_config(config);

    let app = Router::new()
        .route("/ws/{room}", get(ws_handler))
        .with_state(AppState { realtime });

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok(Harness {
        base_url: format!("ws://{addr}"),
        hub,
        authorizer,
        handler,
        authenticator,
    })
}

async fn connect(
    harness: &Harness,
    identity: &str,
    room_id: &str,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let url = format!("{}/ws/{room_id}", harness.base_url);
    let mut request = url.into_client_request()?;
    request
        .headers_mut()
        .insert("x-test-identity", identity.parse()?);
    let (socket, _) = tokio_tungstenite::connect_async(request).await?;
    Ok(socket)
}

/// Waits for `predicate` to hold, up to `timeout`. Returns whether it did.
async fn eventually(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    predicate()
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. A client that stops reading must still be reaped (io_timeout).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_stops_reading_is_reaped_within_io_timeout() -> TestResult {
    let config = RealtimeConfig {
        max_message_size: 1024 * 1024,
        max_frame_size: 1024 * 1024,
        revalidate_every: Duration::from_millis(100),
        heartbeat_every: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_millis(300),
        io_timeout: Duration::from_millis(150),
        ..RealtimeConfig::default()
    };
    let harness = start(config, Duration::ZERO).await?;

    let target = room("game:stall");
    // The client completes the handshake and then never reads another byte.
    let _socket = connect(&harness, "player-1", "game:stall").await?;
    assert!(
        eventually(Duration::from_secs(2), || harness.hub.room_count() == 1).await,
        "the connection should have joined its room"
    );

    // Fill the socket's send path: 512 KiB per message until the server's
    // write blocks. Nothing here is bigger than what an ordinary app fans out.
    let payload = vec![0u8; 512 * 1024];
    for _ in 0..256 {
        harness
            .hub
            .publish(RealtimeMessage::new(
                target.clone(),
                "bulk",
                payload.clone(),
            ))
            .await?;
    }

    // Revoke every permission this identity has. Both liveness mechanisms —
    // the heartbeat deadline and the revalidation tick — should now close the
    // socket well inside a second.
    harness.authorizer.revoked.store(true, Ordering::SeqCst);

    let reaped = eventually(Duration::from_secs(5), || harness.hub.room_count() == 0).await;

    let subscribe_calls = harness.authorizer.subscribe_calls.load(Ordering::SeqCst);
    println!(
        "room_count={} subscribe_authorizations={} (1 = handshake only, so no revalidation ran)",
        harness.hub.room_count(),
        subscribe_calls
    );

    assert!(
        reaped,
        "a client that stops reading must still be reaped within io_timeout + one heartbeat \
         tick, even with its access already revoked"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. A slow `on_message` must not suspend revalidation for its whole duration.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_on_message_does_not_suspend_revalidation_past_io_timeout() -> TestResult {
    let config = RealtimeConfig {
        revalidate_every: Duration::from_millis(100),
        heartbeat_every: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_millis(300),
        io_timeout: Duration::from_millis(150),
        ..RealtimeConfig::default()
    };
    // Every inbound frame parks the connection task for 3 seconds.
    let harness = start(config, Duration::from_secs(3)).await?;

    let mut socket = connect(&harness, "player-2", "game:slow").await?;
    assert!(
        eventually(Duration::from_secs(2), || harness.hub.room_count() == 1).await,
        "the connection should have joined its room"
    );

    socket
        .send(ClientMessage::Binary(vec![1, 2, 3].into()))
        .await?;
    assert!(
        eventually(Duration::from_secs(2), || harness
            .handler
            .frames
            .load(Ordering::SeqCst)
            >= 1)
        .await,
        "the handler should have been entered"
    );

    // The principal is banned while its handler is still running.
    harness.authorizer.revoked.store(true, Ordering::SeqCst);

    let closed_within_a_tick =
        eventually(Duration::from_millis(800), || harness.hub.room_count() == 0).await;

    assert!(
        closed_within_a_tick,
        "a slow on_message must not suspend revalidation past io_timeout"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Inbound frames are rate-limited: max_inbound_frames_per_second holds.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_connection_cannot_exceed_the_inbound_frame_rate_limit() -> TestResult {
    let harness = start(RealtimeConfig::default(), Duration::ZERO).await?;
    let mut socket = connect(&harness, "player-3", "game:flood").await?;

    let started = Instant::now();
    let budget = Duration::from_secs(2);
    let mut sent = 0u64;
    // 1 KiB frames, as fast as the socket accepts them.
    let frame = vec![7u8; 1024];
    while started.elapsed() < budget {
        for _ in 0..500 {
            socket
                .send(ClientMessage::Binary(frame.clone().into()))
                .await?;
            sent += 1;
        }
    }
    socket.flush().await?;
    // Let the server drain what is already buffered.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let handled = harness.handler.frames.load(Ordering::SeqCst);
    let publish_authorizations = harness.authorizer.publish_calls.load(Ordering::SeqCst);
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "sent={sent} handled={handled} publish_authorizations={publish_authorizations} \
         in {elapsed:.2}s => {:.0} frames/s, {:.0} authorize() calls/s from ONE connection",
        handled as f64 / elapsed,
        publish_authorizations as f64 / elapsed
    );

    assert_eq!(
        publish_authorizations, handled,
        "every accepted frame runs exactly one authorize()"
    );
    assert!(
        handled < 1000,
        "{handled} frames were accepted from a single connection in {elapsed:.1}s — the \
         max_inbound_frames_per_second limiter must keep this well under what an unlimited \
         connection could push"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Connection churn must never leak a room.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn connection_churn_leaves_no_rooms_behind() -> TestResult {
    let harness = start(RealtimeConfig::default(), Duration::ZERO).await?;

    for batch in 0..20 {
        let mut sockets = Vec::new();
        for i in 0..50 {
            sockets.push(connect(&harness, "player-4", &format!("game:churn-{batch}-{i}")).await?);
        }
        drop(sockets);
    }

    let settled = eventually(Duration::from_secs(5), || harness.hub.room_count() == 0).await;
    println!(
        "rooms_after_1000_connect_disconnect_cycles={} authenticate_calls={}",
        harness.hub.room_count(),
        harness.authenticator.calls.load(Ordering::SeqCst)
    );
    assert!(
        settled,
        "rooms leaked after connection churn: {} still tracked",
        harness.hub.room_count()
    );
    Ok(())
}
