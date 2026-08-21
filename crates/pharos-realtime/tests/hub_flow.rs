//! Chess-agnostic integration tests for `pharos-realtime` itself.
//!
//! The in-process hub tests are the must-have: fan-out to every subscriber of
//! a room, isolation between rooms, and fire-and-forget semantics when nobody
//! is subscribed. On top of those, a real `axum::serve` server driven by
//! `tokio-tungstenite` clients proves the upgrade/pump glue actually delivers
//! a hub publish as a WebSocket frame — and, just as importantly, that it
//! *refuses* the connections it should: an unauthenticated client, a client
//! asking for someone else's room, and a client whose access is revoked while
//! its socket is already open.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::StreamExt;
use pharos_realtime::{
    Access, ConnectionAuthenticator, Identity, InMemoryHub, OnMessage, Realtime, RealtimeConfig,
    RealtimeError, RealtimeMessage, RealtimePublisher, RealtimeSubscriber, Reply, RoomAuthorizer,
    RoomId, forbidden,
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

fn msg(room: &RoomId, payload: &str) -> RealtimeMessage {
    RealtimeMessage::new(room.clone(), "test", payload.as_bytes().to_vec())
}

#[tokio::test]
async fn two_subscribers_on_the_same_room_both_receive_a_publish() -> TestResult {
    let hub = InMemoryHub::new();
    let room = room("game:same-room");

    let mut sub_a = hub.subscribe(&room).await?;
    let mut sub_b = hub.subscribe(&room).await?;

    hub.publish(msg(&room, "hello")).await?;

    let Some(received_a) = sub_a.next().await else {
        panic!("subscriber a should have received the message");
    };
    let Some(received_b) = sub_b.next().await else {
        panic!("subscriber b should have received the message");
    };
    assert_eq!(received_a.payload, b"hello");
    assert_eq!(received_b.payload, b"hello");
    Ok(())
}

#[tokio::test]
async fn a_subscriber_on_a_different_room_never_sees_a_publish_meant_for_another() -> TestResult {
    let hub = InMemoryHub::new();
    let target_room = room("game:target");
    let other_room = room("game:other");

    let mut on_other_room = hub.subscribe(&other_room).await?;

    hub.publish(msg(&target_room, "not for you")).await?;
    // A message on the subscriber's own room afterwards confirms the stream
    // is alive and observing messages in order — if the cross-room publish
    // had leaked through, it would have arrived first.
    hub.publish(msg(&other_room, "for you")).await?;

    let Some(received) = on_other_room.next().await else {
        panic!("subscriber should have received its own room's message");
    };
    assert_eq!(received.payload, b"for you");
    Ok(())
}

#[tokio::test]
async fn publishing_to_a_room_with_no_subscribers_is_not_an_error() -> TestResult {
    let hub = InMemoryHub::new();
    // Fire-and-forget: a fan-out hub has no ack/nack, so an empty room is a
    // normal outcome, not a failure to surface to the publisher.
    hub.publish(msg(&room("game:nobody-home"), "anyone?"))
        .await?;
    Ok(())
}

/// Resolves the identity from an `x-test-identity` header, refusing a request
/// that carries none — good enough for a harness, not a real auth mechanism.
struct HeaderAuthenticator;

impl ConnectionAuthenticator for HeaderAuthenticator {
    async fn authenticate(&self, parts: &Parts) -> Result<Identity, RealtimeError> {
        parts
            .headers
            .get("x-test-identity")
            .and_then(|value| value.to_str().ok())
            .map(Identity::new)
            .ok_or_else(|| RealtimeError::Unauthorized("no x-test-identity header".into()))
    }
}

/// Grants access only to the room named after the identity, and can have that
/// access revoked at runtime to model a logout or a permission change.
#[derive(Default)]
struct OwnRoomOnly {
    revoked: AtomicBool,
}

impl RoomAuthorizer for OwnRoomOnly {
    async fn authorize(
        &self,
        identity: &Identity,
        room: &RoomId,
        _access: Access,
    ) -> Result<(), RealtimeError> {
        if self.revoked.load(Ordering::SeqCst) {
            return Err(forbidden(identity, room));
        }
        if room.as_str() == format!("game:{identity}") {
            Ok(())
        } else {
            Err(forbidden(identity, room))
        }
    }
}

/// Records the frames it was handed, so a test can assert what actually
/// reached the application.
#[derive(Default)]
struct RecordInbound {
    seen: std::sync::Mutex<Vec<(String, String)>>,
}

impl OnMessage for RecordInbound {
    async fn on_message(
        &self,
        identity: &Identity,
        room: &RoomId,
        payload: Bytes,
    ) -> Result<Option<Reply>, RealtimeError> {
        let mut seen = self.seen.lock().unwrap_or_else(|p| p.into_inner());
        let body = String::from_utf8_lossy(&payload).into_owned();
        seen.push((format!("{identity}@{room}"), body.clone()));

        // A business rejection answers the sender instead of failing the
        // frame, which is exactly what `Reply` exists for.
        if body.starts_with("reject:") {
            return Ok(Some(Reply::new("rejected", b"nope".to_vec())));
        }
        Ok(None)
    }
}

type TestRealtime = Realtime<InMemoryHub, HeaderAuthenticator, OwnRoomOnly, RecordInbound>;

#[derive(Clone)]
struct TestAppState {
    realtime: TestRealtime,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    parts: Parts,
    Path(room): Path<String>,
    State(state): State<TestAppState>,
) -> Response {
    let room = match RoomId::parse(room) {
        Ok(room) => room,
        Err(error) => return RealtimeError::from(error).into_response(),
    };
    // The app decides where `since` travels; this harness reads a query
    // parameter, which is the shape most clients will use.
    let since = parts.uri.query().and_then(|query| {
        query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == "since").then(|| value.parse::<u64>().ok())?
        })
    });

    match state.realtime.upgrade_since(ws, &parts, room, since).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

struct Harness {
    base_url: String,
    hub: Arc<InMemoryHub>,
    authorizer: Arc<OwnRoomOnly>,
    inbound: Arc<RecordInbound>,
}

async fn spawn_test_server(
    config: RealtimeConfig,
) -> Result<Harness, Box<dyn std::error::Error + Send + Sync>> {
    let hub = Arc::new(InMemoryHub::new());
    let authorizer = Arc::new(OwnRoomOnly::default());
    let inbound = Arc::new(RecordInbound::default());

    let realtime = Realtime::new(
        Arc::clone(&hub),
        Arc::new(HeaderAuthenticator),
        Arc::clone(&authorizer),
        Arc::clone(&inbound),
    )
    .with_config(config);

    let app = Router::new()
        .route("/ws/{room}", get(ws_handler))
        .with_state(TestAppState { realtime });

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok(Harness {
        base_url: format!("ws://{addr}"),
        hub,
        authorizer,
        inbound,
    })
}

/// Builds a client request carrying an identity header.
fn request_as(
    url: &str,
    identity: &str,
) -> Result<
    tokio_tungstenite::tungstenite::handshake::client::Request,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mut request = url.into_client_request()?;
    request
        .headers_mut()
        .insert("x-test-identity", identity.parse()?);
    Ok(request)
}

#[tokio::test]
async fn a_real_websocket_client_receives_a_message_published_through_the_hub() -> TestResult {
    let harness = spawn_test_server(RealtimeConfig::default()).await?;
    let room = room("game:alice");

    let (mut client, _response) = tokio_tungstenite::connect_async(request_as(
        &format!("{}/ws/game:alice", harness.base_url),
        "alice",
    )?)
    .await?;

    // Give the server a moment to have called `hub.subscribe` before
    // publishing — otherwise the publish could race the subscription and
    // this fan-out hub (no replay buffer) would never deliver it.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let started = std::time::Instant::now();
    harness
        .hub
        .publish(RealtimeMessage::new(room, "test", b"e2e-hello".to_vec()))
        .await?;

    let frame = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await?
        .ok_or("connection closed before a frame arrived")??;

    assert!(
        started.elapsed() < Duration::from_millis(500),
        "propagation took {:?}, expected sub-second",
        started.elapsed()
    );
    match frame {
        ClientMessage::Binary(bytes) => assert_eq!(bytes.as_ref(), b"e2e-hello"),
        other => panic!("expected a binary frame, got {other:?}"),
    }

    client.close(None).await?;
    Ok(())
}

#[tokio::test]
async fn two_real_websocket_clients_on_the_same_room_both_receive_the_broadcast() -> TestResult {
    let harness = spawn_test_server(RealtimeConfig::default()).await?;
    let url = format!("{}/ws/game:alice", harness.base_url);

    let (mut client_a, _) = tokio_tungstenite::connect_async(request_as(&url, "alice")?).await?;
    let (mut client_b, _) = tokio_tungstenite::connect_async(request_as(&url, "alice")?).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    harness
        .hub
        .publish(RealtimeMessage::new(
            room("game:alice"),
            "test",
            b"move-made".to_vec(),
        ))
        .await?;

    let frame_a = tokio::time::timeout(Duration::from_secs(2), client_a.next())
        .await?
        .ok_or("client a: connection closed before a frame arrived")??;
    let frame_b = tokio::time::timeout(Duration::from_secs(2), client_b.next())
        .await?
        .ok_or("client b: connection closed before a frame arrived")??;

    for frame in [frame_a, frame_b] {
        match frame {
            ClientMessage::Binary(bytes) => assert_eq!(bytes.as_ref(), b"move-made"),
            other => panic!("expected a binary frame, got {other:?}"),
        }
    }

    client_a.close(None).await?;
    client_b.close(None).await?;
    Ok(())
}

#[tokio::test]
async fn an_unauthenticated_client_is_refused_before_the_upgrade() -> TestResult {
    let harness = spawn_test_server(RealtimeConfig::default()).await?;
    let url = format!("{}/ws/game:alice", harness.base_url);

    // No `x-test-identity` header at all.
    let result = tokio_tungstenite::connect_async(url.into_client_request()?).await;
    assert!(result.is_err(), "the handshake must not have completed");
    Ok(())
}

/// The IDOR this crate exists to prevent: a perfectly authenticated client
/// asking for a room that belongs to someone else.
#[tokio::test]
async fn an_authenticated_client_cannot_subscribe_to_another_principals_room() -> TestResult {
    let harness = spawn_test_server(RealtimeConfig::default()).await?;

    let result = tokio_tungstenite::connect_async(request_as(
        &format!("{}/ws/game:bob", harness.base_url),
        "alice",
    )?)
    .await;

    assert!(
        result.is_err(),
        "alice must not be able to open bob's room, authenticated or not"
    );

    // And the refusal happened before the hub ever created the room.
    assert_eq!(harness.hub.room_count(), 0);
    Ok(())
}

#[tokio::test]
async fn a_room_id_the_hub_would_reject_never_reaches_the_hub() -> TestResult {
    let harness = spawn_test_server(RealtimeConfig::default()).await?;

    // `*` is a NATS subject wildcard and a legal raw URI path character (a
    // sub-delim), so it reaches the server unencoded and must be rejected by
    // `RoomId::parse` rather than by the HTTP client's own URI validation.
    let result = tokio_tungstenite::connect_async(request_as(
        &format!("{}/ws/game:*", harness.base_url),
        "alice",
    )?)
    .await;

    assert!(result.is_err(), "a wildcard room id must be refused");
    assert_eq!(harness.hub.room_count(), 0);
    Ok(())
}

/// An established connection must not outlive the permission that opened it.
#[tokio::test]
async fn revoking_access_closes_an_already_open_connection() -> TestResult {
    let harness = spawn_test_server(RealtimeConfig {
        revalidate_every: Duration::from_millis(100),
        heartbeat_every: Duration::from_millis(500),
        heartbeat_timeout: Duration::from_secs(30),
        ..RealtimeConfig::default()
    })
    .await?;

    let (mut client, _) = tokio_tungstenite::connect_async(request_as(
        &format!("{}/ws/game:alice", harness.base_url),
        "alice",
    )?)
    .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        harness.hub.room_count(),
        1,
        "the connection joined its room"
    );

    // Alice logs out / loses permission while her socket is open.
    harness.authorizer.revoked.store(true, Ordering::SeqCst);

    // The next revalidation tick must close the socket without the client
    // having to do anything.
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = client.next().await {
            match frame {
                Ok(ClientMessage::Close(_)) | Err(_) => return true,
                _ => continue,
            }
        }
        true
    })
    .await?;

    assert!(
        closed,
        "the connection should have been closed on revocation"
    );
    Ok(())
}

/// A room's channel must be released when its last connection goes, even
/// though nothing was ever published to it.
#[tokio::test]
async fn a_disconnect_releases_the_room_even_with_no_publish() -> TestResult {
    let harness = spawn_test_server(RealtimeConfig::default()).await?;

    let (mut client, _) = tokio_tungstenite::connect_async(request_as(
        &format!("{}/ws/game:alice", harness.base_url),
        "alice",
    )?)
    .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(harness.hub.room_count(), 1);

    client.close(None).await?;

    let released = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if harness.hub.room_count() == 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;

    assert!(
        released,
        "the room should have been collected on disconnect"
    );
    Ok(())
}

#[tokio::test]
async fn an_inbound_frame_reaches_the_app_with_its_identity_and_room() -> TestResult {
    let harness = spawn_test_server(RealtimeConfig::default()).await?;

    let (mut client, _) = tokio_tungstenite::connect_async(request_as(
        &format!("{}/ws/game:alice", harness.base_url),
        "alice",
    )?)
    .await?;

    use futures::SinkExt;
    client
        .send(ClientMessage::Binary(b"e2:e4".to_vec().into()))
        .await?;

    let delivered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            {
                let seen = harness
                    .inbound
                    .seen
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                if let Some(first) = seen.first() {
                    return first.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;

    assert_eq!(
        delivered.0, "alice@game:alice",
        "identity and room both arrive"
    );
    assert_eq!(delivered.1, "e2:e4");

    client.close(None).await?;
    Ok(())
}

#[tokio::test]
async fn a_reconnecting_client_is_handed_what_it_missed_before_anything_live() -> TestResult {
    let harness = spawn_test_server(RealtimeConfig::default()).await?;
    let room = room("game:alice");
    let url = format!("{}/ws/game:alice", harness.base_url);

    // Two connections, as a two-player room really has: the second keeps the
    // room — and therefore its backlog — alive while the first is away.
    let (mut first, _) = tokio_tungstenite::connect_async(request_as(&url, "alice")?).await?;
    let (mut opponent, _) = tokio_tungstenite::connect_async(request_as(&url, "alice")?).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    for payload in [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
        harness
            .hub
            .publish(RealtimeMessage::new(room.clone(), "test", payload))
            .await?;
    }

    // It read the first message and then its socket died.
    let _ = tokio::time::timeout(Duration::from_secs(2), first.next()).await?;
    first.close(None).await?;

    // It comes back saying "I processed version 1".
    let (mut resumed, _) =
        tokio_tungstenite::connect_async(request_as(&format!("{url}?since=1"), "alice")?).await?;

    let mut replayed = Vec::new();
    for _ in 0..2 {
        let frame = tokio::time::timeout(Duration::from_secs(2), resumed.next())
            .await?
            .ok_or("connection closed before the backlog arrived")??;
        match frame {
            ClientMessage::Binary(bytes) => replayed.push(bytes.to_vec()),
            other => panic!("expected a binary frame, got {other:?}"),
        }
    }

    assert_eq!(
        replayed,
        vec![b"two".to_vec(), b"three".to_vec()],
        "the client must receive exactly the gap, in order, and nothing it already saw"
    );

    // And the live stream continues on top of the replay.
    harness
        .hub
        .publish(RealtimeMessage::new(room, "test", b"four".to_vec()))
        .await?;
    let frame = tokio::time::timeout(Duration::from_secs(2), resumed.next())
        .await?
        .ok_or("connection closed before the live message arrived")??;
    match frame {
        ClientMessage::Binary(bytes) => assert_eq!(bytes.as_ref(), b"four"),
        other => panic!("expected a binary frame, got {other:?}"),
    }

    resumed.close(None).await?;
    opponent.close(None).await?;
    Ok(())
}

#[tokio::test]
async fn a_business_rejection_answers_the_sender_instead_of_vanishing() -> TestResult {
    use futures::SinkExt as _;

    let harness = spawn_test_server(RealtimeConfig::default()).await?;
    let url = format!("{}/ws/game:alice", harness.base_url);

    let (mut client, _) = tokio_tungstenite::connect_async(request_as(&url, "alice")?).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client
        .send(ClientMessage::Binary(b"reject:bad-move".to_vec().into()))
        .await?;

    let frame = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await?
        .ok_or("the rejection must come back on the same socket")??;
    match frame {
        ClientMessage::Binary(bytes) => assert_eq!(bytes.as_ref(), b"nope"),
        other => panic!("expected the rejection frame, got {other:?}"),
    }

    // The connection stays usable: a refused command is not a broken socket.
    harness
        .hub
        .publish(RealtimeMessage::new(
            room("game:alice"),
            "test",
            b"still-here".to_vec(),
        ))
        .await?;
    let frame = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await?
        .ok_or("connection closed after a business rejection")??;
    match frame {
        ClientMessage::Binary(bytes) => assert_eq!(bytes.as_ref(), b"still-here"),
        other => panic!("expected a binary frame, got {other:?}"),
    }

    client.close(None).await?;
    Ok(())
}
