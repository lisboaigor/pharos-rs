//! [`InMemoryHub`] — the single-node MVP backend for [`super::hub`]'s
//! `RealtimePublisher`/`RealtimeSubscriber` traits.
//!
//! One `tokio::sync::broadcast` channel per room, held in a `DashMap` so
//! publishing and subscribing from many connections never contend on a single
//! lock.
//!
//! # Room lifetime
//!
//! A room's channel is created on first use and removed once its last
//! subscriber goes away — driven by the subscription's own `Drop`, not by the
//! next publish. That distinction is the whole point: collecting rooms only on
//! publish means a room nobody ever publishes to is never collected at all, so
//! `subscribe` to a fresh room and disconnect, repeated, leaks a channel every
//! time. `broadcast::channel(capacity)` preallocates its whole ring up front
//! (~24 KB at the default capacity), so that leak is measured in gigabytes
//! after a few tens of thousands of cheap handshakes, and the room id usually
//! comes straight off the request path.
//!
//! [`InMemoryHub::with_limits`] additionally caps how many rooms may exist at
//! once, so even a bug in the drop path cannot grow without bound.
//!
//! Horizontal scale-out (a NATS-backed hub sharing state across nodes) is
//! explicitly Fase 7+ in the plan — this backend does not attempt it.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures::Stream;
use futures::stream::BoxStream;
use tokio::sync::broadcast;

use crate::hub::{RealtimeError, RealtimeMessage, RealtimePublisher, RealtimeSubscriber, RoomId};

/// Per-room channel capacity: how many not-yet-delivered messages a lagging
/// subscriber can fall behind by before it starts missing messages.
const DEFAULT_CHANNEL_CAPACITY: usize = 256;

/// Default ceiling on concurrently tracked rooms.
///
/// Sized so the hub's rooms cannot exceed roughly a few hundred megabytes at
/// the default channel capacity, which is a resource-exhaustion bound rather
/// than a product limit — raise it deliberately with
/// [`InMemoryHub::with_limits`] if an app genuinely needs more.
const DEFAULT_MAX_ROOMS: usize = 10_000;

type Rooms = DashMap<RoomId, RoomEntry>;

/// A room's sender plus the number of live subscriptions holding it open.
///
/// The count is tracked here rather than read from
/// `broadcast::Sender::receiver_count` because a receiver only exists after
/// `subscribe()` returns: a publisher running between "look up the sender" and
/// "create the receiver" would see zero receivers, collect the room, and leave
/// the arriving subscriber attached to a sender no longer in the map — its
/// stream would then close immediately or silently never receive anything.
/// Incrementing before the map lock is released closes that window.
struct RoomEntry {
    sender: broadcast::Sender<RealtimeMessage>,
    subscribers: usize,
}

/// Single-node, in-process fan-out hub backed by one `broadcast` channel per
/// room.
#[derive(Clone)]
pub struct InMemoryHub {
    rooms: Arc<Rooms>,
    capacity: usize,
    max_rooms: usize,
}

impl Default for InMemoryHub {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryHub {
    /// Creates an empty hub with the default per-room channel capacity and
    /// room ceiling.
    pub fn new() -> Self {
        Self::with_limits(DEFAULT_CHANNEL_CAPACITY, DEFAULT_MAX_ROOMS)
    }

    /// Creates an empty hub with an explicit per-room channel capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_limits(capacity, DEFAULT_MAX_ROOMS)
    }

    /// Creates an empty hub with an explicit channel capacity and room ceiling.
    ///
    /// Both are clamped to at least 1.
    pub fn with_limits(capacity: usize, max_rooms: usize) -> Self {
        Self {
            rooms: Arc::default(),
            capacity: capacity.max(1),
            max_rooms: max_rooms.max(1),
        }
    }

    /// The number of rooms currently tracked.
    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    /// Returns the room's sender for publishing, without registering a
    /// subscriber. Returns `None` when the room has no subscribers, which for
    /// fire-and-forget fan-out is success with nobody listening — creating a
    /// channel here would let publishes to unread rooms fill the map.
    fn sender_for_publish(&self, room: &RoomId) -> Option<broadcast::Sender<RealtimeMessage>> {
        self.rooms.get(room).map(|entry| entry.sender.clone())
    }

    /// Registers a subscriber on `room`, creating the channel if needed, and
    /// returns a receiver plus the guard that will release the room.
    fn register_subscriber(
        &self,
        room: &RoomId,
    ) -> Result<(broadcast::Receiver<RealtimeMessage>, RoomGuard), RealtimeError> {
        use dashmap::mapref::entry::Entry;

        // Checked *before* taking the shard lock via `entry()` below.
        // `DashMap::entry` holds a write lock on the key's shard for as long
        // as the returned `Entry` lives, and `DashMap::len()` needs a read
        // lock on every shard, including that one — calling it from inside
        // an `entry()` match arm is a same-thread, self-deadlock, not a slow
        // path (dashmap's own docs call this out: "may deadlock if called
        // when holding a mutable reference into the map"). That is
        // deliberately not a false economy fixed by "just take a snapshot
        // first, then enter": the check below and the insert inside `entry()`
        // are two separate locks, so two concurrent subscribes to two
        // different brand-new rooms can both pass it. That is fine for a
        // resource-exhaustion ceiling — the bound only needs to prevent
        // unbounded growth, not land on an exact count — but it is why this
        // is not phrased as a hard guarantee.
        if !self.rooms.contains_key(room) && self.rooms.len() >= self.max_rooms {
            metrics::counter!("pharos.realtime.rooms.rejected").increment(1);
            return Err(RealtimeError::Capacity(format!(
                "hub is already tracking {} rooms",
                self.max_rooms
            )));
        }

        let receiver = match self.rooms.entry(room.clone()) {
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                entry.subscribers += 1;
                entry.sender.subscribe()
            }
            Entry::Vacant(vacant) => {
                let (sender, receiver) = broadcast::channel(self.capacity);
                vacant.insert(RoomEntry {
                    sender,
                    subscribers: 1,
                });
                receiver
            }
        };

        // The `Entry` above is fully out of scope by this point — its shard
        // lock released — so `guard()`'s own `len()` call is safe.
        Ok((receiver, self.guard(room)))
    }

    fn guard(&self, room: &RoomId) -> RoomGuard {
        metrics::gauge!("pharos.realtime.rooms.active").set(self.rooms.len() as f64);
        RoomGuard {
            rooms: Arc::clone(&self.rooms),
            room: room.clone(),
        }
    }
}

/// Releases one subscriber's hold on a room, removing the room once the last
/// one is gone. Dropping the subscription is what collects the room, so a room
/// that is never published to is collected just the same.
struct RoomGuard {
    rooms: Arc<Rooms>,
    room: RoomId,
}

impl Drop for RoomGuard {
    fn drop(&mut self) {
        // `remove_if` re-checks under the shard lock, so a subscriber arriving
        // concurrently cannot have the room removed out from under it.
        let mut collected = false;
        if let Some(mut entry) = self.rooms.get_mut(&self.room) {
            entry.subscribers = entry.subscribers.saturating_sub(1);
            collected = entry.subscribers == 0;
        }
        if collected {
            self.rooms.remove_if(&self.room, |_, e| e.subscribers == 0);
        }
        metrics::gauge!("pharos.realtime.rooms.active").set(self.rooms.len() as f64);
    }
}

/// Stream of a room's messages that releases the room when dropped.
///
/// Wraps a [`futures::stream::unfold`] rather than polling
/// `broadcast::Receiver::recv()` directly: tokio's `Recv` future unlinks
/// itself from the channel's waiter list when dropped, so a `poll_next` that
/// creates and drops a fresh `recv()` future on every call throws its own
/// waker registration away the instant it returns `Pending` — the task then
/// only gets polled again by some unrelated wakeup (a heartbeat tick, say),
/// not by the message that was just published. `unfold` avoids this by
/// keeping the same in-flight future alive, polled in place, until it
/// resolves.
pub struct RoomStream {
    inner: BoxStream<'static, RealtimeMessage>,
}

impl RoomStream {
    fn new(receiver: broadcast::Receiver<RealtimeMessage>, guard: RoomGuard) -> Self {
        let inner = futures::stream::unfold(
            (receiver, guard),
            |(mut receiver, guard)| async move {
                loop {
                    match receiver.recv().await {
                        Ok(message) => return Some((message, (receiver, guard))),
                        // A slow subscriber fell behind the channel capacity.
                        // There is no ack/nack contract to preserve, so delivery
                        // continues from the current head — but the gap is
                        // reported rather than swallowed: silently dropping a
                        // move in a live game is the kind of loss that must be
                        // visible to whoever operates this.
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            metrics::counter!("pharos.realtime.messages.lagged").increment(skipped);
                            tracing::warn!(
                                skipped,
                                "realtime subscriber fell behind; skipped messages will not be redelivered"
                            );
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                    }
                }
            },
        );
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for RoomStream {
    type Item = RealtimeMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RealtimePublisher for InMemoryHub {
    async fn publish(&self, msg: RealtimeMessage) -> Result<(), RealtimeError> {
        // `kind` is a `&'static str`, so it is bounded and safe as a label.
        // The room is not: it comes from the request and would give the
        // metrics backend one time series per room an attacker can invent.
        metrics::counter!("pharos.realtime.messages.published", "kind" => msg.kind).increment(1);

        if let Some(sender) = self.sender_for_publish(&msg.room) {
            // `send` errors only when every receiver dropped between the
            // lookup and here — fire-and-forget fan-out treats that as
            // success, not a failure to report upward.
            let _ = sender.send(msg);
        }
        Ok(())
    }
}

impl RealtimeSubscriber for InMemoryHub {
    type Stream = RoomStream;

    async fn subscribe(&self, room: &RoomId) -> Result<Self::Stream, RealtimeError> {
        let (receiver, guard) = self.register_subscriber(room)?;
        Ok(RoomStream::new(receiver, guard))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

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
    async fn two_subscribers_on_the_same_room_both_receive_a_publish()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = InMemoryHub::new();
        let room = room("game:1");

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
    async fn a_subscriber_on_a_different_room_does_not_receive_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = InMemoryHub::new();
        let room_a = room("game:a");
        let room_b = room("game:b");

        let mut sub_b = hub.subscribe(&room_b).await?;

        hub.publish(msg(&room_a, "for-a-only")).await?;
        // room_b's own publish confirms the subscriber's stream is alive and
        // ordered — if the earlier publish had leaked across rooms, this
        // message would not be the first one observed.
        hub.publish(msg(&room_b, "for-b")).await?;

        let Some(received) = sub_b.next().await else {
            panic!("subscriber b should have received its own room's message");
        };
        assert_eq!(received.payload, b"for-b");
        Ok(())
    }

    #[tokio::test]
    async fn publishing_with_no_subscribers_does_not_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = InMemoryHub::new();
        let room = room("game:empty");

        hub.publish(msg(&room, "nobody is listening")).await?;
        assert_eq!(
            hub.room_count(),
            0,
            "a publish to an unsubscribed room must not create one"
        );
        Ok(())
    }

    /// The leak this guards against: collecting rooms only on publish means a
    /// room nobody publishes to is never collected, so subscribe-then-drop in
    /// a loop grows the map forever — measured at roughly 24 KB per
    /// iteration, because the broadcast ring is preallocated.
    #[tokio::test]
    async fn a_room_is_collected_when_its_last_subscriber_drops_even_without_a_publish()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = InMemoryHub::new();

        for i in 0..1_000 {
            let sub = hub.subscribe(&room(&format!("attacker:{i}"))).await?;
            drop(sub);
        }

        assert_eq!(
            hub.room_count(),
            0,
            "every room must have been released by its subscription's drop"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_room_survives_until_its_last_subscriber_drops()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = InMemoryHub::new();
        let room = room("game:gc");

        let first = hub.subscribe(&room).await?;
        let second = hub.subscribe(&room).await?;
        assert_eq!(hub.room_count(), 1);

        drop(first);
        assert_eq!(hub.room_count(), 1, "one subscriber still holds the room");

        drop(second);
        assert_eq!(hub.room_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn subscribing_past_the_room_ceiling_is_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = InMemoryHub::with_limits(8, 2);

        let _a = hub.subscribe(&room("room:a")).await?;
        let _b = hub.subscribe(&room("room:b")).await?;

        let result = hub.subscribe(&room("room:c")).await;
        assert!(
            matches!(result, Err(RealtimeError::Capacity(_))),
            "a third room must be refused, not silently allocated"
        );

        // An existing room still accepts more subscribers at the ceiling.
        let _a2 = hub.subscribe(&room("room:a")).await?;
        Ok(())
    }

    /// A publish racing a subscribe must not be able to collect the room out
    /// from under the arriving subscriber.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_concurrent_publish_never_deafens_an_arriving_subscriber()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::time::Duration;

        for attempt in 0..200 {
            let hub = Arc::new(InMemoryHub::new());
            let room = room(&format!("race:{attempt}"));

            let publisher_hub = Arc::clone(&hub);
            let publisher_room = room.clone();
            let publisher = tokio::spawn(async move {
                for n in 0..50u8 {
                    let _ = publisher_hub
                        .publish(RealtimeMessage::new(publisher_room.clone(), "k", vec![n]))
                        .await;
                    tokio::task::yield_now().await;
                }
            });

            let mut stream = hub.subscribe(&room).await?;
            // The racing publisher above may have drained all 50 of its
            // messages before `subscribe` even returned — in which case a
            // correctly-attached subscriber legitimately has nothing waiting,
            // and the old "wait for any message" assertion timed out spuriously
            // (the flake). Publish one probe *after* we hold the subscription:
            // it must reach us unless the concurrent publish collected the room
            // out from under the arriving subscriber, which is the actual
            // regression under test.
            hub.publish(RealtimeMessage::new(room.clone(), "probe", vec![255]))
                .await?;
            let received = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
            assert!(
                matches!(received, Ok(Some(_))),
                "attempt {attempt}: the subscriber was cut off by a concurrent publish"
            );
            let _ = publisher.await;
        }
        Ok(())
    }
}
