//! What the default hub limits actually cost in bytes.
//!
//! Two separate per-room costs scale with payload size: the retained
//! backlog (bounded in bytes, independent of payload size, by
//! `with_max_backlog_bytes`) and the live `broadcast` channel's `capacity`
//! (which cannot be bounded in bytes without wrapping the channel itself —
//! each of its slots holds a full message until a subscriber consumes it).
//!
//! Kept in its own test binary, separate from `ws_stress.rs`: the second
//! test here allocates several hundred megabytes on purpose, and
//! `ws_stress.rs`'s WebSocket-pump tests allocate their own — sharing a
//! process with them would make this file's own memory shape harder to
//! reason about.

use std::sync::Arc;

use pharos_realtime::{Backlog, InMemoryHub, RealtimeMessage, RealtimePublisher};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn room(id: &str) -> pharos_realtime::RoomId {
    let Ok(room) = pharos_realtime::RoomId::parse(id) else {
        panic!("`{id}` should be a valid room id");
    };
    room
}

/// The retained backlog's byte budget is enforced exactly (see
/// `stamp_and_retain` in `memory.rs`: it evicts down to `max_backlog_bytes`
/// on every publish, no fudge factor), so this asks the hub itself what it
/// actually retained rather than inferring it from process-wide RSS — a
/// measurement an OS allocator's own bookkeeping, thread scheduling, and
/// page reuse make too noisy to gate a test on at this granularity.
#[tokio::test]
async fn the_retained_backlogs_bytes_never_exceed_max_backlog_bytes() -> TestResult {
    use pharos_realtime::RealtimeSubscriber;

    const PAYLOAD: usize = 32 * 1024;
    const MESSAGES: usize = 256;

    let hub = InMemoryHub::new();
    let target = room("game:budget");

    // A live subscription keeps the room — and its backlog — from being
    // collected between publishes.
    let _keepalive = hub.subscribe(&target).await?;

    for _ in 0..MESSAGES {
        hub.publish(RealtimeMessage::new(
            target.clone(),
            "bulk",
            vec![0u8; PAYLOAD],
        ))
        .await?;
    }

    // The byte budget evicts almost everything published, so asking for
    // "since the very start" correctly reports `Gap` rather than `Replayed`
    // — that gap tells us the oldest version still retained, exactly what a
    // real reconnecting client would use to re-ask for precisely the window
    // that *is* still there.
    let (backlog, _stream) = hub.subscribe_since(&target, Some(0)).await?;
    let Backlog::Gap { oldest_retained } = backlog else {
        panic!(
            "publishing {MESSAGES} oversized messages against a {}-byte budget should evict \
             all but a couple, producing a Gap when asked for the full history: got {backlog:?}",
            hub.max_backlog_bytes()
        );
    };

    let (backlog, _stream) = hub
        .subscribe_since(&target, Some(oldest_retained.saturating_sub(1)))
        .await?;
    let Backlog::Replayed(messages) = backlog else {
        panic!("asking for exactly the retained window must return Replayed, got {backlog:?}");
    };
    let retained_bytes: usize = messages.iter().map(|message| message.payload.len()).sum();

    println!(
        "published {MESSAGES} messages of {PAYLOAD}B each ({:.1} MiB total); the retained \
         backlog holds {} messages / {retained_bytes} bytes against a {}-byte budget",
        (MESSAGES * PAYLOAD) as f64 / 1024.0 / 1024.0,
        messages.len(),
        hub.max_backlog_bytes(),
    );

    assert!(
        retained_bytes <= hub.max_backlog_bytes(),
        "the retained backlog must never exceed max_backlog_bytes ({}) regardless of how many \
         oversized messages were published; measured {retained_bytes} bytes across {} messages",
        hub.max_backlog_bytes(),
        messages.len(),
    );
    Ok(())
}

/// What the live `broadcast` channel alone costs when a subscriber stalls or
/// disconnects without ever dropping its receiver (a leaked handle, a paused
/// client) — a cost that scales with `capacity × payload size` by
/// construction, since each of the channel's slots holds a full message
/// until consumed. There is no byte-based bound possible here short of
/// wrapping the channel itself, so this documents the shape of the cost
/// rather than asserting an absolute ceiling this scenario cannot meet.
/// Unlike the test above, an OS-level RSS measurement is adequate here
/// because nothing is asserted against it — it can only ever print.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_permanently_lagging_subscriber_costs_capacity_times_payload_size_per_room() -> TestResult
{
    use pharos_realtime::RealtimeSubscriber;

    const ROOMS: usize = 500;
    const PAYLOAD: usize = 32 * 1024;
    /// Mirrors `InMemoryHub`'s current default channel capacity so this test
    /// publishes comfortably past it regardless of future tuning.
    const DEFAULT_HUB_CAPACITY_HINT: usize = 32;

    let hub = Arc::new(InMemoryHub::new());
    let mut held = Vec::new();
    for i in 0..ROOMS {
        let target = room(&format!("game:{i}"));
        held.push(hub.subscribe(&target).await?);
    }

    let before = rss_bytes();
    for i in 0..ROOMS {
        let target = room(&format!("game:{i}"));
        // Publish well past `capacity` so the ring is saturated, not just
        // partially filled — the steady-state worst case for a room that is
        // actually receiving ongoing traffic.
        for _ in 0..(DEFAULT_HUB_CAPACITY_HINT * 4) {
            hub.publish(RealtimeMessage::new(
                target.clone(),
                "bulk",
                vec![0u8; PAYLOAD],
            ))
            .await?;
        }
    }
    let after = rss_bytes();
    let grew = after.saturating_sub(before);

    let per_room = grew / ROOMS as u64;
    let projected = per_room * 10_000;
    println!(
        "rooms={ROOMS} payload={PAYLOAD}B lagging_channel_growth={:.1} MiB => {:.1} MiB/room, \
         projected at the default 10_000-room ceiling: {:.1} GiB — this scales with \
         capacity × payload size by construction; tune InMemoryHub::with_capacity against \
         the app's own max payload if this must be bounded lower",
        grew as f64 / 1024.0 / 1024.0,
        per_room as f64 / 1024.0 / 1024.0,
        projected as f64 / 1024.0 / 1024.0 / 1024.0,
    );

    drop(held);
    Ok(())
}

/// Resident set size of this process, in bytes.
fn rss_bytes() -> u64 {
    let pid = std::process::id();
    let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
    else {
        return 0;
    };
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u64>()
        .unwrap_or(0)
        * 1024
}
