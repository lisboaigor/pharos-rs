use std::fmt::Display;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use pharos_app::{OutboxMessage, TenantContext};
use pharos_core::RepositoryError;
use pharos_es::{EventStore, Snapshot, SnapshotStore, StoredEvent};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::Row;
use thiserror::Error;
use tracing::{Instrument, info_span};
use uuid::Uuid;

use crate::pool::{PgPoolError, Pool};
use crate::transaction::insert_outbox_in_tx;

/// Sentinel tenant used by the deprecated, non-tenant-scoped constructors
/// ([`PgEventStore::with_stream_type`], [`PgSnapshotStore::with_stream_type`]).
///
/// A single-tenant deployment that never adopts [`TenantContext`] gets exactly
/// its previous behavior: every stream lives under one fixed, shared value
/// that a real [`TenantId`](pharos_app::TenantId) — generated as a UUID v4 or
/// v7 — will not collide with in practice. What this does **not** do is
/// isolate a stream created through the deprecated path from a real tenant
/// that also happens to use `stream_id`s from the same namespace: mixing the
/// deprecated constructor and [`PgEventStore::new`] against the same
/// `stream_type` in one deployment reintroduces the original collision risk.
/// Migrate fully to [`PgEventStore::new`] before onboarding a second tenant.
pub const NO_TENANT: Uuid = Uuid::nil();

/// Default PostgreSQL schema for event streams and snapshots.
///
/// `tenant_id` closes a cross-tenant collision: without it, two tenants using
/// the same natural or sequential `stream_id` (`"order-42"`, `"ledger-1"`)
/// shared one stream — tenant B's append would see tenant A's head and either
/// conflict with it or interleave into its history, and `load` would replay
/// both tenants' events into a single aggregate. The `ALTER TABLE` /
/// conditional primary-key swap upgrade an installation created before the
/// column existed; a fresh install gets the tenant-scoped primary key
/// directly from `CREATE TABLE`. Both are safe to run on every startup: the
/// column add is a no-op once present (`ADD COLUMN IF NOT EXISTS`), and the
/// key swap is skipped once `tenant_id` is already part of the primary key.
pub const POSTGRES_EVENT_STORE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pharos_event_streams (
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    event_type TEXT NULL,
    schema_version INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, stream_type, stream_id, sequence)
);
ALTER TABLE pharos_event_streams
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000000';
-- NULL, not NOT NULL: a row written before this column existed has no
-- retroactive way to know its event type. `PgEventStore::load` treats a
-- NULL `event_type` as "no upcasting opinion" and passes the payload
-- through unchanged — the only behavior available before this column
-- existed. Every row appended from here on always gets one.
ALTER TABLE pharos_event_streams
    ADD COLUMN IF NOT EXISTS event_type TEXT NULL;
ALTER TABLE pharos_event_streams
    ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 0;
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tc.constraint_name
         AND kcu.table_schema = tc.table_schema
        WHERE tc.table_name = 'pharos_event_streams'
          AND tc.constraint_type = 'PRIMARY KEY'
          AND kcu.column_name = 'tenant_id'
    ) THEN
        ALTER TABLE pharos_event_streams DROP CONSTRAINT pharos_event_streams_pkey;
        ALTER TABLE pharos_event_streams
            ADD PRIMARY KEY (tenant_id, stream_type, stream_id, sequence);
    END IF;
END
$do$;
CREATE TABLE IF NOT EXISTS pharos_snapshots (
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    version BIGINT NOT NULL,
    taken_at TIMESTAMPTZ NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, stream_type, stream_id)
);
ALTER TABLE pharos_snapshots
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000000';
ALTER TABLE pharos_snapshots
    ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 0;
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tc.constraint_name
         AND kcu.table_schema = tc.table_schema
        WHERE tc.table_name = 'pharos_snapshots'
          AND tc.constraint_type = 'PRIMARY KEY'
          AND kcu.column_name = 'tenant_id'
    ) THEN
        ALTER TABLE pharos_snapshots DROP CONSTRAINT pharos_snapshots_pkey;
        ALTER TABLE pharos_snapshots
            ADD PRIMARY KEY (tenant_id, stream_type, stream_id);
    END IF;
END
$do$;
"#;

/// Installs the event store schema.
pub async fn migrate_postgres_event_store_schema(pool: &Pool) -> Result<(), PgPoolError> {
    sqlx::raw_sql(POSTGRES_EVENT_STORE_SCHEMA)
        .execute(pool)
        .await?;
    Ok(())
}

/// Error produced by [`PgEventStore`] and [`PgSnapshotStore`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostgresEventStoreError {
    #[error("postgres event store failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Enqueuing an outbox message alongside the append failed.
    #[error("outbox enqueue failed: {0}")]
    Outbox(String),
    /// A single `load`/`load_after` call would have returned more than
    /// [`PgEventStore::max_events_per_load`] events.
    ///
    /// Returned instead of a silently truncated `Vec`: handing back only the
    /// first `max_events_per_load` rows would let a caller rehydrate an
    /// aggregate from an incomplete stream and believe it was replayed in
    /// full — worse than refusing outright. Snapshot the aggregate (see
    /// [`SnapshottingEventSourcedRepository`](pharos_es::SnapshottingEventSourcedRepository))
    /// so ordinary reads only ever replay the tail since the last snapshot,
    /// or raise the limit if a stream this long is expected.
    #[error(
        "stream '{stream_type}/{stream_id}' has more than {limit} events; \
         refusing to load it unbounded — snapshot this aggregate or raise the limit"
    )]
    StreamTooLarge {
        /// The stream's type discriminator.
        stream_type: String,
        /// The stream id that exceeded the limit.
        stream_id: String,
        /// The configured ceiling that was exceeded.
        limit: usize,
    },
    /// A registered [`EventUpcasterRegistry`] upcaster rejected the payload.
    #[error("upcast of '{event_type}' from version {from_version} failed: {source}")]
    Upcast {
        /// Logical event type being upcast.
        event_type: String,
        /// Version the failing upcaster consumes.
        from_version: u32,
        /// Error returned by the upcaster.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A row's `schema_version` is higher than any version the configured
    /// [`EventUpcasterRegistry`]'s chain for its `event_type` reaches — a
    /// row claiming to be from a future schema this consumer has not been
    /// upgraded to understand.
    #[error(
        "'{event_type}' claims schema_version {version}, but the registered upcaster chain \
         only reaches version {known_up_to}"
    )]
    UnsupportedEventVersion {
        /// Logical event type.
        event_type: String,
        /// The version the row claimed.
        version: u32,
        /// The highest version the registry's chain for `event_type` reaches.
        known_up_to: u32,
    },
    /// The upcaster chain for `event_type` stopped short of the registry's
    /// known top version, with no upcaster registered for the version it
    /// stopped at — a hole in the chain, not a too-new schema (see
    /// [`Self::UnsupportedEventVersion`] for that case). Passing a stalled,
    /// partially-upcast payload through to deserialize risks silently
    /// misinterpreting it under the wrong shape.
    #[error(
        "'{event_type}' upcast chain stopped at version {stopped_at}, short of the registry's \
         known top version {known_up_to} — an upcaster registration is missing for version \
         {stopped_at}"
    )]
    EventUpcastGap {
        /// Logical event type.
        event_type: String,
        /// The version the chain stalled at (no upcaster registered from here).
        stopped_at: u32,
        /// The highest version the registry's chain for `event_type` reaches.
        known_up_to: u32,
    },
}

/// PostgreSQL append-only event store with JSONB payloads.
///
/// Optimistic concurrency is enforced twice: the current stream head is
/// compared against `expected_version` inside the append transaction, and the
/// `(tenant_id, stream_type, stream_id, sequence)` primary key is the arbiter
/// for concurrent appenders — the loser's unique violation is reported as
/// [`RepositoryError::ConcurrencyConflict`].
///
/// Every query is scoped by `tenant_id`, so two tenants using the same
/// `stream_id` under the same `stream_type` — the natural or sequential keys
/// most aggregates use — get two separate streams, never one shared or
/// interleaved one.
pub struct PgEventStore<I, E> {
    pool: Pool,
    tenant_id: Uuid,
    stream_type: String,
    max_events_per_load: usize,
    upcaster: Option<Arc<EventUpcasterRegistry>>,
    _marker: std::marker::PhantomData<fn() -> (I, E)>,
}

/// Function that transforms one event payload from `from_version` to
/// `from_version + 1`.
type EventUpcastFn = Box<
    dyn Fn(Value) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>>
        + Send
        + Sync,
>;

/// Chain of upcasters keyed by `(event_type, from_version)`, applied to a
/// persisted [`pharos_core::DomainEvent`] payload before it is deserialized.
///
/// An append-only stream outlives the code that wrote it. Adding an optional
/// field is handled by `#[serde(default)]`, but renaming one, splitting a
/// variant, or changing a type is not: the old payload simply stops
/// deserializing, and an event that cannot be read is an aggregate that
/// cannot be rebuilt. For anything the history is used to *settle* — a
/// balance, a match result — that is unrecoverable rather than inconvenient.
///
/// Each registered upcaster steps a payload exactly one version forward;
/// [`PgEventStore::load`]/[`load_after`](PgEventStore::load_after) apply as
/// many as needed, in order, driven by the row's stored `event_type` and
/// `schema_version` columns — no inferring the version from the JSON's own
/// shape, and no reimplementing the chaining logic per store, unlike the
/// single-hop, blind design this replaced.
///
/// This is the same chaining and gap-detection design as
/// [`pharos_app::upcast::JsonUpcasterRegistry`], applied to event-store rows
/// instead of integration-event envelopes — the two exist separately because
/// the version lives in different places (JSON envelope fields there,
/// dedicated columns here), not because the logic differs.
///
/// ```
/// # use serde_json::Value;
/// # use pharos_postgres::EventUpcasterRegistry;
/// let registry = EventUpcasterRegistry::new().with_upcaster(
///     "MoveMade",
///     0,
///     |mut payload: Value| {
///         let Some(object) = payload.as_object_mut() else {
///             return Err("payload must be an object");
///         };
///         if let Some(mover) = object.remove("mover") {
///             object.insert("player_id".to_string(), mover);
///         }
///         Ok(payload)
///     },
/// );
/// ```
#[derive(Default)]
pub struct EventUpcasterRegistry {
    upcasters: std::collections::HashMap<(String, u32), EventUpcastFn>,
    max_known_version: std::collections::HashMap<String, u32>,
}

impl std::fmt::Debug for EventUpcasterRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventUpcasterRegistry")
            .field("registered", &self.upcasters.len())
            .finish()
    }
}

impl EventUpcasterRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an upcaster that transforms `event_type` payloads from
    /// `from_version` to `from_version + 1`.
    pub fn with_upcaster<F, Err>(
        mut self,
        event_type: impl Into<String>,
        from_version: u32,
        upcast: F,
    ) -> Self
    where
        F: Fn(Value) -> Result<Value, Err> + Send + Sync + 'static,
        Err: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        let event_type = event_type.into();
        let reaches = from_version + 1;
        self.max_known_version
            .entry(event_type.clone())
            .and_modify(|max| *max = (*max).max(reaches))
            .or_insert(reaches);
        self.upcasters.insert(
            (event_type, from_version),
            Box::new(move |payload| upcast(payload).map_err(Into::into)),
        );
        self
    }

    fn get(&self, event_type: &str, from_version: u32) -> Option<&EventUpcastFn> {
        self.upcasters.get(&(event_type.to_string(), from_version))
    }

    fn max_known_version(&self, event_type: &str) -> Option<u32> {
        self.max_known_version.get(event_type).copied()
    }

    /// Applies every upcaster in the chain for `event_type`, starting at
    /// `schema_version`, until reaching this registry's known top version
    /// for that type (or until no upcaster is registered for `event_type`
    /// at all, in which case the payload passes through unchanged).
    ///
    /// Errors on a version above the chain's known top (a schema newer than
    /// this registry understands) and on a gap in the chain (the loop
    /// stopped short of the known top, meaning a registration is missing) —
    /// passing either through as if current would risk silently
    /// misinterpreting the payload under the wrong shape.
    fn apply(
        &self,
        event_type: &str,
        schema_version: u32,
        mut payload: Value,
    ) -> Result<Value, PostgresEventStoreError> {
        let mut version = schema_version;
        while let Some(upcast) = self.get(event_type, version) {
            payload = upcast(payload).map_err(|source| PostgresEventStoreError::Upcast {
                event_type: event_type.to_string(),
                from_version: version,
                source,
            })?;
            version += 1;
        }

        if let Some(known_up_to) = self.max_known_version(event_type) {
            if version > known_up_to {
                return Err(PostgresEventStoreError::UnsupportedEventVersion {
                    event_type: event_type.to_string(),
                    version,
                    known_up_to,
                });
            }
            if version < known_up_to {
                return Err(PostgresEventStoreError::EventUpcastGap {
                    event_type: event_type.to_string(),
                    stopped_at: version,
                    known_up_to,
                });
            }
        }
        Ok(payload)
    }
}

/// Rewrites a persisted [`Snapshot`](pharos_es::Snapshot) payload into the
/// shape the current state type deserializes from.
///
/// Without this, [`PgSnapshotStore::load`] deserializes the stored payload
/// directly — no upcaster runs on the snapshot path at all. If a change to
/// the state type is serde-compatible (a new field with `#[serde(default)]`)
/// but not semantically compatible, that direct deserialization *succeeds*,
/// silently, into a value that means something different than the code now
/// assumes it does; the aggregate then rehydrates from corrupted state with
/// no error anywhere. [`EventUpcasterRegistry`] exists to prevent exactly
/// this on the event side — this is the same guard for snapshots, just a
/// single blind hop rather than a version-keyed chain: a snapshot is always
/// re-derived from the event stream it is an optimization over, so a stale
/// snapshot shape only ever needs one step forward to the shape the code
/// currently reads.
///
/// `schema_version` (the value stored alongside the row, from
/// [`Snapshot::schema_version`](pharos_es::Snapshot::schema_version)) is
/// passed in, so an implementation can branch on it directly instead of
/// inferring the version from the payload's shape.
///
/// ```
/// # use serde_json::Value;
/// # use pharos_postgres::SnapshotUpcaster;
/// struct AddDefaultLoyaltyTier;
///
/// impl SnapshotUpcaster for AddDefaultLoyaltyTier {
///     fn upcast(&self, payload: &mut Value, schema_version: u32) {
///         if schema_version < 1
///             && let Some(object) = payload.as_object_mut()
///         {
///             object
///                 .entry("loyalty_tier")
///                 .or_insert_with(|| Value::String("standard".to_string()));
///         }
///     }
/// }
/// ```
pub trait SnapshotUpcaster: Send + Sync + 'static {
    /// Rewrites one raw snapshot payload in place, given the schema version
    /// it was stored under. A payload already in the current shape must be
    /// left untouched.
    fn upcast(&self, payload: &mut Value, schema_version: u32);
}

/// Default ceiling on events returned by a single `load`/`load_after` call.
///
/// `load`/`load_after` materialize their whole result into a `Vec` — there is
/// no batching or streaming — so an unbounded stream is an unbounded
/// in-memory allocation. Ordinary reads through
/// [`SnapshottingEventSourcedRepository`](pharos_es::SnapshottingEventSourcedRepository)
/// only ever replay the tail since the last snapshot, which stays well under
/// this by construction; a stream large enough to hit it either has no
/// snapshot store configured or has a bug driving unbounded appends into one
/// stream.
pub const DEFAULT_MAX_EVENTS_PER_LOAD: usize = 100_000;

impl<I, E> std::fmt::Debug for PgEventStore<I, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgEventStore")
            .field("tenant_id", &self.tenant_id)
            .field("stream_type", &self.stream_type)
            .finish_non_exhaustive()
    }
}

impl<I, E> PgEventStore<I, E> {
    /// Creates an event store scoped to one tenant, with the default
    /// per-load event ceiling ([`DEFAULT_MAX_EVENTS_PER_LOAD`]).
    pub fn new(pool: Pool, tenant: &TenantContext, stream_type: impl Into<String>) -> Self {
        Self {
            pool,
            tenant_id: tenant.tenant_id().as_uuid(),
            stream_type: stream_type.into(),
            max_events_per_load: DEFAULT_MAX_EVENTS_PER_LOAD,
            upcaster: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Installs an [`EventUpcasterRegistry`] applied to every payload,
    /// driven by the row's `event_type`/`schema_version` columns, before it
    /// is deserialized.
    pub fn with_upcaster(mut self, upcaster: EventUpcasterRegistry) -> Self {
        self.upcaster = Some(Arc::new(upcaster));
        self
    }

    /// Overrides the per-load event ceiling.
    ///
    /// Raise this deliberately for a stream expected to legitimately exceed
    /// the default — prefer adding a [`SnapshottingEventSourcedRepository`](
    /// pharos_es::SnapshottingEventSourcedRepository) first, since that
    /// bounds ordinary read cost regardless of how long the stream grows.
    pub fn with_max_events_per_load(mut self, max_events_per_load: usize) -> Self {
        self.max_events_per_load = max_events_per_load;
        self
    }

    /// Creates an event store with no tenant scoping — every stream lives
    /// under the fixed [`NO_TENANT`] sentinel.
    ///
    /// Correct for a single-tenant deployment, since nothing else will ever
    /// share that sentinel. **Do not** mix this with [`PgEventStore::new`]
    /// against the same `stream_type`: onboarding a real tenant means
    /// migrating fully to [`new`](Self::new), not adding it alongside this
    /// constructor, or the collision this crate's tenant scoping exists to
    /// prevent comes right back for the sentinel's streams.
    #[deprecated(
        note = "use PgEventStore::new with a TenantContext — this constructor scopes every \
                stream to the same fixed sentinel tenant and offers no cross-tenant isolation"
    )]
    pub fn with_stream_type(pool: Pool, stream_type: impl Into<String>) -> Self {
        Self {
            pool,
            tenant_id: NO_TENANT,
            stream_type: stream_type.into(),
            max_events_per_load: DEFAULT_MAX_EVENTS_PER_LOAD,
            upcaster: None,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub fn tenant_id(&self) -> Uuid {
        self.tenant_id
    }
    pub fn stream_type(&self) -> &str {
        &self.stream_type
    }
    pub fn max_events_per_load(&self) -> usize {
        self.max_events_per_load
    }

    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_event_store_schema(&self.pool).await
    }

    async fn stream_head(&self, stream_id: &str) -> Result<u64, PostgresEventStoreError> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(sequence), 0) AS head FROM pharos_event_streams
             WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3",
        )
        .bind(self.tenant_id)
        .bind(&self.stream_type)
        .bind(stream_id)
        .fetch_one(&self.pool)
        .await?;
        let head: i64 = row.try_get("head")?;
        Ok(head as u64)
    }
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(db) if db.is_unique_violation()
    )
}

impl<I, E> EventStore<I, E> for PgEventStore<I, E>
where
    I: Display + Send + Sync + 'static,
    E: pharos_core::DomainEvent + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Error = PostgresEventStoreError;

    async fn load(&self, id: &I) -> Result<Vec<StoredEvent<E>>, Self::Error> {
        self.load_after(id, 0).await
    }

    async fn load_after(&self, id: &I, after: u64) -> Result<Vec<StoredEvent<E>>, Self::Error> {
        async move {
            let stream_id = id.to_string();

            // Fetch one row past the ceiling to tell "exactly at the limit"
            // from "over it" without a separate COUNT query.
            let rows = sqlx::query(
                "SELECT sequence, payload, recorded_at, event_type, schema_version
                 FROM pharos_event_streams
                 WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3 AND sequence > $4
                 ORDER BY sequence
                 LIMIT $5",
            )
            .bind(self.tenant_id)
            .bind(&self.stream_type)
            .bind(&stream_id)
            .bind(after as i64)
            .bind(self.max_events_per_load as i64 + 1)
            .fetch_all(&self.pool)
            .await?;

            if rows.len() > self.max_events_per_load {
                return Err(PostgresEventStoreError::StreamTooLarge {
                    stream_type: self.stream_type.clone(),
                    stream_id,
                    limit: self.max_events_per_load,
                });
            }

            rows.into_iter()
                .map(|row| {
                    let sequence: i64 = row.try_get("sequence")?;
                    let payload: Value = row
                        .try_get::<sqlx::types::Json<Value>, _>("payload")
                        .map(|j| j.0)?;
                    let recorded_at: DateTime<Utc> = row.try_get("recorded_at")?;
                    // `event_type` is NULL for rows written before this
                    // column existed: there is no way to retroactively know
                    // what type they are, so they pass through unupcast,
                    // exactly the (only) behavior available before this
                    // column was added. A row with the column populated
                    // always runs the full registry chain, gap-checked.
                    let event_type: Option<String> = row.try_get("event_type")?;
                    let schema_version: i32 = row.try_get("schema_version")?;
                    let payload = match (&self.upcaster, event_type) {
                        (Some(upcaster), Some(event_type)) => {
                            upcaster.apply(&event_type, schema_version as u32, payload)?
                        }
                        _ => payload,
                    };
                    Ok(StoredEvent {
                        sequence: sequence as u64,
                        event: serde_json::from_value(payload)?,
                        recorded_at,
                    })
                })
                .collect()
        }
        .instrument(info_span!(
            "postgres.event_store.load",
            stream_type = self.stream_type,
        ))
        .await
    }

    async fn append(
        &self,
        id: &I,
        expected_version: u64,
        events: Vec<E>,
    ) -> Result<(), RepositoryError<Self::Error>> {
        self.append_with_outbox(id, expected_version, events, |_| Vec::new())
            .await
    }

    async fn delete_stream(&self, id: &I) -> Result<(), Self::Error> {
        async move {
            let stream_id = id.to_string();
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "DELETE FROM pharos_event_streams
                 WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3",
            )
            .bind(self.tenant_id)
            .bind(&self.stream_type)
            .bind(&stream_id)
            .execute(&mut *tx)
            .await?;
            // A snapshot without its stream would resurrect deleted state.
            sqlx::query(
                "DELETE FROM pharos_snapshots
                 WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3",
            )
            .bind(self.tenant_id)
            .bind(&self.stream_type)
            .bind(&stream_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            metrics::counter!(
                "pharos.postgres.event_store.stream_deleted",
                "stream_type" => self.stream_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.event_store.delete_stream",
            stream_type = self.stream_type,
        ))
        .await
    }
}

/// PostgreSQL snapshot store with JSONB payloads.
///
/// Pairs with [`PgEventStore`] under the same `tenant_id` and `stream_type`
/// so `PgEventStore::delete_stream` removes both the events and the
/// snapshot, and so a snapshot never gets read back for the wrong tenant's
/// aggregate. Construct both from the same [`TenantContext`] and
/// `stream_type`.
pub struct PgSnapshotStore<I, S> {
    pool: Pool,
    tenant_id: Uuid,
    stream_type: String,
    upcaster: Option<Arc<dyn SnapshotUpcaster>>,
    _marker: std::marker::PhantomData<fn() -> (I, S)>,
}

impl<I, S> std::fmt::Debug for PgSnapshotStore<I, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgSnapshotStore")
            .field("tenant_id", &self.tenant_id)
            .field("stream_type", &self.stream_type)
            .finish_non_exhaustive()
    }
}

impl<I, E> PgEventStore<I, E>
where
    I: Display + Send + Sync + 'static,
    E: pharos_core::DomainEvent + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Appends events and enqueues outbox messages in the **same** transaction.
    ///
    /// This is the event-sourced counterpart of
    /// [`save_aggregate_and_enqueue`](crate::save_aggregate_and_enqueue). Use
    /// it whenever an event crossing a context boundary must not be lost if the
    /// process dies right after the append: with a plain
    /// [`EventStore::append`] the events are durable but the in-process
    /// delivery that follows is not, so a crash in between leaves the event
    /// persisted and never delivered.
    ///
    /// `map_events` receives the events about to be appended and returns the
    /// messages to enqueue. Returning an empty vector makes this exactly
    /// equivalent to `append`.
    pub async fn append_and_enqueue<F>(
        &self,
        id: &I,
        expected_version: u64,
        events: Vec<E>,
        map_events: F,
    ) -> Result<(), RepositoryError<PostgresEventStoreError>>
    where
        F: Fn(&[E]) -> Vec<OutboxMessage> + Send,
    {
        self.append_with_outbox(id, expected_version, events, map_events)
            .await
    }

    async fn append_with_outbox<F>(
        &self,
        id: &I,
        expected_version: u64,
        events: Vec<E>,
        map_events: F,
    ) -> Result<(), RepositoryError<PostgresEventStoreError>>
    where
        F: Fn(&[E]) -> Vec<OutboxMessage> + Send,
    {
        async move {
            if events.is_empty() {
                return Ok(());
            }
            let outbox = map_events(&events);
            let stream_id = id.to_string();

            // Serialize before touching the database so a bad payload never
            // opens a transaction.
            let payloads = events
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Serialization(e)))?;

            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Storage(e)))?;

            // A stale expected_version above the head would otherwise insert
            // past a gap without tripping the primary key.
            let row = sqlx::query(
                "SELECT COALESCE(MAX(sequence), 0) AS head FROM pharos_event_streams
                 WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3",
            )
            .bind(self.tenant_id)
            .bind(&self.stream_type)
            .bind(&stream_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Storage(e)))?;
            let head: i64 = row
                .try_get("head")
                .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Storage(e)))?;
            if head as u64 != expected_version {
                return Err(RepositoryError::ConcurrencyConflict {
                    expected: expected_version,
                    actual: Some(head as u64),
                });
            }

            let now = Utc::now();
            for (offset, (event, payload)) in events.iter().zip(payloads.iter()).enumerate() {
                let sequence = expected_version + offset as u64 + 1;
                let result = sqlx::query(
                    "INSERT INTO pharos_event_streams
                        (tenant_id, stream_type, stream_id, sequence, payload, recorded_at,
                         event_type, schema_version)
                     VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8)",
                )
                .bind(self.tenant_id)
                .bind(&self.stream_type)
                .bind(&stream_id)
                .bind(sequence as i64)
                .bind(payload)
                .bind(now)
                .bind(event.event_type())
                .bind(event.schema_version() as i32)
                .execute(&mut *tx)
                .await;

                if let Err(error) = result {
                    // The primary key arbitrates concurrent appenders: the
                    // loser sees a unique violation, reported as a conflict.
                    if is_unique_violation(&error) {
                        drop(tx);
                        let actual = self.stream_head(&stream_id).await.ok();
                        return Err(RepositoryError::ConcurrencyConflict {
                            expected: expected_version,
                            actual,
                        });
                    }
                    return Err(RepositoryError::Storage(PostgresEventStoreError::Storage(
                        error,
                    )));
                }
            }

            for message in &outbox {
                insert_outbox_in_tx(&mut tx, message).await.map_err(|e| {
                    RepositoryError::Storage(PostgresEventStoreError::Outbox(e.to_string()))
                })?;
            }

            tx.commit()
                .await
                .map_err(|e| RepositoryError::Storage(PostgresEventStoreError::Storage(e)))?;
            metrics::counter!(
                "pharos.postgres.event_store.appended",
                "stream_type" => self.stream_type.clone()
            )
            .increment(payloads.len() as u64);
            if !outbox.is_empty() {
                metrics::counter!(
                    "pharos.postgres.event_store.enqueued",
                    "stream_type" => self.stream_type.clone()
                )
                .increment(outbox.len() as u64);
            }
            Ok(())
        }
        .instrument(info_span!(
            "postgres.event_store.append",
            stream_type = self.stream_type,
        ))
        .await
    }
}

impl<I, S> PgSnapshotStore<I, S> {
    /// Creates a snapshot store scoped to one tenant.
    pub fn new(pool: Pool, tenant: &TenantContext, stream_type: impl Into<String>) -> Self {
        Self {
            pool,
            tenant_id: tenant.tenant_id().as_uuid(),
            stream_type: stream_type.into(),
            upcaster: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Installs a [`SnapshotUpcaster`] applied to every payload — with the
    /// row's stored `schema_version` — before it is deserialized.
    pub fn with_upcaster(mut self, upcaster: Arc<dyn SnapshotUpcaster>) -> Self {
        self.upcaster = Some(upcaster);
        self
    }

    /// Creates a snapshot store with no tenant scoping. See
    /// [`PgEventStore::with_stream_type`] for exactly what this does and
    /// does not protect against.
    #[deprecated(
        note = "use PgSnapshotStore::new with a TenantContext — this constructor scopes every \
                snapshot to the same fixed sentinel tenant and offers no cross-tenant isolation"
    )]
    pub fn with_stream_type(pool: Pool, stream_type: impl Into<String>) -> Self {
        Self {
            pool,
            tenant_id: NO_TENANT,
            stream_type: stream_type.into(),
            upcaster: None,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
    pub fn tenant_id(&self) -> Uuid {
        self.tenant_id
    }
    pub fn stream_type(&self) -> &str {
        &self.stream_type
    }

    pub async fn migrate(&self) -> Result<(), PgPoolError> {
        migrate_postgres_event_store_schema(&self.pool).await
    }
}

impl<I, S> SnapshotStore<I, S> for PgSnapshotStore<I, S>
where
    I: Display + Send + Sync + 'static,
    S: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Error = PostgresEventStoreError;

    async fn load(&self, id: &I) -> Result<Option<Snapshot<S>>, Self::Error> {
        async move {
            let row = sqlx::query(
                "SELECT payload, version, taken_at, schema_version FROM pharos_snapshots
                 WHERE tenant_id = $1 AND stream_type = $2 AND stream_id = $3",
            )
            .bind(self.tenant_id)
            .bind(&self.stream_type)
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;

            row.map(|row| {
                let version: i64 = row.try_get("version")?;
                let mut payload: Value = row
                    .try_get::<sqlx::types::Json<Value>, _>("payload")
                    .map(|j| j.0)?;
                let taken_at: DateTime<Utc> = row.try_get("taken_at")?;
                let schema_version: i32 = row.try_get("schema_version")?;
                let schema_version = schema_version as u32;
                if let Some(upcaster) = &self.upcaster {
                    upcaster.upcast(&mut payload, schema_version);
                }
                Ok(Snapshot {
                    state: serde_json::from_value(payload)?,
                    version: version as u64,
                    taken_at,
                    schema_version,
                })
            })
            .transpose()
        }
        .instrument(info_span!(
            "postgres.snapshot_store.load",
            stream_type = self.stream_type,
        ))
        .await
    }

    async fn save(&self, id: &I, snapshot: Snapshot<S>) -> Result<(), Self::Error> {
        async move {
            let payload = serde_json::to_string(&snapshot.state)?;
            // The `WHERE` on the upsert is a monotonicity guard: without it,
            // a delayed writer (a retried snapshot task, a race between two
            // callers) can overwrite a newer snapshot with an older one.
            // Losing that race is silent and harmless for correctness — the
            // event store remains authoritative and the stale snapshot's tail
            // just gets replayed on the next read — but it is still a
            // regression worth refusing rather than accepting quietly.
            sqlx::query(
                "INSERT INTO pharos_snapshots
                    (tenant_id, stream_type, stream_id, payload, version, taken_at, schema_version)
                 VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
                 ON CONFLICT (tenant_id, stream_type, stream_id) DO UPDATE
                 SET payload = EXCLUDED.payload,
                     version = EXCLUDED.version,
                     taken_at = EXCLUDED.taken_at,
                     schema_version = EXCLUDED.schema_version
                 WHERE pharos_snapshots.version < EXCLUDED.version",
            )
            .bind(self.tenant_id)
            .bind(&self.stream_type)
            .bind(id.to_string())
            .bind(&payload)
            .bind(snapshot.version as i64)
            .bind(snapshot.taken_at)
            .bind(snapshot.schema_version as i32)
            .execute(&self.pool)
            .await?;
            metrics::counter!(
                "pharos.postgres.snapshot_store.saved",
                "stream_type" => self.stream_type.clone()
            )
            .increment(1);
            Ok(())
        }
        .instrument(info_span!(
            "postgres.snapshot_store.save",
            stream_type = self.stream_type,
        ))
        .await
    }
}
