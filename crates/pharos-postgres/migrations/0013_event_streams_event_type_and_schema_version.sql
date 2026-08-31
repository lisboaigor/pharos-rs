-- Pharos PostgreSQL migrations: event_type + schema_version for
-- pharos_event_streams
--
-- Pairs with pharos_core::DomainEvent::schema_version and
-- pharos-postgres's EventUpcasterRegistry (crates/pharos-postgres/src/event_store.rs):
-- without these columns, the event store had no way to tell which shape a
-- stored payload was written under except by inferring it from the JSON's
-- own structure. `event_type` is nullable on purpose — a row written before
-- this column existed has no retroactive way to know its type, and
-- PgEventStore::load treats a NULL event_type as "no upcasting opinion",
-- passing the payload through unchanged (the only behavior available
-- before this column existed). Every row appended from here on always gets
-- both columns populated.

ALTER TABLE pharos_event_streams
    ADD COLUMN IF NOT EXISTS event_type TEXT NULL;

ALTER TABLE pharos_event_streams
    ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 0;
