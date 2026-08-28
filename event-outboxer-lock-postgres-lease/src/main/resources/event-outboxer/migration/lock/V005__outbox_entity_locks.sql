-- event-outboxer entity-lock lease table (ADR-0022). See docs/STORAGE.md.
-- Opt-in: apply only when event-outboxer.lock.type=postgres-lease, via
-- spring.flyway.locations=classpath:db/migration/outbox/lock (in addition
-- to the core location).
--
-- V005 continues the shared numbering sequence (core: V001/V003/V004,
-- archive: V002) so aggregated Flyway locations never collide. Adopt this
-- location at upgrade time: enabling it after a later core migration has
-- been applied is an out-of-order migration for Flyway.
--
-- Schema name is parameterised the same way as the core migrations; the
-- Spring Boot starter auto-wires the placeholder from
-- event-outboxer.storage.schema (default: event_outboxer).

CREATE SCHEMA IF NOT EXISTS ${eventOutboxerSchema};

-- One row per held (or recently expired) business-key lock. All expiry
-- arithmetic runs on the database clock (now()); rows of crashed holders
-- are overwritten in place by the next acquirer once expires_at passes.
CREATE TABLE ${eventOutboxerSchema}.entity_locks (
    lock_key     VARCHAR(512) PRIMARY KEY,
    owner_token  VARCHAR(64)  NOT NULL,
    owner_worker VARCHAR(64),
    acquired_at  TIMESTAMPTZ  NOT NULL,
    expires_at   TIMESTAMPTZ  NOT NULL,

    CONSTRAINT entity_locks_expiry_after_acquire CHECK (expires_at > acquired_at)
);

-- No secondary index: the primary key serves both acquire and release, and
-- the table stays tiny (bounded by concurrently held leases plus leases
-- orphaned by crashes).
