-- event-outboxer entity-lock lease table (ADR-0022). See docs/STORAGE.md.
-- Location: classpath:event-outboxer/migration/lock, shipped in
-- event-outboxer-lock-postgres-lease — the starter's Flyway instance applies
-- it whenever the module is on the classpath (ADR-0028).
--
-- V005 continues the shared numbering sequence (core: V001/V003/V004/V006,
-- archive: V002/V007). The lanes touch disjoint tables and the starter runs
-- with outOfOrder on, so adopting this lane after later core migrations have
-- been applied is fine.
--
-- Schema name is parameterised the same way as the core migrations; the
-- Spring Boot starter sets the placeholder from
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
