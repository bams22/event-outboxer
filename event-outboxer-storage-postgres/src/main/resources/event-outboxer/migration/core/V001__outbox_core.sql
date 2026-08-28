-- event-outboxer core schema. See docs/STORAGE.md for rationale.
-- Location: classpath:event-outboxer/migration/core — applied by the Spring
-- Boot starter's own Flyway instance (ADR-0028); plain-Java users point
-- their Flyway at the location themselves.
--
-- Schema name is parameterised so the library does not conflict with other
-- tables or libraries already living in the caller's database. The starter
-- sets the placeholder from event-outboxer.storage.schema (default:
-- event_outboxer). Plain-Java Flyway users set the placeholder themselves
-- via Flyway's placeholders(Map) API or -Dflyway.placeholders.*.

CREATE SCHEMA IF NOT EXISTS ${eventOutboxerSchema};

CREATE TABLE ${eventOutboxerSchema}.events (
    id               UUID         PRIMARY KEY,
    event_type       VARCHAR(128) NOT NULL,
    payload          JSONB        NOT NULL,
    payload_class    VARCHAR(512) NOT NULL,
    priority         SMALLINT     NOT NULL DEFAULT 0,
    attempts         INT          NOT NULL DEFAULT 0,
    status           VARCHAR(16)  NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    run_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    claimed_by       VARCHAR(64),
    claimed_at       TIMESTAMPTZ,
    last_fail_reason TEXT,
    trace_context    JSONB,
    version          BIGINT       NOT NULL DEFAULT 0,

    CONSTRAINT events_status_check
        CHECK (status IN ('PENDING', 'PROCESSING', 'DISABLED')),
    CONSTRAINT events_attempts_nonneg CHECK (attempts >= 0),
    CONSTRAINT events_version_nonneg  CHECK (version >= 0),
    CONSTRAINT events_priority_range  CHECK (priority BETWEEN -32768 AND 32767),
    CONSTRAINT events_processing_has_claim CHECK (
        (status = 'PROCESSING' AND claimed_by IS NOT NULL AND claimed_at IS NOT NULL)
        OR (status <> 'PROCESSING')
    )
);

-- Hot path: the claim query filters on status=PENDING, event_type and run_at,
-- then orders by priority DESC, run_at ASC. Partial on PENDING keeps the
-- index tiny even when historical DISABLED rows accumulate.
CREATE INDEX idx_events_ready
    ON ${eventOutboxerSchema}.events (event_type, priority DESC, run_at)
    WHERE status = 'PENDING';

-- Orphan recovery: find every event owned by a dead worker quickly.
CREATE INDEX idx_events_processing_by_worker
    ON ${eventOutboxerSchema}.events (claimed_by)
    WHERE status = 'PROCESSING';

-- Watchdog: scan PROCESSING events with stale claimed_at.
CREATE INDEX idx_events_processing_claimed_at
    ON ${eventOutboxerSchema}.events (claimed_at)
    WHERE status = 'PROCESSING';


CREATE TABLE ${eventOutboxerSchema}.workers (
    worker_id       VARCHAR(64)  PRIMARY KEY,
    host            VARCHAR(256) NOT NULL,
    pid             INT,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    graceful_stop   BOOLEAN      NOT NULL DEFAULT FALSE,
    metadata        JSONB
);

-- Orphan detection: find stale-heartbeat workers quickly; exclude clean shutdowns.
CREATE INDEX idx_workers_heartbeat
    ON ${eventOutboxerSchema}.workers (last_heartbeat)
    WHERE graceful_stop = FALSE;
