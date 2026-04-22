-- Optional archive table for successfully-processed events.
-- Applied only when the application configures
--   spring.flyway.locations=classpath:db/migration/outbox/core,classpath:db/migration/outbox/archive
-- See docs/STORAGE.md and ADR-0008.
--
-- Schema name comes from the ${eventOutboxerSchema} Flyway placeholder,
-- auto-wired by the starter from outbox.storage.schema (default:
-- event_outboxer).

CREATE TABLE ${eventOutboxerSchema}.event_archive (
    id               UUID         PRIMARY KEY,
    event_type       VARCHAR(128) NOT NULL,
    payload          JSONB        NOT NULL,
    payload_class    VARCHAR(512) NOT NULL,
    priority         SMALLINT     NOT NULL,
    attempts         INT          NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL,
    run_at           TIMESTAMPTZ  NOT NULL,
    last_fail_reason TEXT,
    trace_context    JSONB,
    archived_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    archived_by      VARCHAR(64)  NOT NULL
);

CREATE INDEX idx_archive_archived_at ON ${eventOutboxerSchema}.event_archive (archived_at);

CREATE INDEX idx_archive_event_type_created_at
    ON ${eventOutboxerSchema}.event_archive (event_type, created_at);
