-- Admin/retention support (ADR-0019): partial index over DISABLED rows.
--
-- Serves OutboxAdmin.findByStatus(DISABLED, ...) keyset pagination,
-- reenableAll and purgeDisabled sweeps — all of which filter on
-- status = 'DISABLED' and created_at. Without it every admin query over
-- the accumulated-failures backlog is a sequential scan of the hot table.
--
-- Schema name comes from the ${eventOutboxerSchema} Flyway placeholder,
-- auto-wired by the starter from event-outboxer.storage.schema (default:
-- event_outboxer).

CREATE INDEX idx_events_disabled_created_at
    ON ${eventOutboxerSchema}.events (created_at, id)
    WHERE status = 'DISABLED';
