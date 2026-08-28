-- Coalescing dedup key (ADR-0021): at most one PENDING event per (event_type, dedup_key).
--
-- The unique index is partial over PENDING on purpose:
--   * a PROCESSING event does not block a new publish with the same key — the new event runs
--     after the current one and sees the fresh data;
--   * DISABLED events do not block the key;
--   * inserts without a dedup key never touch the index.
-- The publisher pairs the ON CONFLICT DO NOTHING insert with SELECT ... FOR UPDATE on the
-- coalesced-into row, so claim queries (FOR UPDATE SKIP LOCKED) skip it until the publishing
-- transaction commits — the handler is guaranteed to see the coalesced transaction's changes.
--
-- Schema name comes from the ${eventOutboxerSchema} Flyway placeholder.

ALTER TABLE ${eventOutboxerSchema}.events ADD COLUMN dedup_key VARCHAR(256);

CREATE UNIQUE INDEX uq_events_pending_dedup_key
    ON ${eventOutboxerSchema}.events (event_type, dedup_key)
    WHERE status = 'PENDING' AND dedup_key IS NOT NULL;
