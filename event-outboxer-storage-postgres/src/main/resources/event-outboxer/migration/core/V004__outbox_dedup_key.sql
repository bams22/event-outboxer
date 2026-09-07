-- Dedup key (ADR-0021 for the key, ADR-0037 for the coalescing): duplicates of an
-- (event_type, dedup_key) are collapsed by the claim statement, never by the insert.
--
-- The index is a plain partial index over PENDING rows — the lookup of the claim's duplicate
-- sweep, one probe per distinct key of a claim batch. It is deliberately NOT unique: a unique
-- index would arbitrate an ON CONFLICT insert at publish time, and such an index collides with
-- every transition of a PROCESSING event back to PENDING once a twin of its key was inserted
-- (23505 on retry, release, reclaim and reenable). ADR-0037 records that design and why it went.
--   * inserts without a dedup key never touch the index;
--   * PROCESSING and DISABLED rows are outside it, so it stays as small as the due backlog.
--
-- Schema name comes from the ${eventOutboxerSchema} Flyway placeholder.

ALTER TABLE ${eventOutboxerSchema}.events ADD COLUMN dedup_key VARCHAR(256);

CREATE INDEX ix_events_pending_dedup_key
    ON ${eventOutboxerSchema}.events (event_type, dedup_key)
    WHERE status = 'PENDING' AND dedup_key IS NOT NULL;
