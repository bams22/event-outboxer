-- Claim-time dedup coalescing (ADR-0037): duplicates of a (event_type, dedup_key) are collapsed by
-- the claim statement, not by the insert. The V004 partial UNIQUE index goes — it was the arbiter of
-- the publisher's ON CONFLICT insert and it collided with every transition of a PROCESSING event
-- back to PENDING once a twin of its key had been inserted (23505 on retry, release, reclaim and
-- reenable). A plain partial index over the same columns stays: it is the lookup of the claim's
-- duplicate sweep and costs one probe per distinct key in a claim batch.
--
-- Coordinated step: the pre-ADR-0037 adapter names the unique index in its ON CONFLICT clause and
-- fails a keyed publish once it is gone. Apply together with the adapter that ships this migration.
--
-- Schema name comes from the ${eventOutboxerSchema} Flyway placeholder.

DROP INDEX IF EXISTS ${eventOutboxerSchema}.uq_events_pending_dedup_key;

CREATE INDEX IF NOT EXISTS ix_events_pending_dedup_key
    ON ${eventOutboxerSchema}.events (event_type, dedup_key)
    WHERE status = 'PENDING' AND dedup_key IS NOT NULL;
