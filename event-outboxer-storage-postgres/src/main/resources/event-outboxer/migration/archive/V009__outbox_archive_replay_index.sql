-- ADR-0033: index the access path of OutboxAdmin.replayAllFromArchive —
--   WHERE event_type = ? [AND archived_at > ?] [AND archived_at < ?]
--   [AND (archived_at, id) > (?, ?)] ORDER BY archived_at, id LIMIT ?
-- Neither V002 index serves it: idx_archive_archived_at has no event_type as a
-- leading column, and idx_archive_event_type_created_at is ordered by created_at.
-- Without this index the bulk replay walks the archive in archived_at order
-- filtering on event_type, and it does so inside the statement that INSERTs into
-- events and DELETEs from the archive — so those locks are held for the whole
-- scan. The trailing id makes the keyset predicate and the ORDER BY tie-break
-- index-resolvable too, so a cursor-driven sweep stays O(limit) per batch.

CREATE INDEX idx_archive_event_type_archived_at
    ON ${eventOutboxerSchema}.event_archive (event_type, archived_at, id);
