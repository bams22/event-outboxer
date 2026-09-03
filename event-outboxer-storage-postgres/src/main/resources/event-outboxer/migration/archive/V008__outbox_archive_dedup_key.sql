-- ADR-0033: carry the coalescing dedup key (ADR-0021) into the archive for audit
-- and replay. Nullable, no index — uniqueness stays on the hot events table only
-- (partial unique index of V004).

ALTER TABLE ${eventOutboxerSchema}.event_archive ADD COLUMN dedup_key VARCHAR(256);
