-- ADR-0025: binary-capable payload lane + per-event serializer format.
--
-- The payload now travels in exactly one of two columns:
--   payload        JSONB  — textual formats (JSON), readable in psql, GIN-indexable
--   payload_binary BYTEA  — binary formats (e.g. Protobuf), stored verbatim
-- payload_format records the stable EventSerializer.format() id that wrote the
-- row; the dispatcher selects the deserializer by this value, which keeps
-- rolling deploys and format migrations safe.
--
-- Versions V003-V005 are taken (V005 by the lock-postgres-lease module — all
-- lanes share one version sequence in the outbox's history table), hence V006.

ALTER TABLE ${eventOutboxerSchema}.events ALTER COLUMN payload DROP NOT NULL;

ALTER TABLE ${eventOutboxerSchema}.events ADD COLUMN payload_binary BYTEA;

ALTER TABLE ${eventOutboxerSchema}.events ADD COLUMN payload_format VARCHAR(64);

-- Backfill before NOT NULL: every pre-existing row was written by the Jackson
-- serializer — the only implementation shipped before this migration (ADR-0011).
UPDATE ${eventOutboxerSchema}.events SET payload_format = 'jackson-json' WHERE payload_format IS NULL;

ALTER TABLE ${eventOutboxerSchema}.events ALTER COLUMN payload_format SET NOT NULL;

ALTER TABLE ${eventOutboxerSchema}.events ADD CONSTRAINT events_payload_exactly_one
    CHECK ((payload IS NULL) <> (payload_binary IS NULL));
