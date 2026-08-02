-- ADR-0025: mirror the dual payload lane + payload_format in the archive table.
-- See V006__outbox_payload_format.sql for rationale.

ALTER TABLE ${eventOutboxerSchema}.event_archive ALTER COLUMN payload DROP NOT NULL;

ALTER TABLE ${eventOutboxerSchema}.event_archive ADD COLUMN payload_binary BYTEA;

ALTER TABLE ${eventOutboxerSchema}.event_archive ADD COLUMN payload_format VARCHAR(64);

UPDATE ${eventOutboxerSchema}.event_archive SET payload_format = 'jackson-json' WHERE payload_format IS NULL;

ALTER TABLE ${eventOutboxerSchema}.event_archive ALTER COLUMN payload_format SET NOT NULL;

ALTER TABLE ${eventOutboxerSchema}.event_archive ADD CONSTRAINT event_archive_payload_exactly_one
    CHECK ((payload IS NULL) <> (payload_binary IS NULL));
