-- Application-level migration for the starter integration tests. Its version deliberately
-- collides with the library's own V001: the application's Flyway instance (spring.flyway.*,
-- scanning db/migration) and the starter-managed outbox instance (event-outboxer/migration/*)
-- must never see each other's files.
CREATE TABLE orders (
    id          UUID   PRIMARY KEY,
    total_cents BIGINT NOT NULL
);
