-- Container init script for OutboxFlywayDedicatedConnectionIT: a separate DDL role the outbox
-- Flyway instance logs in as, distinct from the application role the engine runs under.
CREATE ROLE migrator LOGIN PASSWORD 'migrator';
GRANT CREATE ON DATABASE outboxer TO migrator;
