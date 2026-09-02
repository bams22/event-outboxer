# ADR-0028: Starter-managed Flyway instance for the outbox schema

## Status

Accepted.

## Date

2026-08-28

## Context

Up to 0.4.0 the library shipped its PostgreSQL migrations as classpath
resources under `db/migration/outbox/{core,archive,lock}` and asked
users to append those directories to `spring.flyway.locations`, so the
*application's* Flyway instance applied them next to the application's
own migrations. Two properties of Flyway made that arrangement fail on
first contact:

1. **Locations are scanned recursively, and sub-locations are
   discarded.** Almost every Spring Boot application keeps
   `classpath:db/migration` in its locations; Flyway then already sees
   `db/migration/outbox/**`, logs `Discarding location
   'classpath:db/migration/outbox/core' as it is a sub-location of
   'classpath:db/migration'`, and applies **every** outbox lane on the
   classpath — the documented opt-in of `archive/` and `lock/` through
   separate locations never actually happened.
2. **Version numbers share one sequence per history table.** The
   library's `V001…V007` collided with the application's own `V1`/
   `V001`; the shipped example (`V001__orders.sql`) died at startup with
   `Found more than one migration with version 001`.

Both were confirmed against Flyway 9.22 and 11.7. The root cause is
structural: a library cannot safely share a Flyway history table (and
therefore a version sequence) with the application that embeds it.

## Alternatives considered

- **Keep sharing the application's instance, renumber with
  timestamps** (`V20260101_001__outbox_core.sql`). Removes the
  collision but not the recursive-scan problem, still requires users
  to edit `spring.flyway.locations`, and still couples the outbox
  schema's history to the application's table (a `flyway clean` or
  repair on the application side touches outbox rows).
- **Ship plain DDL only, no migration tooling** (db-scheduler style).
  Simplest for the library, but every user re-invents ordering and
  upgrade tracking; the archive / lock lanes and the `payload_format`
  upgrade already require sequencing.
- **Run DDL from the storage adapter at startup** (`CREATE TABLE IF NOT
  EXISTS`). Breaks the "adapters never issue DDL" stance, cannot
  express `ALTER`-style upgrades safely, and hides schema changes from
  the operator's migration pipeline.
- **A dedicated Flyway instance owned by the starter** — chosen.

## Decision

1. **Migrations move out of `db/migration/`.** They now live under
   `classpath:event-outboxer/migration/{core,archive,lock}` (header
   comments refreshed; the DDL itself is unchanged). Nothing the
   application's Flyway scans by default can reach them.
2. **The starter runs its own Flyway instance**
   (`OutboxFlywayAutoConfiguration`) when Flyway and the PostgreSQL
   adapter are on the classpath and `event-outboxer.storage.type=postgres`:
   - locations are **fixed** — core and archive always (both ship in
     `event-outboxer-storage-postgres`), lock whenever
     `event-outboxer-lock-postgres-lease` is present. Nothing is read
     from, or needs to be written to, `spring.flyway.locations`;
   - `schemas` = `event-outboxer.storage.schema`, so the instance
     creates the schema when missing and keeps its own
     `flyway_schema_history` **inside** it — the application's
     `public.flyway_schema_history` and version sequence are untouched;
   - the `${eventOutboxerSchema}` placeholder is set from the same
     property;
   - `outOfOrder=true`: the lanes touch disjoint tables, so a lane
     adopted later (the lease module added after core migrations ran)
     applies without a validation error;
   - connection: the outbox `DataSource` (ADR-0024 resolution,
     transaction-aware proxy unwrapped) by default;
     `event-outboxer.flyway.url` / `user` / `password` /
     `driver-class-name` build a dedicated `SimpleDriverDataSource`
     instead — the same precedence Boot gives `spring.flyway.url`;
     `user` without `url` derives a connection from the outbox
     `DataSource` with the given credentials;
   - `event-outboxer.flyway.enabled=false` opts out (Liquibase users,
     externally managed DDL).
3. **No `Flyway` or `FlywayMigrationInitializer` bean is exposed.**
   Boot's own `flyway` / `flywayInitializer` beans are
   `@ConditionalOnMissingBean`; sharing their types would make the
   application's migrations silently disappear. The starter's
   `OutboxFlywayMigrationInitializer` is a distinct type, registered as
   a database initializer through a `DatabaseInitializerDetector`
   (`spring.factories`) so `@DependsOnDatabaseInitialization` beans
   (the lease-table probe) and JDBC consumers are created after the
   outbox schema is migrated.
4. **Upgrade path from ≤ 0.4.0** is explicit, one-time and diagnosed:
   `event-outboxer.flyway.baseline-on-migrate=true` +
   `baseline-version=<highest outbox migration already applied>`
   records the existing objects; a `relation already exists` failure
   on a schema without its own history is rethrown with that recipe
   instead of the raw SQL error.

## Rationale

A separate history table per schema owner is how Flyway itself
recommends multi-module setups, and it is what Boot users already do
by hand for multi-tenant or multi-datasource applications. It removes
every point of contact between the two migration sets: no shared
version space, no shared locations, no shared history rows, no shared
credentials. Fixing the locations in code rather than properties is
deliberate — the set of migrations is a property of the jars on the
classpath, not of the deployment, and exposing it invites the exact
misconfiguration this ADR retires.

The migration files are moved rather than duplicated: keeping copies
under `db/migration/outbox/**` "for compatibility" would recreate the
recursive-scan collision for everyone.

## Consequences

### Users of the library

- Quick start shrinks: no `spring.flyway.locations` edits, no
  placeholder wiring; `flyway-core` + `flyway-database-postgresql` on
  the classpath is enough.
- Adding `event-outboxer-lock-postgres-lease` (or turning on
  `archive-enabled`) needs no migration bookkeeping — the lane is
  applied on the next start.
- A DDL role separate from the runtime role is a three-property
  change (`event-outboxer.flyway.url` / `user` / `password`).
- **Breaking** for ≤ 0.4.0 installations: remove the outbox locations
  from `spring.flyway.locations`, tell the application instance to
  ignore the now-missing rows (`spring.flyway.ignore-migration-patterns:
  "*:missing"` or delete them), and baseline the outbox instance once
  (recipe in the CHANGELOG). Users who prefer to keep applying the SQL
  through their own pipeline set `event-outboxer.flyway.enabled=false`
  and point their tool at the new locations.
- The archive lane is now always applied (an empty `event_archive`
  table when `archive-enabled=false`); the flag only governs runtime
  behaviour.

### Library maintainers

- New migrations go under `event-outboxer/migration/<lane>/` and keep
  the single shared version sequence; lanes must stay disjoint in the
  tables they touch (the `outOfOrder` guarantee depends on it).
- Once a release has users, shipped SQL is frozen by checksum — path
  moves are fine, edits are not. (The header comments were refreshed
  in this change while no released installation depended on them.)
- The `FlywayConfigurationCustomizer` that feeds the placeholder into
  the application's instance stays, for the `enabled=false` path.

### Operations

- Two history tables: `public.flyway_schema_history` (application)
  and `<outbox schema>.flyway_schema_history` (outbox). Boot's
  `/actuator/flyway` endpoint lists only the application's instance;
  the outbox instance logs applied migrations at INFO on start.
- Migrations run during context refresh, before the engine's
  `SmartLifecycle` start; a failure aborts startup with the Flyway
  error (or the baseline recipe).

## Related decisions

- [ADR-0020](0020-no-inmemory-storage-in-production.md) — PostgreSQL
  is the only durable storage; the instance activates only for it.
- [ADR-0022](0022-lease-table-postgres-entity-locker.md) — the lock
  lane (V005) and the fail-fast table probe this ADR orders after the
  migration.
- [ADR-0024](0024-outbox-datasource-selection.md) — how the outbox
  `DataSource` is chosen when the instance has no dedicated URL.
- [ADR-0027](0027-starter-managed-redis-connection.md) — the same
  "starter owns the infrastructure it needs" stance for Redis.
