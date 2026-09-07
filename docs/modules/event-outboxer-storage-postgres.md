# event-outboxer-storage-postgres

The production storage adapter: PostgreSQL 15+ implementations of the
`EventStore`, `WorkerRegistry` and `OutboxAdmin` SPI ports, built on
`SELECT … FOR UPDATE SKIP LOCKED` claim semantics and plain JDBC.
Spring-free — all connections go through the `ConnectionSupplier` SPI
port, so the same jar serves plain-Java and Spring setups.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-storage-postgres` |
| Java package | `io.github.bams22.outboxer.storage.postgres.*` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `org.postgresql:postgresql`, `slf4j-api` (`flyway-core` optional) |
| Requires | PostgreSQL **15+** (partial indexes, JSONB, CTE-in-UPDATE) |
| Spring / core | None — `event-outboxer-core` is banned by the enforcer |

## Why it exists

The engine is storage-agnostic ([ADR-0010](../adr/0010-storage-agnostic-core-via-spi.md));
this module is the durable backend that makes the outbox actually
transactional: events are INSERTed through the caller's JDBC
connection, so they commit or roll back together with the business
data ([ADR-0002](../adr/0002-participate-in-client-transaction.md)).
It is the production default — the in-memory adapter is test-only
infrastructure ([ADR-0020](../adr/0020-no-inmemory-storage-in-production.md)).

## What it does

### Public classes

| Class | Responsibility |
|---|---|
| `PostgresEventStore` | `EventStore`: save, claim (with the dedup sweep), finalize, reclaim, sweep, metrics snapshot. All SQL precomputed in the constructor. |
| `PostgresWorkerRegistry` | `WorkerRegistry`: register / heartbeat / findDead / removeDead over `event_outboxer.workers`. |
| `PostgresOutboxAdmin` | `OutboxAdmin` ([ADR-0019](../adr/0019-admin-and-retention-surface.md)): list by status (keyset pagination), archive lookup, re-enable, purge, replay-from-archive ([ADR-0033](../adr/0033-archive-dedup-key-and-replay-from-archive.md)). Takes the same `MetricsSnapshotCache` as the store and invalidates it after a mutation that changed rows. |
| `PostgresStorageProperties` | Plain record (`schema`, `tablePrefix`, `archiveEnabled`, `metricsCacheTtl`); `defaults()` = `event_outboxer` / `""` / `false` / `30s`. No Spring annotations. |
| `SchemaResolver` | Builds fully-qualified table names once (`<schema>.<prefix>events`, `…workers`, `…event_archive`). |

Classes under `…storage.postgres.internal` (`OutboxJdbcRunner`,
`JsonbHandler`, `FlatMapJson`, `EventRows` — the column lists and row
mappers the store and the admin share) are not public API and may
change between minor versions.

### Key behaviors

- **Transaction participation.** Every statement runs on a connection
  from `ConnectionSupplier.get()`. Inside a caller's transaction the
  supplier returns that transaction's connection and local-transaction
  wrapping is skipped, so `publish()` is atomic with the business
  write. The Spring starter wires this to
  `DataSourceUtils.getConnection` on a `TransactionAwareDataSourceProxy`.
- **Claim** is a single CTE + `UPDATE … RETURNING` statement with
  `FOR UPDATE SKIP LOCKED`, ordered `priority DESC, run_at`, served by
  the partial index `idx_events_ready` — concurrent replicas never
  block each other.
- **Optimistic locking** ([ADR-0014](../adr/0014-optimistic-locking-via-version-field.md)):
  every finalize statement is guarded by
  `WHERE id=? AND version=? AND claimed_by=? AND status='PROCESSING'`
  and reports whether the guard matched. Batch forms
  (`markProcessedAll` / `markForRetryAll`, used by the engine's group
  commit) return the per-row verdict via `RETURNING`.
- **Archive** (opt-in, [ADR-0008](../adr/0008-three-statuses-plus-optional-archive.md)):
  with `archive-enabled`, `markProcessed` becomes one atomic
  `WITH del AS (DELETE … RETURNING …) INSERT INTO event_archive …`
  statement (carrying `dedup_key` since V008). The reverse move —
  `replayFromArchive` — is the mirror CTE, insert-first, so a row the
  INSERT skips (its id is live) keeps the archive row
  ([ADR-0033](../adr/0033-archive-dedup-key-and-replay-from-archive.md)).
- **Dedup coalescing** ([ADR-0037](../adr/0037-claim-time-dedup-coalescing.md)):
  the insert is unconditional; the claim statement elects one
  representative per `(event_type, dedup_key)` among the rows it
  locked and sweeps the other due `PENDING` rows of those keys — in the
  batch and beyond it, up to 1 000 per claim, `FOR UPDATE SKIP LOCKED`
  so another worker's rows are skipped, never waited on — deleting
  them (or archiving them as `coalesced at claim`) in the same
  statement and returning their ids and carriers per representative.
  V004 creates the plain partial index the sweep uses.
- **Dual payload lane** ([ADR-0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md)):
  text payloads land in `payload JSONB`, binary payloads in
  `payload_binary BYTEA` — exactly one is non-null (CHECK constraint)
  — and `payload_format` records which serializer wrote the row, so
  reads route to the right deserializer.
- **DB-clock liveness.** `heartbeat()` and the stale-claim sweep stamp
  and compare the *database* clock (`now()`), so worker liveness is
  immune to JVM clock skew.

The full DDL, index rationale and every query are documented in
[STORAGE.md](../STORAGE.md).

### Schema and migrations — shipped, applied by the starter

The module never issues DDL at runtime. It ships parameterized SQL
(placeholder `${eventOutboxerSchema}`) as classpath resources —
outside `db/migration/`, so an application Flyway instance never scans
them (ADR-0028):

| Location | Contents | Applied by the starter? |
|---|---|---|
| `event-outboxer/migration/core` | V001 (`events`, `workers`), V003 (admin index), V004 (dedup key + sweep index), V006 (payload format) | always |
| `event-outboxer/migration/archive` | V002, V007, V008, V009 (`event_archive`) | always (`storage.archive-enabled` only governs runtime) |
| `event-outboxer/migration/lock` | V005 (`entity_locks`) | when [`event-outboxer-lock-postgres-lease`](event-outboxer-lock-postgres-lease.md) is on the classpath |
| `db/changelog/outbox/{core,archive}/changelog.xml` | Liquibase changelogs delegating to the same SQL files | no — for `event-outboxer.flyway.enabled=false` setups |

The Spring Boot starter runs a **dedicated Flyway instance** for these
locations with its own history table inside `storage.schema` (see
[STORAGE.md §Migrations](../STORAGE.md#migrations-flyway) and
[`event-outboxer.flyway.*`](../CONFIGURATION.md#event-outboxerflyway)).

## When to use it

Whenever your service runs on PostgreSQL 15+ — this is the production
default and currently the only durable adapter. Do not use
`event-outboxer-storage-inmemory` outside tests. If your outbox lives
in a different database than your default `DataSource`, see
[`@OutboxDataSource`](../CONFIGURATION.md#selecting-the-datasource-outboxdatasource)
([ADR-0024](../adr/0024-outbox-datasource-selection.md)).

## How to use it

### With Spring Boot (typical)

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-spring-boot-starter</artifactId>
</dependency>
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-storage-postgres</artifactId>
</dependency>
```

```yaml
event-outboxer:
  storage:
    type: postgres            # required — there is no default (ADR-0020)
    # schema: event_outboxer  # default; used by the SQL, the migrations and their history table
    # table-prefix: ""
    # archive-enabled: false
    # metrics-cache-ttl: 30s
  # flyway:                   # starter-managed instance — nothing to add to spring.flyway.locations
  #   url: jdbc:postgresql://db:5432/orders   # optional dedicated DDL connection
  #   user: outbox_migrator
  #   password: ${OUTBOX_MIGRATOR_PASSWORD}
```

`flyway-core` and `flyway-database-postgresql` on the classpath are
enough for the schema: the starter's `OutboxFlywayAutoConfiguration`
migrates it before the engine starts. `PostgresStorageAutoConfiguration`
activates on `event-outboxer.storage.type=postgres` + a `DataSource`
bean and registers `outboxConnectionSupplier` (transaction-aware,
ADR-0002), `outboxEventStore`, `outboxWorkerRegistry`, `outboxAdmin`,
and — for the `event-outboxer.flyway.enabled=false` path — a
`FlywayConfigurationCustomizer` / Liquibase environment post-processor
that feed `event-outboxer.storage.schema` into the
`${eventOutboxerSchema}` placeholder of the application's instance
(your own placeholder value wins on conflict).

### Without Spring

Implement `ConnectionSupplier` over your pool, run the migrations
yourself, construct directly:

```java
Flyway.configure()
    .dataSource(dataSource)
    .locations(
        "classpath:event-outboxer/migration/core",
        "classpath:event-outboxer/migration/archive")
    .placeholders(Map.of("eventOutboxerSchema", "event_outboxer"))
    .load()
    .migrate();

MetricsSnapshotCache metricsCache =
    MetricsSnapshotCache.inMemory(Clock.system(), Duration.ofSeconds(30));

EventStore store = new PostgresEventStore(
    connectionSupplier, PostgresStorageProperties.defaults(), Clock.system(), metricsCache);

WorkerRegistry registry =
    new PostgresWorkerRegistry(connectionSupplier, PostgresStorageProperties.defaults());

// Optional: the admin surface and the retention task. Same cache instance as the store,
// so an admin mutation is visible in metricsSnapshot() at once.
OutboxAdmin admin =
    new PostgresOutboxAdmin(connectionSupplier, PostgresStorageProperties.defaults(), metricsCache);
```

Then pass the store and the registry (and the admin, for retention) into
`OutboxEngineBuilder` (see
[core](event-outboxer-core.md#usage-without-spring)). For the caller's
transaction to include `publish()`, your `ConnectionSupplier` must
return the transaction's connection when one is active.

If you call an admin mutation from inside your own transaction, invoke
`metricsCache.invalidate()` again once you have committed: the admin
invalidates when its statement runs, and until the commit the changed
rows are invisible to the scrape that recomputes the snapshot. The
Spring Boot starter does this for you (it defers the invalidation to
after-commit for the admin bean it wires).

### pgBouncer note

Polling, claim, finalize and heartbeat carry no session state and are
safe behind transaction pooling. Mind pgJDBC's server-side prepared
statements on pgBouncer < 1.21 (`prepareThreshold=0` or upgrade) and
avoid the advisory locker — see
[CONFIGURATION.md §Running behind pgBouncer](../CONFIGURATION.md#running-behind-pgbouncer).

## Related

- [STORAGE.md](../STORAGE.md) — full schema, all queries, vacuum and monitoring guidance.
- Lockers sharing this database: [postgres-lease](event-outboxer-lock-postgres-lease.md), [postgres-advisory](event-outboxer-lock-postgres-advisory.md).
- ADRs: [0002](../adr/0002-participate-in-client-transaction.md), [0008](../adr/0008-three-statuses-plus-optional-archive.md), [0014](../adr/0014-optimistic-locking-via-version-field.md), [0021](../adr/0021-dedup-key-single-inflight-per-key.md), [0024](../adr/0024-outbox-datasource-selection.md), [0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md).
