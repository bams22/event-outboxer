# event-outboxer-spring-boot-starter

The Spring Boot 3.5+ integration: auto-configuration for every module,
`event-outboxer.*` property binding, transaction integration
(`publish()` joins the caller's transaction), `SmartLifecycle`
start/drain, health indicator and probe-group wiring. The starter
**only wires — it never adds semantics**: everything that affects
correctness lives in [core](event-outboxer-core.md)
(see [ARCHITECTURE.md §Feature parity](../ARCHITECTURE.md#7-feature-parity-starter-vs-plain-core)).

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-spring-boot-starter` |
| Java package | `io.github.bams22.outboxer.spring.*` |
| Brings transitively | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), [`event-outboxer-core`](event-outboxer-core.md) |
| Optional (you add what you use) | every adapter module: storage, serializers, locks, cache, metrics, tracing |
| Requires at runtime | a storage adapter + `DataSource`, and a serializer (Jackson module or a custom `EventSerializer` bean) |

## Why it exists

Assembling the engine by hand means wiring a dozen collaborators and
keeping their invariants straight. The starter derives all of it from
the classpath, the bean context and `event-outboxer.*` properties —
and where configuration is wrong it **fails fast with an actionable
message** (unset storage type, ambiguous `DataSource`, missing lease
migration, unresolvable write serializer) instead of degrading
silently.

## What it does

### Engine assembly

`OutboxEngineAutoConfiguration` (gated on `event-outboxer.enabled`,
default `true`) registers, each behind `@ConditionalOnMissingBean`:
`Clock`, `SpringTransactionContext`, `PollerWakeHub`, `WorkerId`,
`OutboxSerializers` (write/read serializer resolution),
`OutboxEventPublisher`, `OutboxEngine` and `OutboxSmartLifecycle`.

Beans it *collects* rather than creates:

- every **`EventHandler<?>`** bean → registered with the engine
  (duplicate `eventType()` fails startup);
- every **`OutboxListener`** bean → fanned out from engine and
  publisher (a throwing listener is isolated, it cannot poison a
  publish);
- every **`EventSerializer`** bean → the deserialization registry.

### Transaction integration ([ADR-0002](../adr/0002-participate-in-client-transaction.md))

The storage auto-configuration wraps the resolved `DataSource` in a
`TransactionAwareDataSourceProxy` and exposes a `ConnectionSupplier`
backed by `DataSourceUtils.getConnection/releaseConnection`. Inside
`@Transactional` code the outbox INSERT therefore shares the caller's
connection — commit and rollback are atomic with the business writes.
`SpringTransactionContext` additionally drives
`publisher.no-transaction-policy` (default `FAIL`) and registers the
poller wake-up as an *after-commit* synchronization, so a locally
published event is picked up milliseconds after commit and never on
rollback.

The PG lockers get the **unwrapped** raw `DataSource` — their
acquire/release must run autocommit outside the caller's transaction
([ADR-0022](../adr/0022-lease-table-postgres-entity-locker.md)).

### DataSource selection ([ADR-0024](../adr/0024-outbox-datasource-selection.md))

One `DataSource` bean — nothing to do. Several — mark the one holding
the outbox tables:

```java
@Bean @OutboxDataSource
public DataSource ordersDataSource() { ... }
```

Resolution (identical for storage, both PG lockers and the lease
probe): `@OutboxDataSource`-qualified bean (beats `@Primary`) →
unique/`@Primary` bean → fail fast naming the candidates (rendered by
a dedicated `FailureAnalyzer`).

### Lifecycle

`OutboxSmartLifecycle` at **phase 20 000** — after DataSource, pools
and Flyway; stopped before them on shutdown. `stop()` runs the
graceful drain (stop pollers → drain handler executors → release
still-claimed events without burning attempts → `graceful_stop` flag →
stop maintenance → deregister), bounded by
`event-outboxer.maintenance.shutdown-timeout` (default 30 s — raise it
if handlers legitimately run longer). A crashed engine still reports
`isRunning()` so Spring always calls `stop()` and cleanup happens.

### Handler executors ([ADR-0009](../adr/0009-spring-task-executor-in-starter.md))

`event-outboxer.handler-executor.type`:

- `platform` (default) — a fixed-size Spring `ThreadPoolTaskExecutor`
  per event type (`core == max == handler-pool-size`, bounded queue,
  abort policy);
- `virtual` — virtual-thread-per-task executor, pin-free with
  `synchronized`-heavy JDBC drivers (JEP 491, Java 25 baseline).

Both apply `ContextPropagatingTaskDecorator` by default, so MDC,
Micrometer Observation and the Security context reach handler threads;
declare your own `TaskDecorator` bean to replace it.

### Observability

- **`OutboxHealthIndicator`** → `/actuator/health/outbox` (`UP` =
  engine `RUNNING` + metrics snapshot reachable; details carry state,
  backlog totals, workerId). `event-outboxer.health.probe-groups:
  [readiness]` merges it into Kubernetes probe groups via an
  `EnvironmentPostProcessor`.
- **Micrometer** — with
  [metrics-micrometer](event-outboxer-metrics-micrometer.md) present:
  the listener, `…engine.state{state=…}` gauges and per-type backlog
  gauges.
- **Tracing** — auto-detects
  [tracing-micrometer](event-outboxer-tracing-micrometer.md) (Boot
  `ObservationRegistry`+`Tracer`+`Propagator` beans; wins when both
  present) then [tracing-otel](event-outboxer-tracing-otel.md);
  switch: `event-outboxer.tracing.enabled`.

### Migration plumbing

`event-outboxer.storage.schema` flows automatically into the Flyway
placeholder `${eventOutboxerSchema}` (a `FlywayConfigurationCustomizer`)
and the Liquibase parameter
`spring.liquibase.parameters.eventOutboxerSchema` (an
`EnvironmentPostProcessor`) — your own values win on conflict. You
still list the migration locations explicitly (see below).

## When to use it

Any Spring Boot 3.5+ application — this is the intended entry point.
Go [core + builder](event-outboxer-core.md#usage-without-spring) only
outside Spring.

## How to configure it

### Minimal production setup

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-spring-boot-starter</artifactId>
</dependency>
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-storage-postgres</artifactId>
</dependency>
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-lock-postgres-lease</artifactId>   <!-- if handlers use extractLockKey -->
</dependency>
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-metrics-micrometer</artifactId>    <!-- recommended -->
</dependency>
```

```yaml
event-outboxer:
  storage:
    type: postgres        # required — no default (ADR-0020)
  lock:
    type: postgres-lease  # only if you use entity locking; default is noop
```

The outbox schema is migrated by the starter's own Flyway instance
(`flyway-core` + `flyway-database-postgresql` on the classpath, ADR-0028)
— nothing to add to `spring.flyway.locations`; a dedicated DDL
connection is `event-outboxer.flyway.url` / `user` / `password`.

Then inject `OutboxEventPublisher` into `@Transactional` services and
declare `EventHandler` beans — see the
[runnable example](../../examples/spring-boot-postgres/) and the
README quick start.

### Property tree

Everything lives under `event-outboxer.*`; the complete reference with
defaults and startup-validated invariants is
[CONFIGURATION.md](../CONFIGURATION.md). Orientation map:

| Section | Governs |
|---|---|
| `publish-only` | `true` = no pollers on this instance, `EventHandler` beans optional / ignored ([ADR-0029](../adr/0029-publish-only-is-explicit.md)) |
| `storage.*` | adapter selection, schema, archive, metrics-cache TTL |
| `flyway.*` | the starter-managed Flyway instance: on/off, dedicated connection, one-time baseline ([ADR-0028](../adr/0028-starter-managed-flyway-instance.md)) |
| `lock.*`, `cache.*` | `EntityLocker` / `MetricsSnapshotCache` backend selection |
| `serializer.*` | write format + per-type overrides ([ADR-0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md)) |
| `event-types.defaults` / `.overrides.<TYPE>` | per-type polling, pool, runtime and lock-TTL knobs — **thin merge**: overrides change only the fields they set |
| `dispatcher.*` | unknown-handler policy, contention retry delays, finalize batching |
| `maintenance.*` | heartbeat, dead threshold, recovery/watchdog cadence, shutdown timeout |
| `handler-executor.type` | platform vs virtual threads |
| `publisher.no-transaction-policy` | `FAIL` (default) / `IGNORE` |
| `metrics.*`, `tracing.*`, `health.probe-groups` | observability |
| `retention.*` | opt-in archive/`DISABLED` cleanup (off by default) |

Note: **retry/backoff policy is configured in Java, not YAML** —
`FailureHandler` beans (below), per ADR-0007.

### Customization points

Every port is `@ConditionalOnMissingBean` — declare your own bean and
the starter backs off:

| To customize | Provide |
|---|---|
| Jackson mapper | `@Bean("outboxObjectMapper") ObjectMapper` (else primary mapper, else library defaults) |
| Serialization format | any `EventSerializer` bean(s); name one `outboxEventSerializer` or set `serializer.write-format` |
| Global failure chain | `@Bean("outboxDefaultFailureHandler") FailureHandler<?>` |
| Per-type failure chains | `@Bean("outboxPerTypeFailureHandlers") Map<String, FailureHandler<?>>` (or `EventHandler.failureHandler()`) |
| Context propagation | `TaskDecorator` bean |
| Locking / storage / cache / tracing | `EntityLocker`, `EventStore`, `WorkerRegistry`, `ConnectionSupplier`, `OutboxAdmin`, `MetricsSnapshotCache`, `OutboxTracer` beans |
| Outbox schema migration | `OutboxFlywayMigrationInitializer` bean (or `event-outboxer.flyway.enabled=false` + your own pipeline) |
| Time (tests) | `Clock` bean |
| Worker identity | `event-outboxer.worker.id` or a `WorkerId` bean |

Worked snippets for each are in
[CONFIGURATION.md §Overriding through Java code](../CONFIGURATION.md#overriding-through-java-code).

### Testing

- DB-less Spring tests: `@Import(OutboxInMemoryTestConfiguration.class)`
  + [storage-inmemory](event-outboxer-storage-inmemory.md) in test scope.
- Plain-Java handler tests: [testkit](event-outboxer-testkit.md).
- Full-stack: `@SpringBootTest` + Testcontainers
  ([TESTING.md](../TESTING.md#interop-with-springboottest)).

## Failure-analysis you get for free

| Misconfiguration | What happens |
|---|---|
| `storage.type` unset / adapter jar missing / no `DataSource` | startup fails with a `FailureAnalyzer` diagnosis naming the exact fix (ADR-0020) |
| no `EventHandler` bean and `publish-only` unset | startup fails with a diagnosis naming the handler contract and `event-outboxer.publish-only=true` (ADR-0029) |
| several `DataSource`s, none qualified (or two qualified) | fail fast listing candidate beans and the `@OutboxDataSource` fix (ADR-0024) |
| `lock.type=postgres-lease` without migration V005 (only with `flyway.enabled=false`) | fail-fast table probe naming the migration, the classpath location and the escape hatch |
| outbox schema populated by a ≤ 0.4.0 install, no history table of its own | the outbox Flyway instance fails with the one-time `baseline-on-migrate` / `baseline-version` recipe |
| Flyway 10+ without `flyway-database-postgresql` | fail fast naming the artifact |
| old `lock.type=postgres` value | fails listing the valid values (`postgres-lease` / `postgres-advisory`) |
| `write-format` matching no registered serializer, or several serializers with no designated writer | fail fast listing registered formats |
| invariant violations (`dead-threshold < 3×heartbeat`, `lock-ttl < handler-max-runtime`, …) | config record constructors abort context refresh |
| `postgres-advisory` with `Σ handler-pool-size ≥ hikari max-pool-size` | startup WARNING (self-deadlock risk) |

## Related

- [CONFIGURATION.md](../CONFIGURATION.md) — the full property reference.
- [ARCHITECTURE.md §Spring integration](../ARCHITECTURE.md#spring-integration) — lifecycle phases, feature-parity table.
- [OBSERVABILITY.md](../OBSERVABILITY.md) — health, metrics, tracing, troubleshooting.
- ADRs: [0002](../adr/0002-participate-in-client-transaction.md), [0009](../adr/0009-spring-task-executor-in-starter.md), [0020](../adr/0020-no-inmemory-storage-in-production.md), [0024](../adr/0024-outbox-datasource-selection.md).
