# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-05-01

Initial release. Embedded transactional outbox for Java 25 / Spring Boot 3.5.6 /
PostgreSQL 15 / Redis 7 (KeyDB 6).

### Naming

To minimise collisions with other libraries sharing the same database
or Micrometer registry, the library's defaults use a specific prefix:

- **Database schema** defaults to `event_outboxer` (configurable via
  `outbox.storage.schema`). The classpath Flyway migrations use the
  `${eventOutboxerSchema}` placeholder; the Spring Boot starter
  auto-wires it so changing the property updates both the adapter's
  SQL and the migration in lock-step. Plain-Java Flyway users pass
  the placeholder explicitly (see `docs/STORAGE.md §Configurable
  schema name`).
- **Micrometer metric prefix** defaults to `event_outboxer`
  (configurable via `outbox.metrics.prefix`). Every counter / timer /
  summary registered by `MicrometerOutboxListener` carries this
  prefix.

### Added

**Public API (`event-outboxer-api`)**

- `OutboxEventPublisher` with `publish`, `publishAll`, `publishIfAbsent`
  (`publishIfAbsent` is a documented post-MVP extension point — currently
  throws `UnsupportedOperationException`).
- `EventHandler<T>` with `extractLockKey(payload)` and optional
  per-handler `failureHandler()` override.
- Sealed `EventOutcome` (`Success` / `Retry` / `Fail` / `Skip`) and
  `FailureDecision` (`RetryAt` / `Disable` / `Delete`).
- `OutboxListener` observability surface (22 callbacks, record-based
  payloads).
- Built-in `FailureHandler<T>` decorators: log, max-retries,
  exponential-backoff, fixed-delay, no-retry, plus
  `FailureHandlerBuilder` and `FailureHandlers.defaults()`. Listener
  callbacks for retry / disable / delete are emitted by the engine
  dispatcher after the decision is persisted — no listener-forwarding
  decorator exists in the chain (see ADR-0007 §Q25).
- Domain value objects (`Event`, `PendingEvent`, `ClaimedEvent`,
  `WorkerId`, `WorkerInfo`) and a typed exception hierarchy with
  `OUTBOX-XXX` message codes.

**SPI (`event-outboxer-spi`)**

- `EventStore`, `WorkerRegistry`, `EntityLocker`, `EventSerializer`,
  `Clock`, `ConnectionSupplier` ports.
- Reusable abstract contract tests (`AbstractEventStoreContractTest`,
  `AbstractWorkerRegistryContractTest`,
  `AbstractEntityLockerContractTest`) published as a test-jar.

**Core engine (`event-outboxer-core`, Spring-free)**

- `OutboxEngine` + `OutboxEngineBuilder` — per-event-type poller, handler
  dispatcher, in-flight registry, watchdog.
- Per-type `AdaptiveWaiter` polling, `FailureHandlerResolver`, default
  retry chain, `DispatcherConfig`, `EventTypeConfig`,
  `MaintenanceConfig`.
- Maintenance: heartbeat, orphan recovery, stuck-handler watchdog on a
  shared `ScheduledExecutorService`.
- `DefaultOutboxEventPublisher` with `TransactionContext` port and
  `NoTransactionPolicy` (FAIL / IGNORE).
- `OutboxListenerRegistry` with exception isolation, built-in
  `LoggingOutboxListener`.
- Classpath enforcement: the module must not depend on Spring or
  `jakarta.persistence.*`.

**Storage adapters**

- `event-outboxer-storage-postgres` — PostgreSQL 15+ backend: minimalist
  internal JDBC runner, `ConnectionSupplier`-driven, CTE `FOR UPDATE
  SKIP LOCKED` claim query, optimistic-lock finalize, optional archive
  table, cached metrics snapshot. Flyway migrations shipped as
  classpath resources (`classpath:db/migration/outbox/core` +
  `archive`).
- `event-outboxer-storage-inmemory` — thread-safe in-JVM implementation
  of every SPI port; intended for tests and local development.

**Adapter implementations**

- `event-outboxer-serializer-jackson` — `EventSerializer` + a shared
  `JacksonObjectMapperFactory.defaults()` (JavaTime + Jdk8 +
  ParameterNames modules, ISO-8601 dates, strict deserialisation).
- `event-outboxer-lock-postgres` — session-scoped `pg_advisory_lock`
  with SHA-256 key hashing; the handle owns its connection so HikariCP
  hand-off does not leak locks.
- `event-outboxer-lock-redis` — `SET NX PX` with UUID fencing tokens;
  Lua compare-and-delete unlock script. Works against KeyDB 6+ too.
- `event-outboxer-metrics-micrometer` — full Micrometer implementation
  of `OutboxListener` (counters, timers, per-type tags).

**Spring Boot 3.5.6 starter (`event-outboxer-spring-boot-starter`)**

- `@ConfigurationProperties("outbox.*")` binding for the full YAML
  surface documented in `docs/CONFIGURATION.md`.
- Storage / lock / serializer auto-configurations; `@ConditionalOn*`
  selection by property + classpath + bean availability.
- `TransactionAwareDataSourceProxy`-backed `ConnectionSupplier` so
  `@Transactional publish()` participates in the caller's transaction
  (ADR-0002).
- `SpringTransactionContext` backed by
  `TransactionSynchronizationManager.isActualTransactionActive()`.
- `OutboxSmartLifecycle` (phase 20000, auto-startup, graceful stop).
- `OutboxHealthIndicator` for Spring Boot Actuator.
- `OutboxProbeGroupsEnvironmentPostProcessor` — opt-in integration
  with Actuator probe groups via `outbox.health.probe-groups`, for
  k8s deployments that hit only `/actuator/health/liveness` and
  `/readiness`. Merges `outbox` into the configured groups while
  preserving the default `<group>State` contributor.
- `event_outboxer.engine.state{state=stopped|running|stopping}`
  Micrometer gauges for metric-based alerting on engine liveness
  without touching the probe groups.
- **Engine crash detection** for poller threads.
  `EngineHealthCheckTask` runs every
  `outbox.maintenance.watchdog-interval` and inspects each per-type
  poller's thread for unexpected termination (uncaught `Error` that
  bypassed the `Poller.tick()` exception filter). On first
  detection: `OutboxEngine.markCrashed(...)` flips `state()` to
  `STOPPED`, the health indicator flips DOWN (propagating into
  configured probe groups), the `engine.state` gauge flips to
  `stopped=1`, and the new 22nd listener callback
  `OutboxListener.onEngineCrashed(EngineCrashedInfo)` fires. New
  `event_outboxer.engine.crashed` counter increments on each crash.
  Spring's `isRunning()` stays `true` so normal cleanup (worker
  deregister, handler drain) still runs on context close. Out of
  scope in `0.1.0`: detection of stuck-but-alive threads and
  maintenance-scheduler-death.
- `OutboxEngine.isLifecycleActive()` — the
  started-but-not-yet-stopped window; used by
  `OutboxSmartLifecycle.isRunning()` so a crashed engine still gets
  `stop()` called on context close.
- `OutboxEngineBuilder.pollStrategy(...)` is now wired through the
  starter's `OutboxEngineAutoConfiguration` — users can register a
  `PollStrategy` bean to override the default
  `LockAndFetchStrategy`.
- `HandlerExecutorFactory` with platform and virtual-thread modes (JEP
  491 makes virtual threads safe on JDK 25). Platform variant uses
  Spring's `ThreadPoolTaskExecutor` with
  `ContextPropagatingTaskDecorator` so MDC, Micrometer Observation
  and Spring Security context propagate from the poller thread to
  the handler thread (ADR-0009). Virtual variant wraps
  `Executors.newThreadPerTaskExecutor(...)` in a thin
  `ContextPropagatingExecutorService` so the same decorator applies.
  A `SpringTaskExecutorAdapter` routes `execute()`/`submit()` through
  `ThreadPoolTaskExecutor` (so decoration runs) while delegating
  lifecycle calls to the underlying `ThreadPoolExecutor`.
- `io.micrometer:context-propagation` is now a direct compile dep of
  the starter (required at runtime by
  `ContextPropagatingTaskDecorator`, which Spring pulls only
  optionally via `micrometer-observation`).

**Test support (`event-outboxer-testkit`)**

- `SettableClock` for deterministic time-travel.
- `ManualEngine` — synchronous `tick()` driver for step-through tests.
- `OutboxTestContext` one-stop builder (in-memory adapter + Jackson
  defaults + recording listener).
- `RecordingOutboxListener` capturing every callback.
- AssertJ-style `EventAssertions.assertThatStore(store)` with fluent
  event-level assertions.
- JUnit 5 `OutboxExtension` that injects a fresh `OutboxTestContext`
  per test.

**Documentation**

- Full `docs/` suite: `ARCHITECTURE.md`, `CONFIGURATION.md`,
  `STORAGE.md`, `GLOSSARY.md`.
- 18 ADRs under `docs/adr/` capturing every significant design
  decision, including:
  - ADR-0001: local/embedded outbox scope;
  - ADR-0002: participate in the client's transaction;
  - ADR-0004: per-event-type worker isolation;
  - ADR-0005: workers/heartbeat table;
  - ADR-0014: optimistic locking via `version`;
  - ADR-0015: at-least-once semantics;
  - ADR-0016: Maven module structure;
  - ADR-0017: Java 25 + Spring Boot 3.5 baseline;
  - ADR-0018: JSpecify for nullness annotations.

**Build / CI**

- Maven Wrapper pinned to Maven 3.9.12.
- GitHub Actions: `ci.yml` (unit + `-P it` integration on
  Testcontainers), `release.yml` (tag-triggered deploy through
  Sonatype Central Publishing).
- `maven-enforcer-plugin` rules: JDK 25, Maven 3.9+, dependency
  convergence, ban Spring from `event-outboxer-core`.

### Known limitations

- `publishIfAbsent(...)` throws `UnsupportedOperationException`; dedup
  with an idempotency key is planned post-MVP.
- Liquibase mirror of the migrations is deferred — users on Liquibase
  copy the SQL from `docs/STORAGE.md` into their own migration set.
- LISTEN/NOTIFY fast-wake path for the PG adapter (ADR-0006) is not
  implemented in 0.1.0; polling is the only claim mechanism.

### Security

- No known CVEs at release time. Dependencies tracked via Spring Boot
  `spring-boot-dependencies` BOM; patch releases will follow
  upstream advisories.

[0.1.0]: https://github.com/bams22/event-outboxer/releases/tag/v0.1.0
