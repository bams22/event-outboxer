# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
- **Admin and retention surface (ADR-0019).** New `OutboxAdmin` SPI port
  (list events by status with keyset pagination, archive lookup via the new
  `ArchivedEvent` type, `reenable`/`reenableAll` with a fresh attempts
  budget, `purgeDisabled`/`purgeArchive`), implemented by both storage
  adapters and pinned by a shared contract test. Migration V003 adds a
  partial index over `DISABLED` rows. Two new opt-in surface modules:
  `event-outboxer-admin-actuator` (endpoint `outboxadmin`, standard
  Actuator exposure/security model) and `event-outboxer-admin-rest`
  (disabled by default; guarded by `@PreAuthorize` with the authority from
  `event-outboxer.admin.rest.required-authority`, failing fast at startup
  when Spring Security is present without `@EnableMethodSecurity`).
  A shipped `RetentionTask` (off by default, `event-outboxer.retention.*`)
  batch-purges the archive and old `DISABLED` rows. 15 modules total.

### Fixed
- **Payload deserialization failures are recoverable.** They now route
  through the `FailureHandler` chain (retry with backoff, `DISABLED` only
  after the attempt budget) instead of finalizing to `DISABLED` on the
  first failure with no recovery path. A rolling deploy with mixed-version
  replicas no longer permanently disables events; `onEventSerializationError`
  still fires on every failed attempt. Amends ADR-0007.

### Changed
- **Jackson defaults are evolution-friendly**, as ADR-0011 always
  prescribed (the implementation had shipped the opposite):
  `FAIL_ON_UNKNOWN_PROPERTIES` and `ADJUST_DATES_TO_CONTEXT_TIME_ZONE`
  are now disabled, `FAIL_ON_NULL_FOR_PRIMITIVES` is no longer enabled
  (it breaks add-a-primitive-field evolution for record DTOs). Adding or
  removing DTO fields is now safe across a rolling deploy. Behavioral
  change vs 0.2.0; strictness is available via a custom
  `@Bean("outboxObjectMapper")`.

### Added
- **Capacity-coupled polling.** The poller now claims
  `min(claim-batch-size, free executor capacity)`, stops claiming entirely
  while the per-type executor is saturated, re-polls immediately after a
  full batch, and is woken the moment a saturated executor frees a slot.
  This removes the `claim-batch-size / poll-min-interval` throughput
  ceiling (20 events/s per type with defaults, regardless of pool size)
  and eliminates the claim/release write churn under overload (previously
  ~2 wasted hot-table writes per event while the pool was full).
  `handler-pool-size + handler-queue-capacity` now also acts as a soft
  in-flight cap for `handler-executor.type: virtual`. Amends ADR-0004.
- **Same-JVM after-commit poller wake-up.** `OutboxEventPublisher` now wakes
  the local poller of a published event type as soon as the publishing
  transaction commits (`TransactionContext.afterCommit` +
  `PollerWakeHub`/`Poller.wake()`), dropping same-JVM publish→handle latency
  from the poll interval (up to `poll-max-interval`, 10s default) to
  milliseconds. Rollbacks never wake; cross-pod pickup stays poll-bound.
  Always on, no configuration. Amends ADR-0006 (the `afterDone` mitigation
  cited there was never built).
- `EventStore.release(...)` and `EventStore.releaseClaimed(...)` SPI operations:
  return claimed events to `PENDING` **without** incrementing `attempts`. Used
  for lock contention, executor backpressure, unknown-handler SKIP, transient
  finalize failures and shutdown — none of these consume the retry budget any
  more.
- `event-outboxer.dispatcher.dispatch-rejected-retry-delay` property (default
  `1s`) — reschedule delay for dispatches rejected by a saturated handler
  executor.
- Enforcer rule `ban-core-in-adapter` in every adapter module: the
  "adapters must not depend on core" invariant is now build-enforced.
- Apache-2.0 license headers in `examples/`.

### Fixed
- **Stranded `PROCESSING` events.** Four recovery-path bugs could leave a
  claimed event invisible to both the watchdog and orphan recovery while the
  worker stayed alive: a dispatch rejected by a saturated executor was never
  unclaimed; a graceful shutdown deregistered the worker while unfinished
  events were still claimed (making them unrecoverable forever); a transient
  storage error during finalize dropped the event silently; and
  `unknown-handler-policy=FAIL` rows were wrongly documented as recoverable.
  Rejected dispatches and finalize failures now release the event back to
  `PENDING`; shutdown releases all still-claimed rows before deregistering
  (and skips deregister if the release fails, leaving the `graceful_stop` row
  for peer recovery).
- **Archive-mode `markProcessed` race.** The archive INSERT and the events
  DELETE ran as two statements; losing the optimistic-lock race between them
  committed an orphan archive row that blocked every future finalize of that
  event on the archive PK. The finalize is now a single atomic
  `DELETE ... RETURNING` → `INSERT` CTE.
- **Engine restart.** `stop()` permanently shut down the handler executors,
  so a restarted engine claimed events it could never dispatch. Executors are
  now recreated on every `start()`.
- **Heartbeat re-registration.** A live worker whose registry row was reaped
  by a peer (GC pause / DB outage longer than `dead-threshold`) now
  re-registers on the next heartbeat tick instead of running unregistered.
- **`graceful_stop` semantics.** `findDead` treated the flag as "never
  reclaim"; per the SPI contract it now means "reclaim immediately", so a JVM
  killed mid-shutdown no longer strands its events.
- **Worker-liveness clock skew.** The PostgreSQL adapter stamps and compares
  `last_heartbeat` with the database clock (`now()`), removing false-dead /
  false-alive verdicts caused by application-JVM clock skew.
- **Per-type "thin merge".** `event-types.overrides.<type>` previously reset
  every unset field to hard-coded class defaults, silently ignoring
  `event-types.defaults`. Overrides now merge field-by-field over the resolved
  defaults, as documented.
- `docs/CONFIGURATION.md` rewritten to match the actual `OutboxProperties`
  binding (property names, defaults, removed never-implemented subtrees).
- Release workflow now runs the full test suite and validates the changelog
  section **before** the irreversible deploy to Maven Central.

### Changed
- `OutboxEngine` constructor takes the `EventStore`, `Clock` and a
  `HandlerExecutorManager` (engine-owned executor lifecycle); `HeartbeatTask`
  takes `WorkerInfo` instead of `WorkerId`. Plain-Java users going through
  `OutboxEngineBuilder` are unaffected.

## [0.2.0] — 2026-04-22

### Added
- CI release workflow (`release.yml`) that triggers on new tags, builds and tests the project, and deploys to Sonatype Central Publishing.

## [0.1.0] — 2026-04-22

Initial release. Embedded transactional outbox for Java 17+ / Spring Boot 3.5.6 /
PostgreSQL 15 / Redis 7 (KeyDB 6). Baseline is Java 17; JDK 21+ at runtime
enables the optional `event-outboxer.handler-executor.type=virtual` executor
flavour (JDK 25+ additionally brings JEP 491's pin-free behaviour).

### Naming

To minimise collisions with other libraries sharing the same database
or Micrometer registry, the library's defaults use a specific prefix:

- **Database schema** defaults to `event_outboxer` (configurable via
  `event-outboxer.storage.schema`). The classpath Flyway migrations and the
  Liquibase changelog both use the `${eventOutboxerSchema}`
  placeholder; the Spring Boot starter auto-wires it for whichever
  tool is on the classpath so changing the property updates adapter
  SQL and migrations in lock-step. Plain-Java Flyway / Liquibase
  users pass the placeholder explicitly (see `docs/STORAGE.md
  §Configurable schema name`).
- **Micrometer metric prefix** defaults to `event_outboxer`
  (configurable via `event-outboxer.metrics.prefix`). Every counter / timer /
  summary registered by `MicrometerOutboxListener` carries this
  prefix.

### Added

**Public API (`event-outboxer-api`)**

- `OutboxEventPublisher` with `publish` and `publishAll`. Idempotent
  publishing (store-side dedup by key) is planned for 0.2.0 and is
  intentionally not part of the 0.1.0 API surface.
- `EventHandler<T>` with `extractLockKey(payload)` and optional
  per-handler `failureHandler()` override.
- Sealed `EventOutcome` (`Success` / `Retry` / `Fail` / `Skip`) and
  `FailureDecision` (`RetryAt` / `Disable` / `Delete`).
- `OutboxListener` observability surface (21 callbacks, record-based
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
  `Clock`, `ConnectionSupplier`, `MetricsSnapshotCache` ports.
- `MetricsSnapshotCache` ships `noop()` and
  `inMemory(Clock, Duration)` static factories; adapters swap the
  backing store without touching their own query path, so users can
  collapse per-pod snapshot drift with a shared cache (see
  `event-outboxer-cache-redis` + `event-outboxer.cache.type=redis`).
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
  table, cached metrics snapshot. Migrations shipped as classpath
  resources for both migration tools: Flyway
  (`classpath:db/migration/outbox/{core,archive}`) and Liquibase
  (`classpath:db/changelog/outbox/{core,archive}/changelog.xml`).
  Both variants share the same SQL files via Liquibase `<sqlFile>`
  and the starter auto-wires the `eventOutboxerSchema` parameter for
  whichever tool is on the classpath.
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
- `event-outboxer-cache-redis` — Lettuce-backed
  `MetricsSnapshotCache` impl. Every pod reads/writes a single
  Redis key (default `outbox:metrics:snapshot`) so
  `/actuator/health/outbox` returns the same aggregate totals across
  the fleet; TTL is enforced server-side via `SET PX`. Jackson JSON
  codec with `jsr310` for the `Instant` fields. Fail-safe on Redis
  errors (logged, treated as cache miss).
- `event-outboxer-metrics-micrometer` — full Micrometer implementation
  of `OutboxListener` (counters, timers, per-type tags).

**Spring Boot 3.5.6 starter (`event-outboxer-spring-boot-starter`)**

- `@ConfigurationProperties("event-outboxer.*")` binding for the full YAML
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
  with Actuator probe groups via `event-outboxer.health.probe-groups`, for
  k8s deployments that hit only `/actuator/health/liveness` and
  `/readiness`. Merges `outbox` into the configured groups while
  preserving the default `<group>State` contributor.
- `event_outboxer.engine.state{state=stopped|running|stopping}`
  Micrometer gauges for metric-based alerting on engine liveness
  without touching the probe groups.
- Backlog gauges driven by `EventStore.metricsSnapshot()` through
  the `MetricsSnapshotCache` SPI (so every scrape is bounded by the
  cache TTL and — with `event-outboxer.cache.type=redis` — shared across
  pods). Per-type (`event_type` tag):
  `event_outboxer.events.pending`, `…processing`, `…disabled`,
  `event_outboxer.events.oldest_pending_age_seconds`. Global:
  `event_outboxer.events.oldest_claimed_age_seconds`. Registered
  once per `EventHandler` bean at context refresh; aggregate in
  PromQL with `sum without(event_type)(…)`.
- **Engine crash detection** for poller threads.
  `EngineHealthCheckTask` runs every
  `event-outboxer.maintenance.watchdog-interval` and inspects each per-type
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
  491 eliminates synchronized pinning on JDK 25; virtual-thread APIs
  are invoked via reflection so the baseline stays Java 17). Platform variant uses
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
  - ADR-0017: Java 17 baseline (with JDK 21+ opt-ins) + Spring Boot 3.5;
  - ADR-0018: JSpecify for nullness annotations.

**Build / CI**

- Maven Wrapper pinned to Maven 3.9.12.
- GitHub Actions: `ci.yml` (unit + `-P it` integration on
  Testcontainers), `release.yml` (tag-triggered deploy through
  Sonatype Central Publishing).
- `maven-enforcer-plugin` rules: JDK 17+, Maven 3.9+, dependency
  convergence, ban Spring from `event-outboxer-core`.

### Known limitations

- Idempotent publishing (dedup by key) is not part of the 0.1.0 API;
  planned for 0.2.0 alongside an idempotency-key column in the store.
- LISTEN/NOTIFY fast-wake path for the PG adapter (ADR-0006) is not
  implemented in 0.1.0; polling is the only claim mechanism.

### Security

- No known CVEs at release time. Dependencies tracked via Spring Boot
  `spring-boot-dependencies` BOM; patch releases will follow
  upstream advisories.

[0.1.0]: https://github.com/bams22/event-outboxer/releases/tag/v0.1.0
