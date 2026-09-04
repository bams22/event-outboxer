# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
- **Benchmark and invariant harness — `event-outboxer-benchmark`
  (ADR-0034, never published).** A reactor module that drives the
  library as deployed (Spring Boot starter + PostgreSQL storage + lease
  locker: one publish-only context, N worker contexts with explicit
  worker ids) through presets — `smoke`, `throughput`, `hot-key`,
  `failures`, `backlog` — every knob overridable from the command line.
  Every handler invocation goes into the harness's own ledger, which is
  graded after a graceful stop: no lost event, no duplicate, no
  handling of an unpublished event, no overlap on a lock key when a
  real locker is on, and a clean `events` / `entity_locks` schema.
  Invariants fail the run (exit code 1); publish and end-to-end latency
  percentiles, handled/s, retries and `pg_stat_user_tables` row writes
  per event are reported as information, never gated. The report is a
  self-describing JSON (scenario, JVM, host, PostgreSQL version
  embedded). `-P bench` builds an executable jar; without
  `--bench.jdbc-url` the run starts a disposable PostgreSQL via
  Testcontainers. The `smoke` preset also runs as `BenchmarkSmokeIT`
  under `-P it`. The system under test is an interface, so a second
  outbox implementation can be measured with the same scenarios from an
  adapter kept outside this repository.
- **Benchmark harness phase 2: forked fleet and chaos (ADR-0034
  amendment).** `--bench.fleet=forked` runs one JVM per worker (forked
  with the driver's own code, spec file, ready marker, per-worker log)
  writing into a `bench.handled` ledger table. Chaos actions fire from
  the drain loop at a handled-progress trigger: `--bench.kill-workers`
  `SIGKILL`s workers (respawned by default), `--bench.pg-restart=fast|crash`
  restarts the disposable PostgreSQL in place (fixed host port; `fast`
  = `SIGINT`, `crash` = `SIGKILL` + WAL replay). Duplicates within
  ±10 s of a kill of their own worker or of a database restart are
  attributable and do not fail the run; leases owned by killed workers
  or acquired before an outage are discounted by the storage check. New
  presets `crash` and `pg-restart` with shortened recovery timers; new
  ITs `BenchmarkCrashIT` and `BenchmarkPostgresRestartIT`. A crash
  restart resets `pg_stat`, so the database-cost figure is then marked
  unreliable in the report.
- **Benchmark harness: Redis locker.** `--bench.lock=redis` runs the
  scenario on `event-outboxer-lock-redis`, against `--bench.redis-uri`
  or a disposable `redis:7-alpine` container; the report gains a
  `redis` block (commands processed per event from `INFO stats`, lock
  keys left after the stop) and the environment records the Redis
  origin and version. `BenchmarkRedisLockIT` under `-P it`.
- **Benchmark harness: payload format and database-size figures.**
  `--bench.payload=jackson|protobuf` selects the write format (a
  protoc-generated `BenchPayloadProto` with the same fields as the
  Jackson record; `write-format` set on every context). The report's
  database block gains WAL bytes per event and the events table size
  after the publish phase, and every run now begins with `VACUUM FULL`
  on the events table so a previous run's bloat cannot pose as a
  difference between variants. `BenchmarkProtobufIT` under `-P it`.
- **Benchmark harness: statement counts.** When the server preloads
  `pg_stat_statements`, the report's database block carries calls and
  rows per statement class (claim, insert, batched/single finalize,
  release, other) sampled before and after the run — the direct
  measure of round trips per event that the group-commit sessions
  needed.


## [0.7.0] — 2026-09-03

### Breaking
- **`ArchivedEvent` gains a trailing `@Nullable String dedupKey`
  component.** The canonical constructor's arity changes; `@Builder`
  users are unaffected. `null` for key-less events and for rows
  archived before migration V008.

### Added
- **`OutboxAdmin` grows 6 → 8 methods (ADR-0019 amendment,
  ADR-0033).** `replayFromArchive(UUID)` → `ReplayOutcome {REPLAYED,
  COALESCED, ID_IN_USE, NOT_FOUND}` and
  `replayAllFromArchive(eventType, archivedAfter, archivedBefore,
  limit, after)` → `ReplayAllResult(replayed, coalesced, idInUse,
  next)` (both types nested in the interface), plus the new
  `ArchiveCursor(archivedAt, id)` keyset cursor alongside the existing
  `AdminCursor`. Both methods are `default`, carrying the answer for a
  store without an archive — argument validation, then `NOT_FOUND` /
  zero counts with a null cursor — so third-party implementations keep
  compiling and only adapters that *have* an archive override them.
- **The archive carries the dedup key (ADR-0033, migration V008).**
  `event_archive.dedup_key` — nullable, un-indexed, audit-only; both
  archiving CTEs copy it, `findInArchive` returns it, and the admin
  surfaces expose it (`dedupKey` also added to the REST
  `EventResponse` and the Actuator event maps). Runtime coalescing
  semantics are unchanged — uniqueness stays on the hot table's
  partial index only.
  **Upgrade note for `archive-enabled=true` deployments:** the
  archiving statement now names `dedup_key`, so V008 must be applied
  with this upgrade or every `markProcessed` fails at runtime. The
  starter-managed Flyway instance applies it automatically; Liquibase
  users must pick up the `outbox-archive-008-dedup-key` changeset; the
  ADR-0028 legacy-upgrade recipe is unchanged:
  `event-outboxer.flyway.baseline-version=7` — the highest migration a
  ≤ 0.4.0 install actually applied. Do **not** raise it to 8: Flyway
  treats every migration at or below the baseline as already applied, so
  a baseline of 8 skips V008 forever and produces exactly the
  `markProcessed` failure described above.
- **Replay from archive (ADR-0033).** Operators can move archived
  events back into the hot table for re-execution — legal by contract
  since delivery is at-least-once and handlers are idempotent
  (ADR-0015). One atomic insert-first CTE: the replayed row re-enters
  as a fresh `PENDING` (attempts 0, version 0, `created_at = now`,
  `run_at = now` — a replay is a new lifecycle and gets a new retention
  clock; the original publish time stays readable in the archive), and
  when a `PENDING` event with the same `(event_type, dedup_key)`
  already exists the replay *coalesces* — nothing is inserted and the
  archive row is kept. A row whose id is already live in the hot table
  — the application re-published that explicit UUID — is reported
  rather than fatal: `ReplayOutcome.ID_IN_USE` for a single replay
  (REST 409), the `idInUse` counter in bulk. It is skipped, the archive
  row is kept and the rest of the batch still moves; without that one
  such row would abort the whole statement and wedge the sweep on that
  window. Exposed as `OutboxAdmin.replayFromArchive` /
  `replayAllFromArchive`, REST `POST /events/{id}/replay` +
  `/events/replay-all`, and the Actuator write operations'
  `action=replay` discriminator.
  **One thing to get right when adopting it:** drive a bulk sweep with
  the cursor, not the counters. Rows that stay archived (coalesced or
  `idInUse`) are found again by the same window, so no counter ends the
  loop; `ReplayAllResult.next()` is the `ArchiveCursor` of the last row
  the batch considered — pass it back as `after` and loop until it
  comes back null. A window whose `archivedAfter` is not strictly
  before `archivedBefore` (both bounds are exclusive) is rejected with
  `IllegalArgumentException` instead of answered with zeroed counts —
  swapped date pickers must not read as "already replayed".
- **Bulk replay is indexed (ADR-0033, migration V009).**
  `idx_archive_event_type_archived_at (event_type, archived_at, id)`
  covers the sweep's access path — `WHERE event_type = ? [AND
  archived_at …] ORDER BY archived_at, id LIMIT ?` — which neither
  V002 index served. Without it each batch walked the archive in
  `archived_at` order filtering on type, while holding the locks of its
  own INSERT into `events` and DELETE from the archive. Index only:
  applying it late costs performance, not correctness. Liquibase users
  pick it up as the `outbox-archive-009-replay-index` changeset.
- **The admin REST controller answers a rejected argument with 400
  instead of 500.** A malformed cursor, a non-positive limit or a
  replay window whose bounds cannot enclose anything reach the
  controller as `IllegalArgumentException`; a controller-scoped
  `@ExceptionHandler` now renders them as `400` with the reason in the
  usual `{"error": …}` body. Scoped to this controller, not a
  `@ControllerAdvice`, so an opt-in admin surface never changes how the
  host application renders its own exceptions. (The Actuator endpoint
  already returned 400 for these — Spring Boot maps
  `IllegalArgumentException` out of a `@WriteOperation` itself.)
- **New module `event-outboxer-relay-spring-cloud-stream`
  (ADR-0032)** — a ready-made broker relay over Spring Cloud Stream:
  the `StreamOutboxPublisher` facade stores messages (binding, key,
  headers, payload) in the outbox inside the caller's transaction, and
  a built-in `EventHandler` (reserved event type
  `outboxer-stream-relay`) delivers them through `StreamBridge` — no
  per-project event DTO or relay handler needed. Payloads are encoded
  at publish time (Jackson by default, `StreamPayloadEncoder` SPI to
  replace; `String` / `byte[]` / `SerializedPayload` pass through as
  the wire form). Auto-activates when the module and `StreamBridge`
  are on the classpath; configured under
  `event-outboxer.relay.stream.*` (kill switch, configurable
  message-key header — default `kafka_messageKey`, opt-in per-key
  ordering via entity-locker lock keys). The parent build now imports
  the `spring-cloud-dependencies` 2025.0.x BOM; the consumer BOM does
  not re-export it. New message code `OUTBOX-105`
  (`StreamEncodingException`).
  **One thing to get right when adopting it:** configure an
  acknowledged producer send per binding (Kafka:
  `spring.cloud.stream.kafka.bindings.<b>.producer.sync: true`) — on
  binder defaults `StreamBridge.send` returns before the broker acks,
  which makes the hop from the outbox to the broker at-most-once.


## [0.6.0] — 2026-08-31

### Breaking
- **Seven redundant Micrometer meters removed; `events.attempts`
  gains an `outcome` tag.** Every removed series is derivable from a
  surviving one; migrate dashboards and alerts as follows and
  re-import the shipped Grafana dashboard:

  | Removed | Use instead |
  |---|---|
  | `event_outboxer_events_claimed_total` | `event_outboxer_events_queue_time_seconds_count` |
  | `event_outboxer_events_processed_total` | `event_outboxer_events_processing_time_seconds_count` |
  | `event_outboxer_handler_stuck_reclaimed_total` | `event_outboxer_handler_stuck_time_seconds_count` |
  | `event_outboxer_dispatch_rejected_total` | `event_outboxer_events_retry_scheduled_total{reason="dispatch_rejected"}` |
  | `event_outboxer_poller_batch_size_*` | avg batch = `queue_time_seconds_count / poller_polls_total{result="claimed"}` |
  | `event_outboxer_workers_deregistered_total` | `event_outboxer_workers_graceful_stops_total` (documented as same-semantics) |
  | `event_outboxer_orphans_dead_workers_total` | dropped without replacement (`orphans.reclaimed` remains) |

  `event_outboxer.events.attempts` now carries
  `outcome=processed|disabled|deleted` — the untagged series
  disappears; `sum without(outcome)(...)` restores the old view. The
  corresponding `OutboxListener` callbacks are unchanged — only the
  Micrometer implementation slimmed down.
- **`OutboxListener` grows 26 → 28 methods (ADR-0013 amendment).**
  New callbacks `onEventCoalesced(EventCoalescedInfo)` (a keyed publish
  coalesced into an existing PENDING event instead of inserting —
  fires instead of `onEventPublished`, ADR-0021 amendment) and
  `onMaintenanceRunCompleted(MaintenanceRunInfo)` (every periodic
  maintenance task run, OK or FAILED, with a stable task name:
  `heartbeat`, `orphan_recovery`, `watchdog`, `engine_health_check`,
  `retention`, `stale_claim_sweeper`). Both have default no-ops —
  existing listener implementations keep compiling; only the interface
  surface expands.
- **`HandlerErrorInfo` and `PollCompletedInfo` gained a trailing
  `Duration duration` component.** `HandlerErrorInfo.duration` is the
  time spent in the failed `handler.handle(...)` attempt;
  `PollCompletedInfo.duration` is the wall time of the claim query,
  recorded for empty polls too. Constructor calls must append the new
  argument.

  Migration —

  ```java
  // before
  new HandlerErrorInfo(id, "ORDER", 1, cause);
  new PollCompletedInfo("ORDER", 10, 7);
  // after
  new HandlerErrorInfo(id, "ORDER", 1, cause, Duration.ofMillis(250));
  new PollCompletedInfo("ORDER", 10, 7, Duration.ofMillis(3));
  ```
- **Maintenance tasks no longer swallow their own failures — the
  scheduler's guarded wrapper is the single catch-and-continue
  barrier.** Behaviour of the background engine is unchanged (failures
  are still caught, logged, and the schedule survives — a throwing
  task can no longer silently cancel its own `scheduleWithFixedDelay`
  registration either, which previously could happen to the watchdog
  on an `inFlight.snapshot()` failure). But code driving the tasks
  directly sees the difference: testkit's
  `ManualEngine.tickHeartbeat()` / `tickOrphanRecovery()` now
  propagate storage failures to the caller instead of logging them
  away — deliberate, tests should see the failure. A failing retention
  sweep still runs the other dimension and reports partial progress
  via `onRetentionPurged` before propagating.

### Added
- **Six new meters close the observability blind spots.**
  `event_outboxer.maintenance.runs{task,result}` (background-task
  liveness — failures used to be WARN log lines only),
  `event_outboxer.events.coalesced{event_type}` (ADR-0021 dedup
  ratio), `event_outboxer.poller.claim_time{event_type}` (claim-query
  latency, attributes a rising queue time to the DB vs the handlers),
  `event_outboxer.handler.error_time{event_type}` (failed-attempt
  duration — separates instant failures from burnt timeouts),
  `event_outboxer.heartbeat.last_success_age_seconds` (starter gauge,
  NaN until the first success — catches a stalled maintenance
  scheduler that `heartbeat.failed` cannot see), and `storage.errors`
  operations `save`/`finalize`/`release`. The new timers ship without
  SLO buckets (count/sum/max only). `entity_locks.held` — which
  existed all along — is now in the OBSERVABILITY.md table too.
- **`OutboxEngine.lastHeartbeatSuccessAt()`** — instant of the last
  successful heartbeat write ({@code null} until the first); the
  read-only seam behind the starter's upcoming heartbeat-age gauge. A
  stalled maintenance scheduler shows up as a growing age even though
  `onHeartbeatFailed` never fires.
- **`storage.errors` now covers more than the claim path.** The
  dispatcher reports a failed finalize (`operation="finalize"`) and a
  failed recovery release (`operation="release"`); the publisher
  reports a failed insert (`operation="save"`) before rethrowing
  `PublishFailedException`. A finalize failure whose recovery release
  also fails emits both — two distinct failed storage operations.
- **`RecordingOutboxListener`** records the two new callbacks
  (`coalesced()`, `maintenanceRuns()` accessors); its `clear()` now
  also resets the previously-missed `handlersAbandoned` list.
- **Low-watermark claim refill — `claim-min-free` (ADR-0004
  amendment).** New per-type setting
  `event-outboxer.event-types.{defaults,overrides.<TYPE>}.claim-min-free`
  (core: `EventTypeConfig.claimMinFree`): the poller claims only once
  that many in-flight slots are free, and the executor's capacity wake
  fires on that threshold. The default `1` keeps the previous behaviour
  (claim as soon as a slot frees). A larger value lets the handler
  queue drain to a low watermark and refills it with one claim, so
  under sustained load a queue of N costs one claim statement per N
  handler completions instead of one per completion — previously
  `handler-queue-capacity` only decided how many rows a JVM hoarded,
  never how often it claimed. Validated to `[1, handler-pool-size +
  handler-queue-capacity]`; the starter warns at startup when the
  threshold exceeds the queue capacity on a platform executor (idle
  handler threads). `EventTypeConfig.builder()` gains the required
  `claimMinFree` field — callers building the record from scratch must
  set it (`defaults().toBuilder()` is unaffected).


## [0.5.0] — 2026-08-29

### Breaking
- **Typed event key (ADR-0031).** `EventHandler` declares
  `EventType<T> type()`; `eventType()` / `payloadType()` are now
  derived default methods. `OutboxEventPublisher.publish(String, Object, …)`
  is replaced by `<T> publish(EventType<T>, T, …)`; `PublishRequest` is
  `PublishRequest<T>(EventType<T>, T, PublishOptions)` and `publishAll`
  takes `Collection<? extends PublishRequest<?>>`. A payload that is not
  an instance of the key's payload class is rejected at publish with
  `PublishValidationException` (`OUTBOX-101`); event names are limited
  to 128 characters at construction. Migration — one constant per event
  type, shared by both sides:
  ```java
  // before
  @Override public String eventType() { return "SEND_EMAIL"; }
  @Override public Class<SendEmailPayload> payloadType() { return SendEmailPayload.class; }
  publisher.publish("SEND_EMAIL", payload);
  // after
  public static final EventType<SendEmailPayload> SEND_EMAIL =
          EventType.of("SEND_EMAIL", SendEmailPayload.class);
  @Override public EventType<SendEmailPayload> type() { return SEND_EMAIL; }
  publisher.publish(SendEmailHandler.SEND_EMAIL, payload);
  ```
  Producers that only know the name at runtime use
  `EventType.untyped(name)`.
- **`event-outboxer-spring-boot-starter` now depends on
  `event-outboxer-serializer-jackson` non-optionally (amends ADR-0016
  and ADR-0026).** JSON via Jackson is the zero-config default
  serializer, the way any Boot starter ships a default; Jackson
  (`jackson-databind` + datatype modules, Boot-managed versions)
  becomes transitive for starter users. Protobuf-only applications that
  relied on *omitting* the Jackson module must now either set
  `event-outboxer.serializer.write-format=protobuf` (Jackson stays
  registered read-only — recommended) or exclude
  `event-outboxer-serializer-jackson` from the starter in their pom.
  Explicit `event-outboxer-serializer-jackson` dependencies next to the
  starter are redundant and can be removed. The README quick start and
  the starter's minimal setup, which omitted the module, now work as
  written.
- **The outbox migrations moved out of `db/migration/` and are applied
  by a starter-managed Flyway instance (ADR-0028).** Up to 0.4.0 the
  SQL lived under `db/migration/outbox/{core,archive,lock}` and users
  appended those directories to `spring.flyway.locations`. That never
  worked as documented: Flyway scans `classpath:db/migration`
  recursively, discards the sub-locations, applies every lane
  regardless of the opt-in — and the library's `V001…V007` collide
  with the application's own version numbers (`Found more than one
  migration with version 001`; the shipped example was affected). The
  SQL now lives under `classpath:event-outboxer/migration/{core,archive,lock}`
  (header comments refreshed, DDL unchanged) and the starter applies it itself:
  fixed locations, its own `flyway_schema_history` inside
  `event-outboxer.storage.schema`, `outOfOrder` on so a lane adopted
  later applies cleanly. The archive lane is now always applied;
  `storage.archive-enabled` only governs runtime behaviour.
  - Upgrading from ≤ 0.4.0 (outbox rows in the application's history
    table):
    1. remove the `db/migration/outbox/*` entries from
       `spring.flyway.locations`;
    2. let the application instance forget them:
       `spring.flyway.ignore-migration-patterns: "*:missing"` (or
       delete the `outbox_*` rows from `flyway_schema_history`);
    3. for **one** deploy set `event-outboxer.flyway.baseline-on-migrate: true`
       and `event-outboxer.flyway.baseline-version: 7` (the highest
       outbox migration already applied — lower if you never adopted
       the archive or lock lane; apply the missing SQL by hand first in
       that case), then remove both properties. A `relation already
       exists` failure on first start is rethrown with this recipe.
  - To keep applying the SQL through your own tooling set
    `event-outboxer.flyway.enabled: false` and point Flyway at the new
    locations (the `${eventOutboxerSchema}` placeholder is still fed
    into the application instance) or use the unchanged Liquibase
    changelogs under `db/changelog/outbox/*`.

### Changed
- **Failure-chain precedence is defined and enforced (ADR-0030):**
  `EventHandler.failureHandler()` → per-type bean → per-type YAML →
  global bean → YAML defaults → `FailureHandlers.defaults()`. The bean
  names `outboxDefaultFailureHandler` / `outboxPerTypeFailureHandlers`
  keep working as the documented legacy form; two beans claiming the
  same slot now fail startup (`AmbiguousOutboxFailureHandlerException`
  + `FailureAnalyzer`) instead of one silently winning, and a
  `FailureHandler` bean that claims no slot is listed in a startup WARN
  instead of being ignored.
- **Publish-only mode is an explicit opt-in (ADR-0029).**
  `OutboxEngineBuilder.publishOnly(true)` / `event-outboxer.publish-only=true`
  start the engine without pollers: it registers its worker, runs
  heartbeat / orphan recovery / retention and exposes
  `OutboxEventPublisher`; any handlers it was given are ignored, so one
  code base can run as API nodes and worker nodes. Without the flag an
  empty handler set is still rejected — now with the dedicated
  `NoEventHandlersException` (`ConfigurationException`) instead of a
  bare `IllegalStateException`, and the starter turns it into a
  `FailureAnalyzer` diagnosis naming both ways out.

### Added
- `EventType<T>` (`domain`): `of(name, class)`, `untyped(name)`,
  `MAX_NAME_LENGTH`; `PublishRequest.of(type, payload[, options])`;
  `EventType<?>` overloads of `OutboxEngineBuilder.eventTypeConfig` /
  `failureHandlerFor` / `writeSerializerOverride`,
  `OutboxTestContext.Builder` (same three) and `ManualEngine.tick`.
- `EventOutcome` static factories: `success()`, `skip(reason)`,
  `retry(reason)`, `retry(reason, cause)`, `retry(reason, delay)`,
  `retry(reason, delay, cause)`, `fail(reason)`, `fail(reason, cause)`.
  `Success.INSTANCE` is what `success()` returns.
- **Retry policy in YAML (ADR-0030, delivers ADR-0007 §YAML):**
  `event-outboxer.event-types.defaults.failure.*` and
  `event-outboxer.event-types.overrides.<TYPE>.failure.*` — `strategy`
  (`exponential` / `fixed` / `none`), `max-attempts`,
  `exhausted-action` (`DISABLE` / `DELETE`), `base-delay`, `multiplier`,
  `max-delay`, `jitter`, `fixed-delay`, `log-level` (`OFF` drops the
  per-failure log line) — thin-merged like the other per-type knobs
  onto `FailureHandlers.defaults()`; bad values fail startup naming
  the exact property path.
- `@OutboxFailureHandler` qualifier for `FailureHandler` beans: no
  value = the global chain, `{"A", "B"}` = the chain for those event
  types.
- `FailureHandlerResolverTest` covers the core resolution order
  (handler override → per-type → default), previously untested.
- `NoEventSerializersException` (`ConfigurationException`) replaces the
  `InvariantViolationException` thrown when no `EventSerializer` bean is
  registered; the starter maps it to a `FailureAnalyzer` diagnosis
  (`OutboxSerializerFailureAnalyzer`) naming the exclusion, the protobuf
  module and `write-format`.
- `event-outboxer.flyway.*`: `enabled` (default `true`), `url`, `user`,
  `password`, `driver-class-name` for a dedicated migration connection
  (a DDL role separate from the application role), `baseline-on-migrate`
  / `baseline-version` for the one-time upgrade. `user` without `url`
  derives a connection from the outbox `DataSource` with the given
  credentials; with several `DataSource` beans the
  `@OutboxDataSource`-qualified one is used (ADR-0024).
- `OutboxFlywayMigrationInitializer` is registered as a Spring Boot
  database initializer, so `@DependsOnDatabaseInitialization` beans
  (the lease-table probe) and JDBC consumers are created after the
  outbox schema is migrated. Declare your own bean of that type to take
  over the wiring.
- Flyway 10+ without `flyway-database-postgresql` on the classpath
  fails fast at startup naming the artifact.


## [0.4.0] — 2026-08-28

### Breaking
- **Core wiring classes construct through builders (amends ADR-0023).**
  Pre-1.0 breaking change: the telescoping public constructors are
  gone, replaced by Lombok builders on private validating constructors
  (required collaborators throw `NullPointerException` with the
  parameter name; every other parameter defaults when unset).
  - `DefaultOutboxEventPublisher`: the four constructors (7–10 args)
    → `DefaultOutboxEventPublisher.builder()`. Required: `store`,
    `serializer`. Defaults: no write-serializer overrides,
    `Clock.system()`, `TransactionContext.alwaysActive()`,
    `NoTransactionPolicy.FAIL`, `OutboxListener.NOOP`,
    `PollerWaker.NOOP`, `OutboxTracer.NOOP`, `deferredPropagation =
    LINK`, `linkThreshold = 1m`.
  - `HandlerDispatcher`: both constructors (11/12 args) →
    `HandlerDispatcher.builder()`. Required: `store`,
    `serializerRegistry`, `handlerResolver`, `workerId`; everything
    else defaults (`EntityLocker.NOOP`, default failure-handler chain,
    a private `InFlightRegistry`, `OutboxListener.NOOP`,
    `Clock.system()`, `EventTypeConfig.defaults()`,
    `DispatcherConfig.defaults()`, `OutboxTracer.NOOP`).
  - `Poller`, `WatchdogTask`, `OrphanRecoveryTask`,
    `StaleClaimSweeperTask`, `MaintenanceScheduler`: constructors →
    `builder()` with the same required/default split (see each class
    Javadoc). `StaleClaimSweeperTask` takes `threshold` / `interval` by
    name, so the two adjacent `Duration` parameters can no longer be
    swapped silently.
  - `OutboxEngine`'s constructor is package-private: assemble engines
    with `OutboxEngineBuilder`, which the Spring Boot starter uses too.
  - The nullable link threshold (added in this release cycle) is
    replaced by an explicit pair: `OutboxEngineBuilder.tracingLinkThreshold(Duration)`
    → `.deferredPropagation(Propagation)` + `.linkThreshold(Duration)`;
    the former `null` ("never link") is now `Propagation.CHILD`.
    `OutboxProperties.Tracing.resolveLinkThreshold()` is removed — the
    starter passes `deferred-propagation` and `link-threshold` through
    unchanged.
  - Migration:
    ```java
    // before
    new DefaultOutboxEventPublisher(store, serializer, clock, txContext,
            NoTransactionPolicy.FAIL, listener, waker, tracer);
    // after
    DefaultOutboxEventPublisher.builder()
            .store(store).serializer(serializer).clock(clock)
            .transactionContext(txContext).listener(listener)
            .waker(waker).tracer(tracer).build();
    ```

### Changed
- **Deferred events start a new trace linked to the producer span
  (amends ADR-0023).** An event published with a `runAt` further ahead
  than `event-outboxer.tracing.link-threshold` (new, default `1m`) no
  longer gets a CONSUMER span parented by the publish-time context —
  which stretched one trace across the whole delay — but a new root
  span carrying a span link to the PRODUCER span; both are tagged
  `event_outboxer.propagation=link`, baggage is still restored. The
  decision is made once at publish time and stored with the event as
  the extra `trace_context` key `event_outboxer.propagation=link`,
  stripped before the carrier reaches adapters or `EventContext`.
  `event-outboxer.tracing.deferred-propagation: child` (new) restores
  the previous behaviour. On Micrometer Tracing the linked shape
  requires the starter's new `OutboxReceiverTracingObservationHandler`
  bean (auto-registered ahead of Boot's receiver handler); the Brave
  bridge keeps the parent-child shape and renders the link as tags.

### Added
- `OutboxTracer.Propagation`, `PublishSpan.linked()` (default no-op),
  `ProcessSpanInfo.propagation()` with a five-argument compatibility
  constructor, `OutboxTraceAttributes.PROPAGATION` /
  `PROPAGATION_LINK`, `OutboxEngineBuilder.deferredPropagation(...)` /
  `.linkThreshold(...)`; in the Micrometer module
  `OutboxReceiverContext` and `OutboxReceiverTracingObservationHandler`.
- `OutboxListener.NOOP` — a listener whose every callback is the
  default no-op, the non-null default of the wiring builders.
- Lombok `@Builder` on the domain records `Event`, `ArchivedEvent`,
  `ClaimedEvent` and on `EventContext` / `FailureContext` (canonical
  constructors unchanged; handy for tests and custom storage
  adapters).


## [0.3.0] — 2026-08-16

### Breaking
- **Observability surface extended and re-tagged (amends ADR-0013).**
  Pre-1.0 breaking changes to the `OutboxListener` contract and metric
  names:
  - `OutboxListener` grows 21 → 26 methods: `onPollCompleted`,
    `onPollerSaturated` (new Polling group), `onStaleClaimsSwept`,
    `onRetentionPurged` (new Maintenance group) and `onHandlerAbandoned`
    (Recovery group, see the stuck-handler entry under Added) — all
    default no-op.
  - `EventClaimedInfo` gains `createdAt`; `EventRetryScheduledInfo` and
    `EventDisabledInfo` gain a bounded `Trigger` enum (the free-form
    `reason` string stays for logging); `LockAcquisitionInfo` gains
    `Outcome (BUSY | ERROR)` + nullable `cause`, discriminating normal
    contention from locker-backend failures.
  - The three starter backlog gauges `events.pending` / `.processing`
    / `.disabled` merged into one
    `event_outboxer.events.backlog{event_type,status}` gauge (see the
    meter-collision fix below). Dashboards must be updated.
  - `events.retry_scheduled` and `events.disabled` counters gain a
    `reason` tag, `handler.errors` an `exception` tag,
    `lock.acquisition_failed` an `outcome` tag — PromQL that summed
    these by name alone keeps working; per-series dashboards see new
    label sets.
- **Binary-capable serializer SPI and per-event payload format
  (ADR-0025).** Pre-1.0 breaking change preparing the serialization
  seam for binary formats (Protobuf, Smile, Fury) — the first of them
  ships in this release (see the Protobuf serializer module entry under
  Added, ADR-0026); Jackson remains the default writer:
  - `EventSerializer` now declares `String format()` and works over the
    new two-lane `SerializedPayload` value type
    (`ofText`/`ofBytes`, exactly one lane set) instead of `String`.
    Custom serializer implementations must adapt; text-lane migration
    is mechanical (`SerializedPayload.ofText(...)` /
    `payload.requireText()`).
  - `PendingEvent` / `ClaimedEvent` / `Event` / `ArchivedEvent` carry
    `SerializedPayload payload` plus a new `String payloadFormat`
    component. `payloadClass` stays — re-documented as publish-time
    diagnostics; it never selected the deserialization target (that is
    always `EventHandler.payloadType()`), so DTO renames keep being
    safe for stored events.
  - `SerializationErrorInfo` reshaped to
    `(eventId, eventType, payloadFormat, storedPayloadClass,
    targetType, cause)` — fixing a bug where the "class the engine
    tried to deserialize into" was actually the publish-time class.
  - `HandlerDispatcher` construction takes an
    `EventSerializerRegistry` instead of a single `EventSerializer`
    (relevant to plain-Java wiring only; `OutboxEngineBuilder`'s
    `eventSerializer(...)` is source-compatible).
  - Admin surfaces (actuator + REST): `payload` is now nullable (text
    lane) next to new `payloadBase64` (binary lane) and
    `payloadFormat` fields.
- **Stuck-handler cancellation reshapes three core types (amends
  ADR-0014).** Pre-1.0 breaking changes that come with the feature
  described under Added:
  - `StuckHandlerReclaimedInfo` gains an `interrupted` component.
  - `MaintenanceConfig` gains `abandonedHandlerGrace` and
    `EventTypeConfig` gains `interruptStuckHandler` — plain-Java wiring
    that builds them field-by-field must set both.
  - Plain-Java wiring only: `WatchdogTask`'s constructor takes the
    abandoned-handler grace, `InFlightRegistry.Entry` carries a
    `DispatchHandle`, and `InFlightRegistry.register` / `unregister` /
    `markAbandoned` take that entry instead of a bare event id (both
    registry sets are keyed per dispatch, since a force-reclaimed event
    is routinely re-claimed by the same JVM while the old dispatch runs
    on). `OutboxEngineBuilder` users are unaffected.

### Added
- **Stuck handlers are cancelled, not just reclaimed (amends
  ADR-0014).** After a successful `forceReclaim` the watchdog now
  interrupts the dispatching thread: the force-reclaim already
  invalidated that handler's finalize, so letting it run only burned a
  slot of the per-type handler pool. Previously a handler blocked
  without a timeout leaked one thread per `handlerMaxRuntime` — and,
  because the retry chain only runs when a handler *returns*, it never
  reached `MaxRetriesFailureHandler` either, so the type quietly
  stopped processing once every pool slot was held by a zombie.
  - Per-type opt-out `event-outboxer.event-types.*.interrupt-stuck-handler`
    (default `true`) for handlers that are not interrupt-safe.
  - The dispatcher clears a watchdog-issued interrupt as soon as the
    handler unwinds — before the finalize and the entity-lock release,
    so a cancellation cannot break the cleanup — and again before
    returning its thread to the pool, so it never leaks into the next
    event.
  - Dispatches still running after the new
    `event-outboxer.maintenance.abandoned-handler-grace` (default 30s)
    are tracked as **abandoned** and reported once:
    `OutboxListener.onHandlerAbandoned(HandlerAbandonedInfo)` (26th
    callback, default no-op), counter
    `event_outboxer.handler.abandoned`, gauge
    `event_outboxer.handler.abandoned_threads{event_type}`, and a log
    line naming the thread — ERROR when the handler ignored the
    interrupt, WARN when the type opted out of being interrupted.
  - New troubleshooting recipe #7 in
    [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) for a single event
    type that stopped processing.
- **SLO histogram-bucket defaults for all timers, applied
  automatically.** `event-outboxer-metrics-micrometer` ships
  `META-INF/event-outboxer/metrics-defaults.yml` (10ms–1h grid for
  `events.queue_time` — retried events wait from the original publish;
  10ms–10m for `events.processing_time`, covering the default 5m
  `handler-max-runtime` budget; 30s–1h for `handler.stuck_time`); the
  starter's new
  `OutboxMetricsDefaultsEnvironmentPostProcessor` appends it with the
  lowest precedence at startup, so fleet-wide `histogram_quantile()`
  works in Prometheus with zero configuration while any user-set
  `management.metrics.distribution.*` value still wins. Opt out with
  `event-outboxer.metrics.distribution-defaults.enabled=false`.
  `MetricsDistributionDefaultsTest` pins all three behaviours
  (auto-applied, override wins, opt-out).
- **Ready-to-import Grafana dashboard.**
  [docs/grafana/event-outboxer-dashboard.json](docs/grafana/event-outboxer-dashboard.json)
  — 31 panels in 8 rows (health, throughput, latency, backlog, errors,
  saturation, locks, maintenance) over the Prometheus metric catalogue,
  with `environment` / `service` / `pod` / `eventType` template
  variables and a `DS_PROMETHEUS` datasource variable, so importing it
  needs no manual datasource mapping. Store-wide gauges (backlog, ages,
  lease count) aggregate across pods with `max`, per-JVM metrics with
  `sum`; the p50/p99 panels use `histogram_quantile` and so depend on
  the histogram buckets applied by the SLO defaults above. Import
  smoke-tested against Grafana 11.4.
- **Queue-time lag, saturation and maintenance metrics.** New meters in
  `event-outboxer-metrics-micrometer`: `events.queue_time` timer
  (publish → claim lag, the "am I falling behind" signal),
  `handler.stuck_time` timer, `poller.polls{result}` counter,
  `poller.batch_size` summary, `poller.saturated` counter,
  `claims.stale_swept` and `retention.purged{kind}` counters; the
  `events.attempts` summary now also records on disabled and deleted
  finalizes. New starter gauges read directly off the engine:
  `events.in_flight{event_type}`,
  `handler.executor.free_capacity{event_type}` and
  `handler.executor.capacity{event_type}` (uniform pool+queue budget
  semantics for platform and virtual executors), via new read-only
  `OutboxEngine` accessors — core stays Micrometer-free.
- **Starter-managed Redis connection (ADR-0027).** Setting
  `event-outboxer.redis.uri` (or `.host`, plus optional
  `port`/`username`/`password`/`database`/`ssl`/`timeout`/
  `client-name`) is now all it takes to run `lock.type: redis` and
  `cache.type: redis` — the starter creates the Lettuce
  `StatefulRedisConnection<String, String>` itself (bean
  `outboxRedisConnection`), shares it between the Redis entity locker
  and the Redis metrics cache, and closes it (connection first, then
  client) on context shutdown. A user-defined connection bean still
  wins and makes the properties inert. New `@OutboxRedisConnection`
  qualifier mirrors `@OutboxDataSource` (ADR-0024) for applications
  with several Redis connections: qualified bean → unique/`@Primary` →
  fail-fast with a dedicated `FailureAnalyzer` naming the candidates.
- **Per-event-type write serializer overrides (ADR-0025 amendment).**
  The default writer can now be overridden per event type — the
  gradual-migration knob: `event-outboxer.serializer.
  write-format-per-type.<TYPE>: <format>` in the starter (each listed
  format must belong to a registered serializer bean, else startup
  fails fast listing the registered formats),
  `OutboxEngineBuilder.writeSerializerOverride(type, serializer)` in
  plain Java (override serializers are auto-registered for reads), and
  the mirrored `OutboxTestContext.Builder.writeSerializerOverride` in
  the testkit. Reads are unaffected — deserialization keeps routing by
  the stored `payload_format`.
- **Protobuf serializer module (`event-outboxer-serializer-protobuf`,
  ADR-0026).** First shipped binary serializer on the ADR-0025 seam:
  `ProtobufEventSerializer` (format id `protobuf`, bytes lane,
  byte-exact round-trips through `payload_binary BYTEA`). Schema-first
  — payloads are protoc-generated `Message` classes; anything else is
  a publish-time `PublishSerializationException`. Deserialization uses
  the generated static `parser()` accessor, resolved reflectively once
  per class and cached; an optional `ExtensionRegistryLite` constructor
  collaborator serves proto2 extension users. The starter's new
  `ProtobufSerializerAutoConfiguration` registers the bean
  `outboxProtobufEventSerializer` additively: Jackson keeps writing
  until `event-outboxer.serializer.write-format=protobuf` selects the
  protobuf writer, and protobuf-only setups write with zero config.
  protoc runs at build time for the module's own tests only — the
  published jar ships no generated code and consumers never need
  protoc through this library.
- **Format-flexible serialization seam (ADR-0025).** New
  `EventSerializerRegistry` in `-spi`: one configured serializer writes
  every new event and stamps its `format()` into the new
  `payload_format` column; deserialization routes by the format stored
  on each event, so format migrations and mixed-version rolling
  deploys need no data rewrite. An unknown stored format raises the
  new `UnknownPayloadFormatException` (`OUTBOX-203`) through the
  FailureHandler chain (retry, never insta-DISABLE).
  `OutboxEngineBuilder.additionalSerializers(...)` (and the testkit
  equivalent) register read-only formats; the starter resolves the
  write serializer via the new
  `event-outboxer.serializer.write-format` property → single bean →
  the `outboxEventSerializer`-named bean, else fails fast listing the
  registered formats. PostgreSQL migrations
  `V006__outbox_payload_format.sql` /
  `V007__outbox_archive_payload_format.sql` (+ Liquibase changeSets)
  add the nullable-`payload` + `payload_binary BYTEA` dual lane with a
  CHECK (exactly one set) and backfill `payload_format` to
  `jackson-json`. The binary path is proven by a deliberately
  non-UTF-8 `BinaryTestEventSerializer` in the SPI test-jar: byte-exact
  round-trips through both stores, engine E2E dispatch, and
  stored-format routing tests.
- **Outbox DataSource selection in multi-DataSource applications
  (ADR-0024) — new `@OutboxDataSource` qualifier in the starter.**
  All outbox JDBC (the PostgreSQL storage adapter, both PostgreSQL
  entity lockers and the lease-table probe) now resolves its
  `DataSource` through one rule: the single `@OutboxDataSource`-marked
  bean (wins even over an unrelated `@Primary`), else the
  unique/`@Primary` bean, else startup fails fast with a
  `FailureAnalyzer` diagnosis naming the candidate beans and the fix —
  mirroring Spring Boot's `@FlywayDataSource` / `@BatchDataSource`
  pattern. The lockers unwrap a `TransactionAwareDataSourceProxy`
  handed in this way back to its raw target (their statements must run
  autocommit, ADR-0022), and the HikariCP pool-size warning resolves
  leniently — ambiguity skips the warning instead of failing startup.
- **End-to-end distributed-trace continuity (ADR-0023) — new SPI port
  `OutboxTracer` and two adapter modules `event-outboxer-tracing-otel`
  and `event-outboxer-tracing-micrometer`.** The publisher now starts a
  PRODUCER span (`outbox publish <type>`) around every insert and
  stores its context in the previously inert `trace_context` column
  (explicit `PublishOptions.traceContext` still wins, as its javadoc
  always promised); the dispatcher starts a CONSUMER span
  (`outbox process <type>`) around every handler attempt, restoring
  the stored context — and baggage — as current, recording handler
  exceptions (exception event + ERROR status), one fresh span per
  retry in the same trace. Spans carry OTel messaging semconv
  attributes plus `event_outboxer.attempt` / `.worker.id` /
  `.coalesced_into` (ADR-0021 coalesce tag). A core-internal
  `SafeOutboxTracer` shields dispatch from adapter failures; plain-Java
  users wire adapters via `OutboxEngineBuilder.tracer(...)`, the
  starter auto-detects them (micrometer wins over otel when both are
  present, `event-outboxer.tracing.enabled` opts out, a user
  `OutboxTracer` bean overrides). STORAGE.md's documented
  `trace_context` shape was corrected to the flat carrier the
  PostgreSQL adapter actually persists (single-string `baggage`
  header value, no nested objects).
  The Micrometer adapter instruments through the **Observation API**
  rather than `Tracer`/`Propagator` directly (ADR-0023 amended
  2026-08-16):
  `MicrometerOutboxTracer(ObservationRegistry, Tracer[, prefix])`
  starts a `SenderContext` observation per publish and a
  `ReceiverContext` observation per attempt, so Boot's propagating
  tracing handlers own span creation, parent extraction and carrier
  injection. This sets the current *observation* around handler
  invocation — the thing `ContextPropagatingTaskDecorator`, Reactor
  `contextCapture()` and `@Async` actually carry — so handler code
  that offloads work no longer lands in a detached trace; a consumer
  span started from an empty stored carrier adopts neither an ambient
  span nor an ambient observation on the worker thread. Span names,
  kinds and attributes are unchanged.
  As a side effect the observations register four meters —
  `<prefix>.publish{,.active}` and `<prefix>.process{,.active}`, where
  `<prefix>` is `event-outboxer.metrics.prefix` — carrying the
  low-cardinality `messaging.*` keys plus Micrometer's `error`, with no
  SLO buckets and no dashboard panels. `<prefix>.process` is **not**
  interchangeable with `event_outboxer.events.processing_time`: it
  wraps only `handler.handle(...)` and records every attempt, while the
  listener's timer measures claim → finalize on success only and is
  tagged `event_type`. Remove the extra meters with a `MeterFilter` —
  not with `management.observations.enable.*`, which would silently
  disable the spans and the stored `trace_context` as well.
  The starter's condition set grows `ObservationRegistry`.
- **Lease-table PostgreSQL entity locker (ADR-0022) — new module
  `event-outboxer-lock-postgres-lease`, selected via
  `lock.type=postgres-lease`.** `PgLeaseEntityLocker` keeps lock state in an
  `event_outboxer.entity_locks` row (migration V005, Flyway location
  `classpath:db/migration/outbox/lock` + Liquibase changelog): acquire
  is one atomic `INSERT ... ON CONFLICT DO UPDATE ... WHERE expired ...
  RETURNING`, release a fencing-token-guarded `DELETE`. No connection
  is held while the handler runs (the advisory self-deadlock scenario
  is structurally impossible; the pool warning now fires only for
  `postgres-advisory`), every statement is safe behind pgBouncer
  transaction pooling, and `lock-ttl` is finally honoured on PostgreSQL
  (crash release ≤ ttl, DB-clock arithmetic). The starter adds a
  fail-fast startup probe for the missing V005 (ordered after
  Flyway/Liquibase), a 10-minute expired-lease sweep on a dedicated
  daemon thread, and a `<metrics.prefix>.entity_locks.held` gauge.
  The entity-locker contract test gains an opt-in
  `supportsTtlExpiry()`/`forceExpire()` hook covering expiry takeover
  and stale-release semantics.
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

### Changed
- **`lock.type=redis` / `cache.type=redis` without a resolvable Redis
  connection now fail startup with a diagnosis (ADR-0027).** The
  Redis lock and cache auto-configurations no longer back off silently
  when no `StatefulRedisConnection` bean exists — previously that
  surfaced later as a cryptic `NoSuchBeanDefinitionException:
  EntityLocker` (or booted with no locker when the engine itself
  backed off). The failure now names both remedies: set
  `event-outboxer.redis.uri`/`.host`, or define a connection bean.
- **BREAKING: Java baseline raised from 17 to 25 (ADR-0017
  amendment).** The minimum runtime and build JDK is now 25
  (`maven.compiler.release=25`, enforcer `requireJavaVersion [25,)`).
  `event-outboxer.handler-executor.type=virtual` now calls
  `Thread.ofVirtual()` natively — the reflection gate and its
  bean-creation-time JDK check are gone, and JEP 491 makes the variant
  pin-free with `synchronized`-heavy JDBC drivers on every supported
  runtime. Sealed-type routing in the dispatcher uses
  pattern-matching `switch` (exhaustiveness is now compiler-enforced),
  and Java 21/25 idioms (unnamed variables, SequencedCollection
  accessors, `Thread.ofPlatform()` builders) were adopted across
  modules. Applications on JDK 17/21 must upgrade the JVM before
  taking this release.
- **BREAKING: the `postgres` lock type is split into explicit
  values.** `event-outboxer.lock.type` now accepts
  `noop | postgres-lease | postgres-advisory | redis`:
  `postgres-lease` selects the new lease-table locker,
  `postgres-advisory` the pre-ADR-0022 session-advisory locker
  (`event-outboxer-lock-postgres-advisory` artifact, unchanged
  semantics), and the old `postgres` value no longer binds — startup
  fails listing the valid values, forcing an explicit choice instead
  of a silent semantics change. When switching a fleet from advisory
  to lease, apply V005 first (the startup probe names it); during the
  rolling deploy old and new pods form disjoint exclusion domains for
  the rollout window (ADR-0022 §Rollout).
- **BREAKING (coordinates): `event-outboxer-lock-postgres` renamed to
  `event-outboxer-lock-postgres-advisory`.** Java package
  `io.github.bams22.outboxer.lock.postgres` →
  `io.github.bams22.outboxer.lock.postgres.advisory`, starter class
  `PostgresLockAutoConfiguration` →
  `PostgresAdvisoryLockAutoConfiguration`. With the lease module
  landing in the same release, each PostgreSQL locker backend now
  carries an explicit suffix. Pre-1.0 rename with no published
  consumers of the old artifact beyond 0.2.0 — update the artifactId
  and imports when upgrading. The retired coordinate keeps publishing
  as a pom-only **relocation stub** (`event-outboxer-lock-postgres`
  module) pointing at `-advisory`: Maven Central coordinates are
  immutable, so 0.1.0/0.2.0 can never be withdrawn, and a relocation
  is the supported way to redirect anyone still resolving the old
  artifactId. It targets `-advisory` rather than `-lease` because a
  relocation must preserve behaviour — the stub is not listed in
  `event-outboxer-bom`.
- **DEPRECATED: 0.1.0 and 0.2.0.** Both predate ADR-0019, ADR-0022,
  ADR-0023 and ADR-0025 and receive no fixes or backports; 0.3.0 is
  the supported baseline. They stay resolvable on Maven Central
  (coordinates are immutable) — deprecated by policy, not withdrawn.
  See the Versions section in `README.md`.
- **Poll-interval jitter.** Every wait emitted by the adaptive poller
  backoff now carries a uniform ±10% jitter, desynchronizing claim
  bursts across a fleet of JVMs deployed together (same
  thundering-herd rationale as the retry jitter in
  `ExponentialBackoffFailureHandler`). Not configurable; the backoff
  state itself stays deterministic — jitter applies on emission only.
- **API-compatibility report (japicmp).** `verify` now compares every
  module's public API against the latest published release (0.2.0) and
  writes an HTML/XML report to `target/japicmp/`. Report-only until
  1.0 — pre-1.0 SPI breaks are intentional and tracked here; the
  break-build flags flip on when preparing 1.0. Modules new since the
  baseline are skipped automatically.
- **Group-commit finalize batching (amends ADR-0014).** Concurrent
  `markProcessed` / `markForRetry` calls now coalesce into one multi-row
  statement per flush: a finalizing handler thread flushes immediately
  when the engine is idle, and while its statement is in flight other
  threads accumulate for the next flush — no timers, no dedicated
  thread, no added latency floor, up to ~batch-size× fewer finalize
  round-trips on hot types. Every row keeps its own optimistic-locking
  guard (`RETURNING id` as per-row verdicts); the call stays
  synchronous, so lock ordering, watchdog visibility, per-event listener
  callbacks and finalize-failure release are unchanged. On by default —
  `event-outboxer.dispatcher.finalize-batching: false` is the
  kill-switch; `finalize-batch-max-size` caps statement size (128). SPI:
  new `EventStore.markProcessedAll` / `markForRetryAll` with
  default-method fallbacks looping the single-row calls.
- **Coalescing dedup key — single in-flight event per key (ADR-0021).**
  `PublishOptions.dedupKey`: at most one PENDING event per
  `(event-type, dedup-key)`; a coalesced publish returns the existing
  event's id (listener/wake fire only on real inserts). The design closes
  the lost-update race of naive dedup: the unique index (migration V004)
  is partial over PENDING — an event already PROCESSING does not swallow
  a new publish — and on conflict the publisher pins the pending row with
  `SELECT ... FOR UPDATE` inside the caller's transaction, so claims
  (`SKIP LOCKED`) skip it until commit and the handler always sees the
  coalescing transaction's changes. This is work coalescing, not
  exactly-once: handler idempotency remains required. SPI:
  `EventStore.save` now returns whether the row was inserted; new
  `lockPendingByDedupKey`.
- **Stale-claim sweeper — last line of defence (amends ADR-0005).** New
  `EventStore.sweepStale(olderThan, limit)` + a maintenance task that
  returns `PROCESSING` rows older than `maintenance.stale-claim-threshold`
  to `PENDING` regardless of owner — covering rows invisible to both the
  watchdog and orphan recovery (`unknown-handler-policy=FAIL` rows, claims
  stranded by a hang before in-flight registration, double release
  failures). The threshold derives as 2 × the largest `handler-max-runtime`
  (an explicit smaller value fails startup); served by the previously
  unused V001 partial index. The in-flight registration now brackets the
  whole dispatch, so the watchdog also catches hangs in deserialization
  and entity-lock acquisition — `handler-max-runtime` budgets the full
  processing of a claim. The executor gate additionally absorbs the
  synchronous-handoff rejection race with a short bounded retry.
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
- **Entity-lock contract made honest and enforced (amends ADR-0012).**
  `lockTtl >= handlerMaxRuntime` is now validated at startup (a shorter
  TTL let the Redis lock expire under a legitimately running handler),
  and the default `lock-ttl` rose from 5m to 10m (2 × the handler
  budget). Configs that set `lock-ttl` below `handler-max-runtime` fail
  fast with instructions. The starter warns when
  `lock.type=postgres-advisory` and the total handler pool size
  reaches HikariCP's maximum-pool-size
  (each held advisory lock pins a pooled connection — self-deadlock
  risk). ADR-0012 now documents the per-backend guarantees, including
  the no-fencing best-effort nature of the Redis locker.
- **BREAKING: in-memory storage is no longer a configuration option
  (ADR-0020).** `event-outboxer.storage.type` has no default and accepts
  only `postgres`; an unconfigured outbox fails at startup with an
  actionable diagnosis (new failure analyzer) instead of silently running
  on a non-durable store that ignores transactions and loses events on
  restart. Migration: production — set
  `event-outboxer.storage.type=postgres`; Spring tests — replace the
  property with `@Import(OutboxInMemoryTestConfiguration.class)` plus
  `event-outboxer-storage-inmemory` in test scope. The
  `event-outboxer-storage-inmemory` module remains published as test
  infrastructure (contract tests, testkit, the test import). This
  supersedes the previously planned "fail-fast when a DataSource is
  present" guard with a stronger measure.
- **Jackson defaults are evolution-friendly**, as ADR-0011 always
  prescribed (the implementation had shipped the opposite):
  `FAIL_ON_UNKNOWN_PROPERTIES` and `ADJUST_DATES_TO_CONTEXT_TIME_ZONE`
  are now disabled, `FAIL_ON_NULL_FOR_PRIMITIVES` is no longer enabled
  (it breaks add-a-primitive-field evolution for record DTOs). Adding or
  removing DTO fields is now safe across a rolling deploy. Behavioral
  change vs 0.2.0; strictness is available via a custom
  `@Bean("outboxObjectMapper")`.
- `OutboxEngine` constructor takes the `EventStore`, `Clock` and a
  `HandlerExecutorManager` (engine-owned executor lifecycle); `HeartbeatTask`
  takes `WorkerInfo` instead of `WorkerId`. Plain-Java users going through
  `OutboxEngineBuilder` are unaffected.

### Fixed
- **`event_outboxer.events.disabled` counter silently never recorded in
  Spring apps.** The starter eagerly registered a *gauge* with the same
  name and tags; Micrometer then rejected the listener's lazy *counter*
  registration and `OutboxListenerRegistry` swallowed the exception.
  The backlog gauges are now a single `events.backlog{status}` meter
  (see Breaking), and `MicrometerMeterCollisionTest` pins the
  coexistence. The never-emitted `lease_renewal_mismatch` metric was
  removed from OBSERVABILITY.md.
- **Documentation drift.** ADR-0002 and the `OutboxEventPublisher`
  javadoc advertised a `no-transaction-policy` value `AUTO` that never
  existed (the enum is `FAIL | IGNORE`; the javadoc also used a stale
  `outbox.*` property prefix). ADR-0010's SPI signature block and port
  inventory were resynchronized with the shipped interfaces. ADR-0013
  wrongly claimed the Spring starter registers `LoggingOutboxListener`
  by default behind a non-existent property — the plain-Java builder
  adds it, the starter opts out. ARTIFACTS.md now lists all 15 modules.
- **Payload deserialization failures are recoverable.** They now route
  through the `FailureHandler` chain (retry with backoff, `DISABLED` only
  after the attempt budget) instead of finalizing to `DISABLED` on the
  first failure with no recovery path. A rolling deploy with mixed-version
  replicas no longer permanently disables events; `onEventSerializationError`
  still fires on every failed attempt. Amends ADR-0007.
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
- `event-outboxer-lock-postgres-advisory` — session-scoped `pg_advisory_lock`
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
  `stopped=1`, and the listener callback
  `OutboxListener.onEngineCrashed(EngineCrashedInfo)` (one of the 21)
  fires. New
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

[Unreleased]: https://github.com/bams22/event-outboxer/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/bams22/event-outboxer/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/bams22/event-outboxer/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/bams22/event-outboxer/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/bams22/event-outboxer/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/bams22/event-outboxer/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/bams22/event-outboxer/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/bams22/event-outboxer/releases/tag/v0.1.0
