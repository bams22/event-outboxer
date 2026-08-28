# event-outboxer-core

The engine. Polling, dispatching, failure handling, maintenance and the
default publisher — everything that decides *when* and *how* an event is
processed, with **no Spring dependency** (mechanically enforced by a
`maven-enforcer-plugin` `bannedDependencies` rule banning
`org.springframework:*`, see [ADR-0010](../adr/0010-storage-agnostic-core-via-spi.md)).

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-core` |
| Java package | `io.github.bams22.outboxer.core.*` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `slf4j-api`, `jspecify` |
| Spring | **None** — banned by the enforcer |

## Why it exists

The core isolates all outbox *semantics* — claim ordering, optimistic
locking, retry decisions, orphan recovery — from any concrete storage,
serialization format or DI framework. Storage adapters implement SPI
ports; the Spring starter only wires beans. Anything that affects
correctness is implemented exactly once, here
(see [ARCHITECTURE.md §Feature parity](../ARCHITECTURE.md#7-feature-parity-starter-vs-plain-core)).

## What it does

### Engine and lifecycle (`engine`)

- **`OutboxEngine`** — the facade. `start()` registers the worker,
  starts the maintenance scheduler, the per-type handler executors and
  the per-type pollers; `stop(Duration)` runs the graceful drain
  sequence (stop pollers → drain executors → release still-claimed
  rows back to `PENDING` without burning an attempt → mark
  `graceful_stop` → stop maintenance → deregister). `state()` reports
  `STOPPED / RUNNING / STOPPING`; `markCrashed(...)` flips the state
  for health surfaces while keeping the lifecycle active so shutdown
  still drains cleanly.
- **`OutboxEngineBuilder`** — the plain-Java assembly path (see
  [Usage](#usage-without-spring) below).
- **`HandlerExecutorManager`** — owns one `ExecutorService` per event
  type; exposes free capacity to the poller through
  `HandlerExecutorGate` and wakes the poller the moment a handler slot
  frees.

### Polling (`polling`)

One `Poller` per event type on a dedicated daemon thread
(`outbox-poller-<eventType>`). Each tick claims
`min(claimBatchSize, freeCapacity)` events via `PollStrategy`
(default `LockAndFetchStrategy` — a single
`EventStore.claim(...)` call, `FOR UPDATE SKIP LOCKED` in the
PostgreSQL adapter). `AdaptiveWaiter` grows the wait after empty polls
(`pollMultiplier`, capped at `pollMaxInterval`, ±10% jitter) and
resets it on any hit; a full batch triggers an immediate re-poll, so
sustained throughput is bounded by the handler pool and the database,
not the timer. `PollerWakeHub` lets the publisher wake the local
poller right after the publishing transaction commits — same-JVM
latency is milliseconds regardless of poll intervals
([ADR-0006](../adr/0006-no-listen-notify-in-mvp.md) amendment).

### Dispatch (`dispatch`)

`HandlerDispatcher` processes one claimed event end-to-end: resolve
handler → deserialize (routed by the `payload_format` stored at
publish time, [ADR-0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md))
→ acquire the optional entity lock → invoke the handler → route the
`EventOutcome` to a finalize call and listener notifications. Key
collaborators:

- **`InFlightRegistry`** — in-memory registry of events being
  processed; the bracket covers the *whole* dispatch (deserialization
  and lock acquisition included), so `handler-max-runtime` budgets the
  full pipeline, not just `handle()`.
- **`GroupCommitEventStore`** — decorator that coalesces concurrent
  `markProcessed` / `markForRetry` calls into one multi-row statement
  (group commit, no timers, no added latency; an idle engine degrades
  to plain single-row calls). Controlled by
  `DispatcherConfig.finalizeBatching` (on by default).
- **`EventHandlerResolver`** / **`FailureHandlerResolver`** — map
  `eventType → EventHandler`, and resolve the effective failure chain
  as `EventHandler.failureHandler()` → per-type registration →
  library-wide default.

Contention and backpressure never consume the retry budget: unknown
handler (`SKIP` policy), busy entity lock, saturated executor and
finalize failures all *release* the event back to `PENDING` with a
delay instead of calling `markForRetry`.

### Failure handling

The chain-of-responsibility `FailureHandler` classes live in
[`event-outboxer-api`](event-outboxer-api.md); core resolves and
invokes them. The library default is
`FailureHandlers.defaults()` = `Log(WARN) → MaxRetries(10, DISABLE) →
ExponentialBackoff(base 5s, ×2.0, cap 1h, jitter 0.2)`
([ADR-0007](../adr/0007-failure-handler-chain-of-responsibility.md)).
Deserialization failures also route through the chain — a mixed-version
fleet often heals them on retry; a truly poisoned payload ends up
`DISABLED` once the attempt budget is exhausted.

### Publishing (`publish`)

`DefaultOutboxEventPublisher` implements the `OutboxEventPublisher`
API: transaction-policy check (`NoTransactionPolicy.FAIL | IGNORE`) →
serialize → PRODUCER span + trace context capture → `EventStore.save`
→ `onEventPublished` → after-commit poller wake. Dedup-keyed publishes
(`PublishOptions.dedupKey`, [ADR-0021](../adr/0021-dedup-key-single-inflight-per-key.md))
use an insert-first coalescing loop: either the insert wins, or the
existing `PENDING` row is row-locked into the caller's transaction and
its id returned, or — if the row was already claimed — the insert is
retried (bounded, then `PublishFailedException`).

The `TransactionContext` port abstracts "is a transaction active" and
"run after commit"; the starter binds it to Spring's transaction
synchronization, plain-Java users pass their own (defaults to
`alwaysActive()`).

### Maintenance (`maintenance`)

One shared 3-thread `ScheduledExecutorService`
(`outbox-maintenance-N`) runs:

| Task | Default cadence | What it does |
|---|---|---|
| `HeartbeatTask` | 5s | refresh `event_outboxer.workers`; re-registers if the row was reaped |
| `OrphanRecoveryTask` | 30s | `findDead` → `reclaimOrphans` → `removeDead` for silent workers |
| `WatchdogTask` | 10s | force-reclaim in-flight events past `handler-max-runtime` |
| `StaleClaimSweeperTask` | 5m | fleet-wide sweep of long-`PROCESSING` rows invisible to the two above |
| `EngineHealthCheckTask` | 10s | detect a dead poller thread → `markCrashed` + `onEngineCrashed` |
| `RetentionTask` | 1h | opt-in archive / `DISABLED` purge via `OutboxAdmin` ([ADR-0019](../adr/0019-admin-and-retention-surface.md)) |

Together with the shutdown-time `releaseClaimed`, these form the
at-least-once safety net: no event can stay stuck in `PROCESSING`
forever ([ADR-0015](../adr/0015-at-least-once-semantics.md)).

### Configuration records (`config`)

Immutable Lombok-built records validating their invariants in the
constructor — a bad value fails fast at assembly time:

- **`EventTypeConfig`** — per-type poll intervals, claim batch size,
  handler pool/queue size, `handlerMaxRuntime`, `lockTtl`
  (must be ≥ `handlerMaxRuntime`).
- **`MaintenanceConfig`** — heartbeat/dead-threshold
  (`deadThreshold ≥ 3 × heartbeatInterval`), recovery and watchdog
  cadences, `shutdownTimeout`, stale-claim knobs.
- **`DispatcherConfig`** — unknown-handler policy, contention retry
  delays, finalize batching.
- **`RetentionConfig`** — both thresholds default to off; deleting
  data is never a surprise default.

The full knob-by-knob reference (defaults, invariants) is in
[CONFIGURATION.md](../CONFIGURATION.md) — every YAML property maps
1-to-1 onto these records.

## When to use it

- **With Spring Boot** you never add core explicitly — the
  [starter](event-outboxer-spring-boot-starter.md) brings it
  transitively and assembles it from `event-outboxer.*` properties.
- **Without Spring** (plain Java, another DI framework, Ktor, etc.)
  add core directly plus a storage adapter and a serializer, and
  assemble via `OutboxEngineBuilder`.
- **Never** implement business logic against core internals — the
  public surface for applications is
  [`event-outboxer-api`](event-outboxer-api.md); core types beyond the
  builder and the config records are wiring detail.

## Usage without Spring

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-core</artifactId>
</dependency>
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-storage-postgres</artifactId>
</dependency>
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-serializer-jackson</artifactId>
</dependency>
```

Minimal assembly — `eventStore`, `workerRegistry`, `eventSerializer`
and at least one handler are required, everything else has defaults:

```java
OutboxEngine engine = new OutboxEngineBuilder()
    .eventStore(store)                  // e.g. PostgresEventStore
    .workerRegistry(registry)           // e.g. PostgresWorkerRegistry
    .eventSerializer(new JacksonEventSerializer(mapper))
    .handler(new SendEmailHandler(mailer))
    .build();

engine.start();
OutboxEventPublisher publisher = engine.publisher();
publisher.publish("SEND_EMAIL", new SendEmailPayload("me@x.io"));
// ...
engine.stop();                          // graceful drain, default 30s timeout
```

Commonly tuned builder options:

```java
new OutboxEngineBuilder()
    // ... required collaborators ...
    .entityLocker(locker)                       // default: EntityLocker.NOOP
    .transactionContext(txContext)              // default: alwaysActive()
    .noTransactionPolicy(NoTransactionPolicy.FAIL) // builder default is IGNORE!
    .defaultEventTypeConfig(EventTypeConfig.defaults().toBuilder()
        .handlerPoolSize(10)
        .build())
    .eventTypeConfig("SEND_EMAIL", customCfg)   // per-type override
    .defaultFailureHandler(FailureHandlers.<Object>builder()
        .withLogging(Level.WARN)
        .withMaxAttempts(5, ExhaustedAction.DISABLE)
        .withExponentialBackoff(Duration.ofSeconds(30), 2.0, Duration.ofHours(2), 0.2)
        .build())
    .maintenance(MaintenanceConfig.defaults())
    .dispatcher(DispatcherConfig.defaults())
    .listener(myListener)                       // in addition to LoggingOutboxListener
    .tracer(new OtelOutboxTracer(otel))         // default: NOOP
    .tracingLinkThreshold(Duration.ofMinutes(1)) // default 1m; events scheduled further ahead get a linked root consumer span, null = never
    .clock(clock)                               // default: Clock.system()
    .admin(outboxAdmin)                         // required for retention
    .retention(RetentionConfig.builder()
        .archiveOlderThan(Duration.ofDays(30)).build())
    .writeSerializerOverride("ORDER_CREATED", protobufSerializer)
    .build();
```

Two defaults to be aware of in plain-Java setups:

- **`noTransactionPolicy` defaults to `IGNORE` on the builder** (the
  starter's product default is `FAIL`). If you want publish-outside-
  transaction to fail fast, set it explicitly — and supply a real
  `TransactionContext`, otherwise `alwaysActive()` makes the check a
  no-op.
- **Context propagation is not built in.** The starter decorates
  executors with `ContextPropagatingTaskDecorator`; plain-Java users
  who need MDC/tracing on handler threads must pass their own
  decorated executors via
  `handlerExecutorFactory(Function<EventTypeConfig, ExecutorService>)`.

## Threads it creates

| Thread(s) | Name | Purpose |
|---|---|---|
| 1 per event type | `outbox-poller-<type>` | claim loop |
| pool per event type | `outbox-handler-N` | handler execution (default `ThreadPoolExecutor`, `core == max`, bounded queue) |
| 3 shared | `outbox-maintenance-N` | heartbeat, orphan recovery, watchdog, health check, sweeper, retention |

All threads are daemons; `stop()` joins and drains them.

## Related

- [ARCHITECTURE.md](../ARCHITECTURE.md) — data flows, event/worker lifecycle, concurrency model.
- [CONFIGURATION.md](../CONFIGURATION.md) — every knob with defaults and invariants.
- ADRs: [0004](../adr/0004-per-event-type-worker-isolation.md) (per-type isolation),
  [0007](../adr/0007-failure-handler-chain-of-responsibility.md) (failure chain),
  [0010](../adr/0010-storage-agnostic-core-via-spi.md) (Spring-free core),
  [0014](../adr/0014-optimistic-locking-via-version-field.md) (optimistic locking),
  [0015](../adr/0015-at-least-once-semantics.md) (at-least-once),
  [0021](../adr/0021-dedup-key-single-inflight-per-key.md) (dedup key).
