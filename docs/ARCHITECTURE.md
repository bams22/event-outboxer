# Architecture

This document describes the high-level architecture, module boundaries, key
components, and primary data flows. For rationale behind specific decisions,
see the [ADRs](adr/README.md).

## Table of contents

1. [Goals and scope](#goals-and-scope)
2. [Design principles](#design-principles)
3. [Module layout](#module-layout)
4. [Key components](#key-components)
5. [Data flows](#data-flows)
6. [Event lifecycle](#event-lifecycle)
7. [Worker lifecycle](#worker-lifecycle)
8. [Concurrency model](#concurrency-model)
9. [Fault tolerance](#fault-tolerance)
10. [Spring integration](#spring-integration)

---

## Goals and scope

**Goal**: give a Java application a simple and reliable way to process events
asynchronously with atomicity guarantees relative to business transactions.

**In scope**:
- Local embedded outbox within a single service (JVM + replicas).
- At-least-once guarantee — handler must be idempotent.
- Retries with exponential backoff, jitter, attempt limits.
- Heartbeat / lease mechanics for detecting crashed workers.
- Per-event-type resource isolation.

**Out of scope**:
- Cross-service messaging (use a broker instead: Kafka, RabbitMQ). The
  broker-publishing handler itself ships pre-packaged as
  `event-outboxer-relay-spring-cloud-stream` (ADR-0032) — the scope
  stays per-service either way.
- Exactly-once (fundamentally impossible in distributed systems; our
  contract is at-least-once + handler idempotency).
- Our own message broker.
- Dashboard (considered post-MVP; for now — REST API + Micrometer metrics).
- Hot-reload of configuration (restarting the pod in Kubernetes solves this).

---

## Design principles

1. **Publish atomicity within the caller's transaction**.
   `OutboxEventPublisher.publish()` participates in the current business
   transaction; rollback of the transaction means the event is not persisted.
   See [ADR-0002](adr/0002-participate-in-client-transaction.md).

2. **Storage-agnostic core**. The engine knows nothing about
   PostgreSQL/JDBC/MongoDB. Everything goes through SPI ports.
   See [ADR-0010](adr/0010-storage-agnostic-core-via-spi.md).

3. **Payload is an explicit DTO, not a lambda**. Unlike jobrunr, we do not
   serialize lambdas. See
   [ADR-0003](adr/0003-explicit-dto-payload.md).

4. **Per-event-type isolation**. Each handler has its own pool, its own
   poller, and its own claim query. See
   [ADR-0004](adr/0004-per-event-type-worker-isolation.md).

5. **At-least-once + idempotent handlers**. Duplicates are possible during
   crashes, orphan recovery, and false-positive watchdog runs. The handler
   MUST tolerate this. See
   [ADR-0015](adr/0015-at-least-once-semantics.md).

6. **Spring-friendly starter**. The core works without Spring; the starter
   provides auto-configuration, integrates with Spring's transaction
   management, and uses `ThreadPoolTaskExecutor` +
   `ContextPropagatingTaskDecorator`. See
   [ADR-0009](adr/0009-spring-task-executor-in-starter.md).

---

## Module layout

The library consists of 20 Maven modules:

```
event-outboxer (parent pom)
├── event-outboxer-bom                      BOM for consistent versions
├── event-outboxer-api                      Public API: interfaces, domain, exceptions
├── event-outboxer-spi                      Ports for adapters
├── event-outboxer-core                     Engine + default publisher
├── event-outboxer-storage-postgres         PG implementation of EventStore/WorkerRegistry
├── event-outboxer-storage-inmemory         Test infrastructure (never a production storage, ADR-0020)
├── event-outboxer-serializer-jackson       Jackson EventSerializer
├── event-outboxer-serializer-protobuf      Protobuf EventSerializer (schema-first, ADR-0026)
├── event-outboxer-lock-postgres-advisory   pg_advisory_lock EntityLocker (postgres-advisory opt-out)
├── event-outboxer-lock-postgres-lease      lease-table EntityLocker — PostgreSQL default (ADR-0022)
├── event-outboxer-lock-redis               Redis/KeyDB EntityLocker
├── event-outboxer-cache-redis              Redis/KeyDB MetricsSnapshotCache
├── event-outboxer-metrics-micrometer       MicrometerOutboxListener
├── event-outboxer-tracing-otel             OpenTelemetry OutboxTracer (ADR-0023)
├── event-outboxer-tracing-micrometer       Micrometer Tracing OutboxTracer (ADR-0023)
├── event-outboxer-relay-spring-cloud-stream  Spring Cloud Stream relay: facade + built-in handler (ADR-0032)
├── event-outboxer-admin-actuator           Actuator endpoint over OutboxAdmin
├── event-outboxer-admin-rest               REST controller over OutboxAdmin
├── event-outboxer-testkit                  Test utilities (SettableClock, ManualEngine)
└── event-outboxer-spring-boot-starter      Autoconfiguration + SmartLifecycle
```

### Dependency graph

```
      api
       ↑
       ├── spi ←── storage-postgres, storage-inmemory, lock-*, serializer-*
       │
       └── core ←── (depends on api + spi)
              ↑
              └── spring-boot-starter (+ spring-boot-autoconfigure, spring-jdbc)

      metrics-micrometer (depends only on api — implements OutboxListener)

      tracing-otel, tracing-micrometer (depend on api + spi — implement OutboxTracer, ADR-0023)

      relay-spring-cloud-stream (depends on api + serializer-jackson + spring-cloud-stream —
                                 ships a StreamOutboxPublisher facade and a built-in EventHandler)

      testkit (depends on api, spi, core, storage-inmemory)
```

### Java packages

Packages mirror the modules 1-to-1 under `io.github.bams22.outboxer.*`:

| Module | Java package |
|---|---|
| `-api` | `io.github.bams22.outboxer.api.*`, `.domain.*`, `.domain.exception.*` |
| `-spi` | `io.github.bams22.outboxer.spi.*` |
| `-core` | `io.github.bams22.outboxer.core.*` |
| `-storage-postgres` | `io.github.bams22.outboxer.storage.postgres.*` |
| `-lock-postgres-advisory` | `io.github.bams22.outboxer.lock.postgres.advisory.*` |
| `-lock-postgres-lease` | `io.github.bams22.outboxer.lock.postgres.lease.*` |
| `-lock-redis` | `io.github.bams22.outboxer.lock.redis.*` |
| `-serializer-jackson` | `io.github.bams22.outboxer.serializer.jackson.*` |
| `-serializer-protobuf` | `io.github.bams22.outboxer.serializer.protobuf.*` |
| `-metrics-micrometer` | `io.github.bams22.outboxer.metrics.micrometer.*` |
| `-tracing-otel` | `io.github.bams22.outboxer.tracing.otel.*` |
| `-tracing-micrometer` | `io.github.bams22.outboxer.tracing.micrometer.*` |
| `-relay-spring-cloud-stream` | `io.github.bams22.outboxer.relay.stream.*` |
| `-testkit` | `io.github.bams22.outboxer.testkit.*` |
| `-spring-boot-starter` | `io.github.bams22.outboxer.spring.*` |

See [ADR-0016](adr/0016-maven-module-structure.md).

---

## Key components

### Public API (`event-outboxer-api`)

| Component | Purpose |
|---|---|
| `OutboxEventPublisher` | Event publication (port with default impl in core) |
| `EventHandler<T>` | Handler for a specific event type (user-facing contract) |
| `EventOutcome` | Sealed interface of outcomes: Success / Retry / Fail / Skip |
| `FailureHandler<T>` | Chain-of-responsibility for handling failures |
| `OutboxListener` | Event bus for observability (26 methods) |
| domain value objects | `Event`, `ClaimedEvent`, `PendingEvent`, `WorkerId`, `WorkerInfo` |
| exceptions | `OutboxException` hierarchy |

### SPI Ports (`event-outboxer-spi`)

| Port | Implementations | Purpose |
|---|---|---|
| `EventStore` | PostgreSQL, InMemory | CRUD + claim + finalize + reclaim |
| `WorkerRegistry` | PostgreSQL, InMemory | register / heartbeat / findDead / deregister |
| `EntityLocker` | PostgreSQL (lease table; advisory opt-out), Redis, NoOp | Lock by lockKey |
| `EventSerializer` | Jackson, Protobuf | Serialize/deserialize payload; `format()` id persisted per event, reads routed via `EventSerializerRegistry` (ADR-0025) |
| `Clock` | SystemClock, SettableClock | Time source (testability) |
| `OutboxTracer` | OpenTelemetry, Micrometer Tracing, NoOp | Trace continuity publish → handle (ADR-0023) |

### Core Engine (`event-outboxer-core`)

| Component | Purpose |
|---|---|
| `OutboxEngine` | Facade — lifecycle management |
| `Poller` (per type) | Background polling loop driven by `PollStrategy` |
| `PollStrategy` | One claim iteration (`LockAndFetchStrategy` / `FetchThenLockStrategy`) |
| `HandlerDispatcher` (per type) | Bridge between poller and executor, full processing lifecycle |
| `InFlightRegistry` | In-memory registry of events being processed (for the watchdog), plus the abandoned set of force-reclaimed dispatches whose thread never returned |
| `HeartbeatTask` | Periodic `workerRegistry.heartbeat()` |
| `OrphanRecoveryTask` | `findDead + reclaimOrphans + removeDead` |
| `WatchdogTask` | Force-reclaim stuck handlers after `handlerMaxRuntime`, interrupt them, report threads that never return |
| `EventHandlerResolver` | Resolves handlers by eventType |
| `DefaultOutboxEventPublisher` | Default publisher implementation |

---

## Data flows

### 1. Publish flow

```
Client code
    │
    │ @Transactional
    │ publisher.publish(TYPE, payload);          // TYPE = EventType.of("TYPE", Payload.class), shared with the handler
    ▼
DefaultOutboxEventPublisher
    │
    ├─▶ validate: payload instanceof TYPE.payloadType() (ADR-0031)
    ├─▶ EventSerializer.serialize(payload) → SerializedPayload (text | bytes)
    │     + format id stamped into payload_format (ADR-0025)
    │
    ├─▶ PendingEvent.builder()...build()
    │
    └─▶ EventStore.save(pendingEvent)
          │ (via TransactionAwareDataSourceProxy)
          │
          ▼
        PostgreSQL
          INSERT INTO event_outboxer.events (...) VALUES (...);
          │ (within the caller's transaction)
          │
          ▼
        COMMIT / ROLLBACK of the caller determines visibility
          │
          ▼ (COMMIT only — registered via TransactionContext.afterCommit)
        PollerWakeHub.wake(eventType) → local poller claims immediately
```

Same-JVM publish→handle latency is therefore bounded by the handler, not
the poll interval; cross-pod pickup and delayed events remain poll-bound
(ADR-0006 amendment).

### 2. Consume flow

```
Poller[SEND_EMAIL] (adaptive timer / after-commit wake / capacity-available wake)
    │
    │ batchSize = min(claimBatchSize, executor.freeCapacity())
    │   freeCapacity < claimMinFree → no claim at all (park until free capacity reaches the
    │     refill threshold — with the default claimMinFree = 1, until a handler slot frees)
    │   full batch claimed → immediate re-poll (throughput bound by handlers, not the timer)
    ▼
PollStrategy.runOnce()
    │
    └─▶ EventStore.claim(ClaimRequest)
          │ CTE + UPDATE + RETURNING in a single SQL
          ▼
        List<ClaimedEvent>
          │
          ▼
For each claimedEvent:
    HandlerDispatcher.dispatch(claimedEvent)
        │
        ├─▶ InFlightRegistry.register(inflight)
        │
        ├─▶ executor.execute(() -> runHandlerTask)
        │        ↓ (worker thread, with TaskDecorator propagating MDC/tracing)
        │
        │   runHandlerTask:
        │       1. EventHandlerResolver.resolve(eventType) → handler
        │       2. serializerRegistry.require(event.payloadFormat())
        │            .deserialize(payload, handler.payloadType())
        │          (routed by the format stored at publish time, ADR-0025)
        │       3. lockKey = handler.extractLockKey(payload)
        │       4. if lockKey != null: locker.tryLock(lockKey, ttl[, lockWait])
        │          busy: wait up to the type's lockWait on this thread (ADR-0035;
        │          0 = one attempt), then release(lockBusyDelay) — attempts not
        │          consumed; return
        │       5. try:
        │            outcome = handler.handle(ctx, payload)
        │            switch outcome:
        │              Success → EventStore.markProcessed(id, version, worker) or DELETE
        │              Retry/Fail → FailureHandler chain → markForRetry/markDisabled/delete
        │          finally:
        │            lock?.close() [no-throws]
        │            InFlightRegistry.unregister(id)
        │            afterDoneCallback → possibly wake the poller
```

### 3. Maintenance flows (shared `maintenanceExecutor`, 2–3 threads)

#### HeartbeatTask (every 30s by default)
```
WorkerRegistry.heartbeat(workerId)
    │
    ▼
  UPDATE event_outboxer.workers SET last_heartbeat = now() WHERE worker_id = ?
```

#### OrphanRecoveryTask (every minute)
```
1. WorkerRegistry.findDead(deadThreshold=90s, limit)
     ↓
   SELECT worker_id FROM event_outboxer.workers
   WHERE last_heartbeat < now() - 90s AND graceful_stop=FALSE
   FOR UPDATE SKIP LOCKED LIMIT ?

2. EventStore.reclaimOrphans(deadWorkerIds)
     ↓
   UPDATE event_outboxer.events
   SET status='PENDING', attempts=attempts+1, claimed_by=NULL, version=version+1,
       last_fail_reason='orphan-recovered: worker X'
   WHERE status='PROCESSING' AND claimed_by = ANY(deadWorkerIds)

3. WorkerRegistry.removeDead(deadWorkerIds)
     ↓
   DELETE FROM event_outboxer.workers WHERE worker_id = ANY(deadWorkerIds)

4. Listener.onOrphansReclaimed(deadWorkerIds, eventCount)
```

All three steps run in one adapter transaction.

#### WatchdogTask (every watchdog-interval)
```
InFlightRegistry.snapshot()
    │
    ▼
For each inflight e where (now - e.claimedAt) > handlerMaxRuntime(e.eventType):
    EventStore.forceReclaim(id, claimedVersion, workerId, "stuck: exceeded handlerMaxRuntime")
        │
        ▼
      UPDATE event_outboxer.events
      SET status='PENDING', attempts=attempts+1, claimed_by=NULL, version=version+1
      WHERE id=? AND claimed_by=? AND version=? AND status='PROCESSING'
        │
        ▼
    if reclaimed:
        e.handle.interruptIfActive()          # unless interruptStuckHandler=false
        InFlightRegistry.markAbandoned(e)     # out of in-flight, into the abandoned set
        Listener.onStuckHandlerReclaimed(..., interrupted)

InFlightRegistry.abandonedSnapshot()
    │
    ▼
For each abandoned a whose thread already returned:
    InFlightRegistry.unregister(a.entry)      # nothing leaked; keeps the gauge honest
For each abandoned a still running after abandonedHandlerGrace (once per dispatch):
    Listener.onHandlerAbandoned(..., threadName, interrupted)
    [its pool slot is lost until it returns — ERROR when the interrupt was
     ignored, WARN when the type opted out of being interrupted at all]
```

Both registry sets are keyed by **dispatch**, not by event id: a
force-reclaimed row is back in `PENDING` and is regularly re-claimed by
this same JVM while the abandoned dispatch is still running, so the two
must not overwrite each other's bookkeeping.

The dispatch's own `finally` blocks clear a watchdog-issued interrupt —
once as soon as the handler unwinds, so it cannot break the finalize or
the entity-lock release, and once before the thread goes back to the
pool — and unregister the dispatch from both sets. A handler that
unwinds therefore gives its slot straight back and never poisons the
next event on that pool thread. See the ADR-0014 amendment.

---

## Event lifecycle

```
       publish()                claim                success
  ─────▶ PENDING  ─────────▶  PROCESSING  ─────────▶  [DELETED]
           ▲ ▲                  │     │                  or
           │ │                  │     │          [ARCHIVED] (opt-in)
           │ │                  │     │
           │ │  ┌───────────────┘     └──── failure+retry ────▶ PENDING
           │ │  │                                              (attempts++)
           │ │  │
           │ └──┴── orphan reclaim                     retry exhausted
           │       (worker dead)                       or explicit Fail
           │                                                │
           └── retry with delay                             ▼
                                                         DISABLED
```

Three active statuses: **PENDING / PROCESSING / DISABLED**.

Successfully processed events are **DELETEd**. The optional archive is a
separate `event_outboxer.event_archive` table (see
[ADR-0008](adr/0008-three-statuses-plus-optional-archive.md)).

---

## Worker lifecycle

```
Startup:
  INSERT INTO event_outboxer.workers (worker_id, host, pid, started_at,
                               last_heartbeat, graceful_stop=FALSE)

Running:
  every heartbeatInterval (default 5s):
    UPDATE event_outboxer.workers SET last_heartbeat = now() WHERE worker_id = ?
    ← O(1) write, independent of the number of in-flight events

Graceful shutdown (see §SmartLifecycle phases for the full sequence):
  1. Poller.stop() — stop claiming

  2. wait for in-flight handlers (up to shutdown-timeout), then
     interrupt whatever is still running

  3. UPDATE event_outboxer.events SET status = PENDING, claimed_by = NULL ...
     WHERE claimed_by = ? AND status = PROCESSING
     ← unfinished claims go back without burning an attempt

  4. UPDATE event_outboxer.workers SET graceful_stop = TRUE
     ← signal to orphan detection: "don't touch me"
     ← heartbeat kept running until here, so peers never saw us as dead

  5. stop the maintenance executor (heartbeat / orphan-recovery / watchdog)

  6. DELETE FROM event_outboxer.workers WHERE worker_id = ?
     ← skipped if step 3 failed: the graceful_stop row then stays so a
       peer reclaims the leftovers and removes it

Crash:
  DELETE does not execute → the row remains with a stale last_heartbeat
  → the next OrphanRecoveryTask picks it up
```

See [ADR-0005](adr/0005-workers-heartbeat-table.md).

---

## Concurrency model

### Optimistic locking via `version`

Every event has `version BIGINT` (default 0). Operations that change the
event state increment `version`:

| Operation | Changes version? | Changes claimed_by? | Changes status? |
|---|---|---|---|
| `save()` / publish | creates version=0 | NULL | → PENDING |
| `claim()` | **YES (+1)** | → workerId | → PROCESSING |
| `heartbeat()` (in workers!) | N/A | N/A | N/A |
| `markProcessed()` (DELETE) | — | (checks) | — (deletes row) |
| `markForRetry()` | **YES (+1)** | → NULL | → PENDING |
| `markDisabled()` | **YES (+1)** | → NULL | → DISABLED |
| `forceReclaim()` | **YES (+1)** | → NULL | → PENDING |
| `reclaimOrphans()` | **YES (+1)** | → NULL | → PENDING |

**The key invariant**: all finalize operations check
`WHERE id=? AND version=:claimedVersion AND claimed_by=:myWorkerId`.
If rowCount = 0, someone beat us (orphan recovery, watchdog), which is an
expected race in at-least-once semantics.

See [ADR-0014](adr/0014-optimistic-locking-via-version-field.md).

### `SELECT FOR UPDATE SKIP LOCKED`

Concurrent claims from different instances do not block each other — PG
skips already-locked rows. This is the foundation for horizontal
scalability of consumers.

Implementation in `LockAndFetchStrategy` (CTE + UPDATE in a single SQL):

```sql
WITH picked AS (
    SELECT id FROM event_outboxer.events
    WHERE event_type = ? AND status = 'PENDING' AND run_at <= now()
    ORDER BY priority DESC, run_at
    LIMIT ? FOR UPDATE SKIP LOCKED
)
UPDATE event_outboxer.events e
SET status='PROCESSING', claimed_by=?, claimed_at=now(), version=version+1
FROM picked WHERE e.id = picked.id
RETURNING e.*;
```

---

## Fault tolerance

### Scenario 1: worker killed with SIGKILL

1. `last_heartbeat` in `event_outboxer.workers` becomes stale.
2. After `deadThreshold` (default 90s), `OrphanRecoveryTask` on another
   instance marks the worker as dead.
3. The worker's events are returned to PENDING with `attempts++`.
4. Another worker (or the restarted one) picks them up.

### Scenario 2: handler hangs (deadlock / infinite loop)

1. The worker is alive, heartbeats keep going.
2. `WatchdogTask` sees that `now - claimedAt > handlerMaxRuntime(eventType)`.
3. `EventStore.forceReclaim()` returns the event to PENDING.
4. The event is claimed again (possibly by another thread on the same JVM).
5. The hung thread remains in the JVM (physical leak — unavoidable in Java
   without a safe-kill API). Metric: `event_outboxer.workers.leaked`.

### Scenario 3: handler threw a Throwable (including Error)

1. `try { ... } catch (Throwable t) { ... } finally { unregister }` in the
   worker code.
2. The `FailureHandler` chain makes a decision: Retry / Disable / Delete.
3. The event state is updated; the event is removed from `InFlightRegistry`.

### Scenario 4: maintenance task itself crashed

1. Heartbeats stop.
2. Spring Boot's `HealthIndicator` flips to DOWN.
3. Kubernetes liveness probe → pod restart.
4. All in-flight events are later picked up by orphan recovery on another
   instance.

---

## Spring integration

Several key integration points with the Spring Boot 3 ecosystem:

### 1. TransactionAwareDataSourceProxy

The starter automatically wraps the DataSource in
`TransactionAwareDataSourceProxy` so that `EventStore.save()` participates in
the caller's transaction. See
[ADR-0002](adr/0002-participate-in-client-transaction.md).

### 2. ThreadPoolTaskExecutor + ContextPropagatingTaskDecorator

The per-type handler executor is a Spring `ThreadPoolTaskExecutor` with
`ContextPropagatingTaskDecorator` (bundled with Spring Boot 3.5.6,
which is the pinned baseline — see
[ADR-0017](adr/0017-java-25-and-spring-boot-3-5-baseline.md)) applied
automatically. This propagates MDC, Micrometer Observation, and the
Security context to worker threads. See
[ADR-0009](adr/0009-spring-task-executor-in-starter.md).

### 3. SmartLifecycle phases

The Spring Boot starter exposes the engine through a single
`OutboxSmartLifecycle` bean at phase **20000** — late enough that
`DataSourceAutoConfiguration`, connection pools and Flyway migrations
are already initialised, early enough that the engine is polling by
the time Actuator reports readiness. Boot stops beans in descending
phase order, so the engine shuts down before application connection
pools are closed.

The internal order of the stop sequence — executed by
`OutboxEngine.stop(Duration)` — is:

```
0. state = STOPPING
   → OutboxEventPublisher keeps working: publish() never consults the
     engine state, so events written by the last in-flight requests are
     persisted and picked up by a peer replica or by this instance
     after restart.

1. Poller.stop() on every per-type poller
   → stop claiming new events: flip the running flag, interrupt the
     dedicated platform thread, join it (the join budget is
     shutdown-timeout split evenly across pollers).
   → a claim statement interrupted mid-flight rolls back with its
     transaction; a claim that completed while the executor was
     already closed is rejected on submit and released straight back
     to PENDING (Poller.submit → HandlerDispatcher.releaseRejected).

2. Drain handler executors — HandlerExecutorManager.drain(timeout)
   → executor.shutdown() on every per-type ExecutorService: no new
     tasks are accepted, running handlers finish normally.
   → awaitTermination against ONE shared deadline (shutdown-timeout
     from the start of the drain, not per type).
   → if the deadline passes, executor.shutdownNow(): queued-but-never-
     started tasks are dropped and still-running handlers are
     interrupted. This interrupt is unconditional — the per-type
     interrupt-stuck-handler flag governs the watchdog only, not the
     shutdown drain.

3. EventStore.releaseClaimed(workerId)
   → every row still PROCESSING under this worker (dropped from the
     queue or interrupted by the drain) goes back to PENDING WITHOUT
     incrementing attempts. Nothing waits for orphan recovery: the
     events become claimable by peers immediately.
   → if this statement fails, the worker row is kept (flagged
     graceful_stop) so a peer's orphan recovery reclaims the leftovers
     — the row must outlive its claims.

4. WorkerRegistry.markGracefulStop(workerId)
   → signal to peer replicas' orphan-recovery: "don't reclaim me,
     I'm shutting down cleanly" (graceful_stop=TRUE excludes the row
     from the findDead partial index; see ADR-0005).
   → fire OutboxListener.onWorkerGracefulStop.

5. MaintenanceScheduler.stop(timeout)
   → shutdown() + awaitTermination on the ScheduledExecutorService
     that drives heartbeat / orphan-recovery / watchdog tasks;
     shutdownNow() if the timeout is exceeded.

6. WorkerRegistry.deregister(workerId)   (skipped when step 3 failed)
   → DELETE FROM event_outboxer.workers WHERE worker_id = ?
   → fire OutboxListener.onWorkerDeregistered.

7. state = STOPPED
```

A handler interrupted in step 2 after it already applied its side
effect is redelivered — this is the at-least-once contract
(ADR-0015) and the reason handlers must be idempotent. Handlers that
are not interrupt-safe (long JDBC work, where `pgjdbc` may close the
connection on interrupt) should finish within `shutdown-timeout`.

#### Sizing the shutdown budget

The default `shutdown-timeout` is 30 seconds, tuned via
`event-outboxer.maintenance.shutdown-timeout`. If your handlers may
legitimately run longer than 30 s, raise it — the alternative
(`shutdownNow()` plus redelivery) is correct but wastes work.

`shutdown-timeout` is not a single budget for the whole sequence: it
bounds the poller join (step 1), the handler drain (step 2) and the
maintenance stop (step 5) separately. Pollers and maintenance threads
react to `interrupt()` immediately, so the observed stop time is
normally ≈ the drain time, but the worst case is roughly
**3 × shutdown-timeout**. Size the orchestrator's grace period
(`terminationGracePeriodSeconds` on k8s) accordingly.

#### Ordering with Spring Boot's web graceful shutdown

With `server.shutdown: graceful`, the embedded web server stops at
phase `SmartLifecycle.DEFAULT_PHASE - 1024` — far above the engine's
20000. Boot stops phases in descending order, so on SIGTERM:

1. the web server stops accepting connections and drains in-flight
   requests (`spring.lifecycle.timeout-per-shutdown-phase`, default
   30 s) — their `@Transactional publish()` calls complete and commit;
2. only then does the outbox engine stop polling and drain its
   handlers;
3. afterwards Boot closes connection pools and the DataSource.

`spring.lifecycle.timeout-per-shutdown-phase` does not cut the engine
short: `OutboxSmartLifecycle.stop(Runnable)` runs `OutboxEngine.stop()`
synchronously on the calling thread. The total shutdown time is
therefore the web drain plus the outbox drain — budget the grace
period as `timeout-per-shutdown-phase + shutdown-timeout` (plus the
worst-case margin above).

Readiness: `/actuator/health/outbox` reports `DOWN` as soon as the
engine leaves RUNNING; it affects `/actuator/health/readiness` only
when `event-outboxer.health.probe-groups` lists `readiness`. Boot
itself flips the readiness state to `REFUSING_TRAFFIC` on context
close regardless, so the pod leaves the Service endpoints either way.

### 4. Bean autowiring

- All `EventHandler<?>` beans are automatically collected into
  `DefaultEventHandlerResolver`.
- All `OutboxListener` beans are registered in `OutboxListenerRegistry`.
- `FailureHandler` beans are registered through the
  `@OutboxFailureHandler` qualifier — global without a value, per event
  type with values — or built from `event-outboxer.event-types.*.failure.*`
  (ADR-0030); beans with neither are reported at startup, never
  silently used or ignored.

### 5. @ConfigurationProperties

All configuration lives under `event-outboxer.*` in application.yml — see
[docs/CONFIGURATION.md](CONFIGURATION.md). Invariant validation
(`deadThreshold >= 3 × heartbeatInterval`, etc.) runs in the
constructors of the core config records (`MaintenanceConfig`,
`EventTypeConfig`, `DispatcherConfig`) when the starter maps the bound
properties, so a bad value aborts context refresh.

### 6. HealthIndicator

`OutboxHealthIndicator` is registered when Spring Boot Actuator is on
the classpath and exposes `/actuator/health/outbox`:

- `UP` when the engine is `RUNNING` and the store's metrics snapshot
  is reachable.
- `DOWN` when the engine is `STOPPED` / `STOPPING`, or when the
  metrics snapshot threw (DB unreachable).

The details block carries the engine state, totals from
`EventStore.metricsSnapshot()`, the snapshot timestamp and the
`workerId`. See [docs/OBSERVABILITY.md §Health indicator](OBSERVABILITY.md#health-indicator)
for the full field reference and operational playbook.

### 7. Feature parity: starter vs plain core

The starter exists to wire things automatically. It must **not** change
semantics relative to the core; everything that affects correctness is
implemented once in `event-outboxer-core`. The table below makes that
contract reviewable — when adding a feature, walk each row and either
answer for both columns or explain why one side deliberately differs.
This audit exists because past drift (e.g. a listener-forwarding
decorator embedded in the default failure chain on one path and not the
other) has been a real source of bugs.

| Feature | `event-outboxer-core` | `event-outboxer-spring-boot-starter` |
|---|---|---|
| Transaction participation | `TransactionContext` SPI — caller wires the implementation | `SpringTransactionContext` + `TransactionAwareDataSourceProxy` auto-wired |
| Poller | raw `Thread` per event type (`Poller.java`) | inherited from core |
| Maintenance (heartbeat / orphan / watchdog / crash-check) | `ScheduledExecutorService` owned by `MaintenanceScheduler` | inherited from core |
| Handler executor shape | `Function<EventTypeConfig, ExecutorService>` supplied to `OutboxEngineBuilder` | `HandlerExecutorFactory.platform()` / `.virtual()` picked by `event-outboxer.handler-executor.type`; Spring `ThreadPoolTaskExecutor` exposed as `ExecutorService` via `SpringTaskExecutorAdapter` |
| Context propagation (MDC / Observation / Security) | none built-in — caller decorates their own `Executor` | `ContextPropagatingTaskDecorator` default; user can swap via `@Bean TaskDecorator` |
| Failure-chain default | `FailureHandlers.defaults()` = `Log → MaxRetries → ExponentialBackoff` | identical when `event-types.*.failure.*` is unset; otherwise the starter builds the chain from YAML through the same `FailureHandlers.builder()` (thin merge, ADR-0030) — never re-wraps a user bean |
| Retry / disable / delete listener emission | `HandlerDispatcher` fires listener callbacks after storage commit | identical — starter adds no second emission path (see [ADR-0007](adr/0007-failure-handler-chain-of-responsibility.md) §Q25) |
| `LoggingOutboxListener` | auto-added by `OutboxEngineBuilder` (plain-Java default) | explicitly opted out (`includeLoggingListener(false)`) to avoid double-logging with the engine's own SLF4J calls |
| `MicrometerOutboxListener` | separate module; caller registers manually | auto-registered by `MicrometerAutoConfiguration` when Micrometer is on the classpath |
| Trace continuity publish → handle (`OutboxTracer`, ADR-0023) | NOOP by default; caller wires an adapter via `builder.tracer(new OtelOutboxTracer(otel))` | auto-detected: `MicrometerTracingAutoConfiguration` (Boot `Tracer`+`Propagator` beans) wins over `OtelTracingAutoConfiguration` (`OpenTelemetry` bean or `GlobalOpenTelemetry`); switch: `event-outboxer.tracing.enabled`; deferred events (`runAt` beyond `event-outboxer.tracing.link-threshold`, 1m) get a linked root consumer span instead of a child |
| Backlog gauges (pending / processing / disabled / oldest-age) | none built-in; caller wires own `Gauge.builder(...)` off `EventStore.metricsSnapshot()` | `MicrometerAutoConfiguration.outboxBacklogGauges` registers per-type gauges for every `EventHandler` bean + a global `oldest_claimed_age_seconds`; reads go through the `MetricsSnapshotCache` SPI |
| Health surface | `OutboxEngine.state()` + `OutboxEngine.isLifecycleActive()` + `OutboxListener.onEngineCrashed` | `OutboxHealthIndicator` + `event-outboxer.health.probe-groups` `EnvironmentPostProcessor` that folds `outbox` into Actuator liveness / readiness groups |
| Crash detection | `EngineHealthCheckTask` in the maintenance scheduler; flips `state()` → `STOPPED`, fires `onEngineCrashed` | inherited — starter only surfaces the result via the health indicator |
| Flyway migrations | classpath `event-outboxer/migration/{core,archive,lock}` with `${eventOutboxerSchema}` placeholder; caller runs Flyway | starter-managed Flyway instance (`OutboxFlywayAutoConfiguration`, ADR-0028): fixed locations, own history table inside `event-outboxer.storage.schema`, optional dedicated connection via `event-outboxer.flyway.url`; the placeholder is also fed into the application's instance for the `enabled=false` path |
| Liquibase changelog | classpath `db/changelog/outbox/{core,archive}/changelog.xml` with the same parameter name | `OutboxLiquibaseParameterEnvironmentPostProcessor` auto-feeds `spring.liquibase.parameters.eventOutboxerSchema` |
| Serializer | `EventSerializer` SPI — caller wires write serializer via `eventSerializer(...)`, read-only formats via `additionalSerializers(...)` | `JacksonSerializerAutoConfiguration` with configurable `ObjectMapper` (qualified `outboxObjectMapper` wins, primary next, defaults last) + additive `ProtobufSerializerAutoConfiguration` (ADR-0026); write serializer resolved per `event-outboxer.serializer.write-format` (ADR-0025) |
| Worker registry | `WorkerRegistry` SPI per adapter | adapter-specific auto-config (PG / in-memory) |
| Engine lifecycle | manual `engine.start()` / `engine.stop(timeout)` | `OutboxSmartLifecycle` at phase 20000 (auto-start on refresh, drain on shutdown) |
| Configuration | programmatic via `OutboxEngineBuilder` | `@ConfigurationProperties("event-outboxer")` (`OutboxProperties`) → thin merge → core config records (invariants validated in their constructors) |
| Metrics-snapshot cache | `MetricsSnapshotCache` SPI with `noop()` / `inMemory(Clock, ttl)` static factories; caller passes the one they want into `PostgresEventStore` | `CacheAutoConfiguration` picks `memory` (default) / `noop` per `event-outboxer.cache.type`; `RedisCacheAutoConfiguration` selects the Lettuce-backed variant from `event-outboxer-cache-redis` when `type=redis`, resolving the connection per ADR-0027 (`@OutboxRedisConnection`-qualified bean → unique/`@Primary` → the `RedisConnectionAutoConfiguration`-managed connection from `event-outboxer.redis.*`); user `@Bean MetricsSnapshotCache` overrides everything |

**Invariant.** If you are tempted to ship something only in the starter
(auto-instantiation, YAML binding, `ObjectProvider` resolution), it must
either expose the same capability in core as a programmatic API or be
pure convenience with no runtime-behaviour difference. Semantic features
belong to the core; the starter only wires, it never adds behaviour.

---

## Related documents

- [Configuration](CONFIGURATION.md)
- [Storage: PostgreSQL](STORAGE.md)
- [Observability](OBSERVABILITY.md)
- [Testing handlers with the testkit](TESTING.md)
- [Glossary](GLOSSARY.md)
- [Architecture Decision Records](adr/README.md)
