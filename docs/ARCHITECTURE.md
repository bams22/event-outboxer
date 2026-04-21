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
- Cross-service messaging (use a broker instead: Kafka, RabbitMQ).
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

The library consists of 12 Maven modules:

```
event-outboxer (parent pom)
├── event-outboxer-bom                      BOM for consistent versions
├── event-outboxer-api                      Public API: interfaces, domain, exceptions
├── event-outboxer-spi                      Ports for adapters
├── event-outboxer-core                     Engine + default publisher
├── event-outboxer-storage-postgres         PG implementation of EventStore/WorkerRegistry
├── event-outboxer-storage-inmemory         In-memory adapter for tests/dev
├── event-outboxer-serializer-jackson       Jackson EventSerializer
├── event-outboxer-lock-postgres            pg_advisory_lock EntityLocker
├── event-outboxer-lock-redis               Redis/KeyDB EntityLocker
├── event-outboxer-metrics-micrometer       MicrometerOutboxListener
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
| `-lock-postgres` | `io.github.bams22.outboxer.lock.postgres.*` |
| `-lock-redis` | `io.github.bams22.outboxer.lock.redis.*` |
| `-serializer-jackson` | `io.github.bams22.outboxer.serializer.jackson.*` |
| `-metrics-micrometer` | `io.github.bams22.outboxer.metrics.micrometer.*` |
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
| `OutboxListener` | Event bus for observability (21 methods) |
| domain value objects | `Event`, `ClaimedEvent`, `PendingEvent`, `WorkerId`, `WorkerInfo` |
| exceptions | `OutboxException` hierarchy |

### SPI Ports (`event-outboxer-spi`)

| Port | Implementations | Purpose |
|---|---|---|
| `EventStore` | PostgreSQL, InMemory | CRUD + claim + finalize + reclaim |
| `WorkerRegistry` | PostgreSQL, InMemory | register / heartbeat / findDead / deregister |
| `EntityLocker` | PostgreSQL (pg_advisory), Redis, NoOp | Lock by lockKey |
| `EventSerializer` | Jackson | Serialize/deserialize payload |
| `Clock` | SystemClock, SettableClock | Time source (testability) |

### Core Engine (`event-outboxer-core`)

| Component | Purpose |
|---|---|
| `OutboxEngine` | Facade — lifecycle management |
| `Poller` (per type) | Background polling loop driven by `PollStrategy` |
| `PollStrategy` | One claim iteration (`LockAndFetchStrategy` / `FetchThenLockStrategy`) |
| `HandlerDispatcher` (per type) | Bridge between poller and executor, full processing lifecycle |
| `InFlightRegistry` | In-memory registry of events being processed (for the watchdog) |
| `HeartbeatTask` | Periodic `workerRegistry.heartbeat()` |
| `OrphanRecoveryTask` | `findDead + reclaimOrphans + removeDead` |
| `WatchdogTask` | Force-reclaim stuck handlers after `handlerMaxRuntime` |
| `EventHandlerResolver` | Resolves handlers by eventType |
| `DefaultOutboxEventPublisher` | Default publisher implementation |

---

## Data flows

### 1. Publish flow

```
Client code
    │
    │ @Transactional
    │ publisher.publish("TYPE", payload);
    ▼
DefaultOutboxEventPublisher
    │
    ├─▶ EventSerializer.serialize(payload) → String JSON
    │
    ├─▶ PendingEvent.builder()...build()
    │
    └─▶ EventStore.save(pendingEvent)
          │ (via TransactionAwareDataSourceProxy)
          │
          ▼
        PostgreSQL
          INSERT INTO outbox.events (...) VALUES (...);
          │ (within the caller's transaction)
          │
          ▼
        COMMIT / ROLLBACK of the caller determines visibility
```

### 2. Consume flow

```
Poller[SEND_EMAIL] (timer / afterDone callback)
    │
    │ batchSize = upperLimit - currentlyInFlight
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
        │       2. serializer.deserialize(payload, handler.payloadType())
        │       3. lockKey = handler.extractLockKey(payload)
        │       4. if lockKey != null: locker.tryLock(lockKey, ttl)
        │          if busy: markForRetry(lockBusyDelay); return
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
  UPDATE outbox.workers SET last_heartbeat = now() WHERE worker_id = ?
```

#### OrphanRecoveryTask (every minute)
```
1. WorkerRegistry.findDead(deadThreshold=90s, limit)
     ↓
   SELECT worker_id FROM outbox.workers
   WHERE last_heartbeat < now() - 90s AND graceful_stop=FALSE
   FOR UPDATE SKIP LOCKED LIMIT ?

2. EventStore.reclaimOrphans(deadWorkerIds)
     ↓
   UPDATE outbox.events
   SET status='PENDING', attempts=attempts+1, claimed_by=NULL, version=version+1,
       last_fail_reason='orphan-recovered: worker X'
   WHERE status='PROCESSING' AND claimed_by = ANY(deadWorkerIds)

3. WorkerRegistry.removeDead(deadWorkerIds)
     ↓
   DELETE FROM outbox.workers WHERE worker_id = ANY(deadWorkerIds)

4. Listener.onOrphansReclaimed(deadWorkerIds, eventCount)
```

All three steps run in one adapter transaction.

#### WatchdogTask (every 30s)
```
InFlightRegistry.snapshot()
    │
    ▼
For each inflight e where (now - e.claimedAt) > handlerMaxRuntime(e.eventType):
    EventStore.forceReclaim(id, claimedVersion, workerId, "stuck: exceeded handlerMaxRuntime")
        │
        ▼
      UPDATE outbox.events
      SET status='PENDING', attempts=attempts+1, claimed_by=NULL, version=version+1
      WHERE id=? AND claimed_by=? AND version=? AND status='PROCESSING'
        │
        ▼
    if reclaimed:
        InFlightRegistry.unregister(id)
        Listener.onStuckHandlerReclaimed(...)
        [physical thread leak — unavoidable in Java]
```

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
separate `outbox.event_archive` table (see
[ADR-0008](adr/0008-three-statuses-plus-optional-archive.md)).

---

## Worker lifecycle

```
Startup:
  INSERT INTO outbox.workers (worker_id, host, pid, started_at,
                               last_heartbeat, graceful_stop=FALSE)

Running:
  every heartbeatInterval (default 30s):
    UPDATE outbox.workers SET last_heartbeat = now() WHERE worker_id = ?
    ← O(1) write, independent of the number of in-flight events

Graceful shutdown:
  1. UPDATE outbox.workers SET graceful_stop = TRUE
     ← signal to orphan detection: "don't touch me"

  2. Poller.stop() — stop claiming

  3. wait for in-flight handlers (up to awaitTermination)

  4. MaintenanceExecutor stays alive until the end — heartbeat keeps running
     while any events are still in flight

  5. DELETE FROM outbox.workers WHERE worker_id = ?

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
    SELECT id FROM outbox.events
    WHERE event_type = ? AND status = 'PENDING' AND run_at <= now()
    ORDER BY priority DESC, run_at
    LIMIT ? FOR UPDATE SKIP LOCKED
)
UPDATE outbox.events e
SET status='PROCESSING', claimed_by=?, claimed_at=now(), version=version+1
FROM picked WHERE e.id = picked.id
RETURNING e.*;
```

---

## Fault tolerance

### Scenario 1: worker killed with SIGKILL

1. `last_heartbeat` in `outbox.workers` becomes stale.
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
   without a safe-kill API). Metric: `outbox.workers.leaked`.

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

```
Start (increasing phase):
  100: WorkerRegistry.register
  200: MaintenanceExecutor (heartbeat, orphan, watchdog)
  300: HandlerExecutors (per-type)
  400: Pollers

Stop (decreasing phase):
  400: Pollers — stop accepting events
  300: HandlerExecutors — awaitTermination
  200: MaintenanceExecutor — heartbeat stays up to the end
  100: WorkerRegistry.markGracefulStop + deregister
```

### 4. Bean autowiring

- All `EventHandler<?>` beans are automatically collected into
  `DefaultEventHandlerResolver`.
- All `OutboxListener` beans are registered in `OutboxListenerRegistry`.
- `FailureHandler<MyPayload>` beans are picked per type via
  `ResolvableType`.

### 5. @ConfigurationProperties

All configuration lives under `outbox.*` in application.yml — see
[docs/CONFIGURATION.md](CONFIGURATION.md). Invariant validation
(`deadThreshold >= 3 × heartbeatInterval`, etc.) runs in
`OutboxPropertiesValidator` at startup.

### 6. HealthIndicator

`OutboxHealthIndicator`:
- `DOWN` if the maintenance executor has not produced a heartbeat for more
  than `deadThreshold/2`.
- `DEGRADED` if `registry.countStuckOver(handlerMaxRuntime) > 0`.

---

## Related documents

- [Configuration](CONFIGURATION.md)
- [Storage: PostgreSQL](STORAGE.md)
- [Glossary](GLOSSARY.md)
- [Architecture Decision Records](adr/README.md)
