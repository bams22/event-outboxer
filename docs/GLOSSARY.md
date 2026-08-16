# Glossary

Terminology used throughout the event-outboxer documentation and code.

## A

**abandoned handler** — a force-reclaimed dispatch that was still
running `abandoned-handler-grace` later: it either ignored the interrupt
or belongs to a type with `interrupt-stuck-handler: false` and was never
asked to stop. The event is safe (back in PENDING); the thread is lost
to its per-type handler pool until it returns on its own. Surfaced by
`OutboxListener.onHandlerAbandoned`, the counter
`event_outboxer.handler.abandoned` and the gauge
`event_outboxer.handler.abandoned_threads`. Usual cause: blocking I/O
with no timeout configured on the client. Tracked per dispatch, not per
event id — the same event is normally re-claimed by this JVM while the
abandoned dispatch is still running.

**at-least-once** — delivery guarantee: the event will be processed **at
least once**, possibly more (due to crashes, orphan recovery, retry after
a transient exception). The handler MUST be idempotent. See
[ADR-0015](adr/0015-at-least-once-semantics.md).

## C

**claim** — atomic operation that moves an event from PENDING to
PROCESSING under a single worker. In the PG adapter it is implemented as a
CTE + UPDATE with `FOR UPDATE SKIP LOCKED`. See [docs/STORAGE.md](STORAGE.md).

**ClaimedEvent** — record returned by `EventStore.claim()`. Contains all
event data plus `claimedVersion` (the version at the moment of claim).
`claimedVersion` is passed into finalize methods for optimistic
concurrency control.

**Clock** — SPI port, time source. In production — `Clock.system()`
(`Instant::now`); in tests — `SettableClock` from the testkit.

**concurrent completion conflict** — a race situation where a finalize
operation returns `false` because `version` has changed (another worker /
orphan recovery / watchdog won). Expected under at-least-once;
`event_outboxer.events.concurrent_completion_conflict` is the corresponding
metric.

## D

**dead-threshold** — how many seconds of silence before a worker is
considered dead. Default 90s (= 3 × heartbeat-interval). Invariant:
`dead-threshold >= 3 × heartbeat-interval` (protects against GC-stall
false positives).

**DISABLED** — terminal status of an event: processing exhausted its
attempts or the handler explicitly returned `Fail`. Not processed
automatically; re-activation via admin API.

**dispatcher** (HandlerDispatcher) — core-engine component that performs
the full lifecycle for a single claimed event: deserialize →
extractLockKey → tryLock → handler.handle() → finalize. Per-type.

## E

**Event** — value object, an immutable DB representation of an event. Used
for admin operations (findById, findAll).

**EventContext** — context passed to `handler.handle()`. Contains event
id, attempts, createdAt, claimedAt, trace_context.

**EventHandler<T>** — user contract. Registered as a Spring bean, picked
up automatically by the core engine via `eventType()`.

**EventOutcome** — sealed interface of the processing result: `Success` |
`Retry(reason, delayOverride, cause)` | `Fail(reason, cause)` |
`Skip(reason)`.

**EventSerializer** — SPI port for payload serialization. Declares a
stable `format()` id and works over `SerializedPayload` (text or bytes
lane), so binary formats plug in without SPI changes; shipped
implementations are Jackson JSON (text lane) and Protobuf (bytes
lane). See [ADR-0011](adr/0011-jackson-json-only-in-mvp.md),
[ADR-0025](adr/0025-binary-capable-serializer-spi-and-payload-format.md)
and [ADR-0026](adr/0026-protobuf-serializer-module.md).

**EventSerializerRegistry** — immutable lookup of serializers by format
id. The dispatcher uses it to deserialize each claimed event with the
serializer that wrote it (`payload_format`), which keeps rolling deploys
and format migrations safe. See
[ADR-0025](adr/0025-binary-capable-serializer-spi-and-payload-format.md).

**EventStore** — SPI port for persistent event storage. Methods: `save`,
`claim`, `markProcessed`, `markForRetry`, `markDisabled`, `forceReclaim`,
`reclaimOrphans`, `findById`, `metricsSnapshot`.

**EntityLocker** — SPI port for business-key locks. Implementations:
PostgreSQL (lease table `entity_locks`, ADR-0022; session-scoped
`pg_advisory_lock` as the `postgres-advisory` opt-out), Redis/KeyDB,
NoOp.

**event_type** — string identifier of the event type. Connects the payload
in the DB to `EventHandler.eventType()`. Must stay stable across releases.

**extractLockKey** — default method on `EventHandler<T>` that returns an
optional lockKey to serialize processing by business key. Null means "no
lock". Computed on the worker after deserialization, not stored in the DB.
See [ADR-0012](adr/0012-extract-lock-key-on-handler.md).

## F

**FailureContext** — record passed to `FailureHandler.onFailure()`.
Contains ClaimedEvent, payload, outcome, cause, attempts, now.

**FailureDecision** — sealed interface of the FailureHandler decision:
`RetryAt(when, reason)` | `Disable(reason)` | `Delete(reason)`.

**FailureHandler<T>** — chain-of-responsibility decorator for handling
failures. Built-in implementations: `LogFailureHandler`,
`MaxRetriesFailureHandler`, `ExponentialBackoffFailureHandler`,
`FixedDelayFailureHandler`, `NoRetryFailureHandler`. Listener callbacks
for retry/disable/delete are emitted by the engine dispatcher, not by
the chain. See
[ADR-0007](adr/0007-failure-handler-chain-of-responsibility.md).

**Fetch-then-Lock strategy** — two-phase claim (SELECT → UPDATE-with-
version-check) for storages without `SKIP LOCKED`. Not used in MVP.

**force reclaim** — forced return of an event from PROCESSING to PENDING
from the WatchdogTask (when `handlerMaxRuntime` is exceeded). Differs from
orphan reclaim in that the worker is still alive. The handler thread is
interrupted at the same time (per-type `interrupt-stuck-handler`,
default on) because its finalize is already doomed to lose the version
check — see [ADR-0014](adr/0014-optimistic-locking-via-version-field.md).

## H

**handler-max-runtime** — upper bound on the execution time of a single
`handler.handle()`. The watchdog force-reclaims events after that. Default
30 min. See [ADR-0005](adr/0005-workers-heartbeat-table.md).

**handler pool** — per-type `ThreadPoolTaskExecutor` in the Spring starter
running handlers of that type. Core-type:
`java.util.concurrent.Executor`.

**heartbeat** — periodic `UPDATE event_outboxer.workers SET last_heartbeat=now()`.
One row per JVM, independent of the in-flight count. Default interval —
30s.

**heartbeat-interval** — how often workers emit heartbeats.

## I

**idempotency** — property of a handler: a repeated call with the same
payload does not cause a double side effect. Mandatory under
at-least-once.

**InFlightRegistry** — in-memory structure (one per JVM) holding all
`InFlightEvent`s processed by this JVM. Used by the WatchdogTask.

**InFlightEvent** — record describing an event "in progress":
`eventId, eventType, claimedVersion, claimedAt, workerId`.

## L

**LockAndFetch strategy** — atomic claim (CTE + UPDATE in a single SQL).
Default for the PG adapter in MVP.

**LockHandle** — `AutoCloseable` handle for a lock acquired via
`EntityLocker.tryLock()`. `close()` is no-throws
(see Q5 in [ADR-0013](adr/0013-outbox-listener-for-observability.md)).

**lockKey** — string key used to serialize processing by business key.
Computed by the handler via `extractLockKey(payload)`.

## M

**maintenance executor** — shared `ScheduledExecutorService` (2–3 threads)
for HeartbeatTask, OrphanRecoveryTask, WatchdogTask.

**markProcessed / markForRetry / markDisabled** — finalizing operations on
EventStore. Return `boolean` (true if applied; false if version mismatch).
See [ADR-0014](adr/0014-optimistic-locking-via-version-field.md).

## O

**optimistic locking** — coordination of concurrent changes through a
`version` field: every change increments it, and finalize verifies that
`version` did not change. If it did, the operation does not proceed and
returns false.

**orphan recovery** — recovery of events from dead workers. Implemented by
OrphanRecoveryTask in the maintenance executor: `findDead()` +
`reclaimOrphans(deadIds)` + `removeDead()` in a single atomic TX.

**OutboxEngine** — the main engine facade. Manages the lifecycle
(start/stop), owns pollers, dispatchers, and maintenance tasks.

**OutboxEventPublisher** — public API for publishing events. The default
implementation in the core uses EventSerializer + EventStore.save().

**OutboxListener** — event bus for observability (26 methods).
Implementations: `LoggingOutboxListener` (default),
`MicrometerOutboxListener` (separate module). See
[ADR-0013](adr/0013-outbox-listener-for-observability.md).

## P

**payload** — event business data in serialized form (`SerializedPayload`).
Textual formats (Jackson JSON) live in `event_outboxer.events.payload
JSONB`; binary formats in `payload_binary BYTEA` — exactly one of the two
per row (ADR-0025). Payload type is an explicit DTO, not a lambda. See
[ADR-0003](adr/0003-explicit-dto-payload.md).

**payload_format** — stable `EventSerializer.format()` id (for example
`jackson-json`) recorded with every event at publish time. The dispatcher
selects the deserializer by this value. See
[ADR-0025](adr/0025-binary-capable-serializer-spi-and-payload-format.md).

**PENDING** — the initial status of an event after publish. Ready to be
claimed once `run_at <= now()`.

**Poller** — per-type core-engine component running the polling loop. Uses
`PollStrategy.runOnce()` + `Waiter` for sleep/wakeup.

**PollStrategy** — a single iteration of the polling loop.
Implementations: `LockAndFetchStrategy`, `FetchThenLockStrategy`.

**PROCESSING** — event status while it is being processed. Has
`claimed_by` and `claimed_at` set.

**publish** — call to `OutboxEventPublisher.publish(eventType, payload)`.
MUST execute within the caller's transaction. See
[ADR-0002](adr/0002-participate-in-client-transaction.md).

## R

**reschedule-to-future** — the retry mechanism: the event is returned to
PENDING with `run_at = now + delay`; the poller picks it up when the time
comes. The only retry mechanism in MVP (no in-memory Thread.sleep).

**retry** — a result that means "try later". Returned explicitly
(`Retry(reason, delayOverride, cause)`) or implicitly for any uncaught
exception.

## S

**Skip** — an EventOutcome meaning "success with no business action" (e.g.
the event has already been processed elsewhere). Handled as Success, but
with distinct metrics/logs.

**SPI (Service Provider Interface)** — interfaces for adapters. Lives in
the `event-outboxer-spi` module. See
[ADR-0010](adr/0010-storage-agnostic-core-via-spi.md).

**storage-agnostic** — architectural principle: the core is unaware of the
specific storage (PG, Mongo, InMemory). All storage specifics live in
adapters.

**SmartLifecycle** — Spring interface for ordered bean start/stop in
phases. Used to start/stop core-engine components.

## T

**TaskDecorator** — Spring interface for decorating Runnables prior to
execution. Default — `ContextPropagatingTaskDecorator` (propagates MDC,
Micrometer Observation, Security context).

**TransactionAwareDataSourceProxy** — Spring wrapper around a DataSource
that neutralizes `commit()`/`rollback()` for connections participating in
an outer TX. Used to participate in the client's TX. See
[ADR-0002](adr/0002-participate-in-client-transaction.md).

**trace_context** — event field holding the flat W3C carrier map
(`traceparent` / `tracestate` / `baggage` as single string values),
captured by the `OutboxTracer` producer span at publish (ADR-0023) and
restored on the worker as the parent of the per-attempt consumer span.

## V

**version** — BIGINT field used for optimistic locking. Incremented on
claim / markForRetry / markDisabled / forceReclaim / reclaimOrphans. Does
NOT change on heartbeat (stored in a separate table).

## W

**WatchdogTask** — maintenance task detecting stuck handlers
(`now - claimedAt > handlerMaxRuntime`), triggering force reclaim,
interrupting the handler thread, and reporting an
[abandoned handler](#a) when that thread does not return. See
[ADR-0005](adr/0005-workers-heartbeat-table.md) and
[ADR-0014](adr/0014-optimistic-locking-via-version-field.md).

**WorkerId** — unique identifier of a JVM instance. Format:
`{hostname}-{pid}-{uuid8}`. One per JVM, shared across all event types.

**WorkerInfo** — record with worker metadata at registration:
`workerId, host, pid, startedAt, metadata`.

**WorkerRegistry** — SPI port for registering workers and heartbeating.
Implemented as the `event_outboxer.workers` table in the PG adapter. See
[ADR-0005](adr/0005-workers-heartbeat-table.md).
