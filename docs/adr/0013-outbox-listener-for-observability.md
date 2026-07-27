# ADR-0013: OutboxListener as an event bus for observability

## Status

Accepted (amended 2026-07-27: corrected where the logging listener is
registered by default)

## Date

2026-04-20

## Context

The library must provide observability: metrics (Prometheus/Micrometer),
structured logs, alerts, audit trails, tracing. We need to decide how to
surface lifecycle events to the outside world.

## Alternatives considered

- **A. A `MetricsCollector` port in SPI**: a custom abstract API (Counter,
  Timer, Gauge, Tags). Implementations: NoOp in core, Micrometer in a
  separate module.
- **B. A direct Micrometer dependency in the core**:
  `ObservationRegistry.NOOP` as the default; a real implementation is
  wired through Spring Boot Actuator.
- **C. `OutboxListener` interface in the core + a separate module with a
  Micrometer implementation**: the core emits abstract events, the
  Micrometer module listens to them and publishes metrics. Pattern from
  db-scheduler.

## Decision

**Option C was chosen**: `OutboxListener` in `event-outboxer-api` + a
separate `event-outboxer-metrics-micrometer` module with
`MicrometerOutboxListener`.

### API

21 methods in `OutboxListener`, all with a default no-op. Arguments are
records (protection against breaking changes when adding fields).

Groups:
1. **Publication**: `onEventPublished`.
2. **Processing lifecycle**: `onEventClaimed`, `onEventProcessed`,
   `onEventRetryScheduled`, `onEventDisabled`, `onEventDeleted`,
   `onEventSkipped`.
3. **Errors & anomalies**: `onHandlerError`, `onUnknownEventType`,
   `onEventSerializationError`, `onLockAcquisitionFailed`,
   `onLockReleaseFailed`.
4. **Worker lifecycle**: `onWorkerRegistered`, `onWorkerGracefulStop`,
   `onWorkerDeregistered`, `onHeartbeatFailed`.
5. **Recovery**: `onOrphansReclaimed`, `onStuckHandlerReclaimed`.
6. **Storage**: `onStorageError`.
7. **Dispatch**: `onDispatchRejected`.
8. **Engine lifecycle**: `onEngineCrashed`.

The complete list lives in `OutboxListener` itself — the methods are
grouped under the same section headings as above.

### Contract

- Methods are called from multiple threads (workers, pollers, maintenance).
- Implementations MUST be thread-safe.
- Implementations MUST NOT block — long operations slow down the core
  engine.
- Exceptions from a listener are caught in `OutboxListenerRegistry` (a bad
  listener does not break the others).

### Registration

In the Spring starter, all `OutboxListener` beans are collected
automatically into `OutboxListenerRegistry`. In plain Java — via a builder.

### MicrometerOutboxListener

The `event-outboxer-metrics-micrometer` module provides a ready
implementation. Mapping (sample):
- `onEventProcessed` → `event_outboxer.events.processed{event_type}` Counter +
  `event_outboxer.events.handle.duration{event_type}` Timer.
- `onEventRetryScheduled` → `event_outboxer.events.retried{event_type}` Counter.
- `onEventDisabled` → `event_outboxer.events.disabled{event_type}` Counter.
- `onOrphansReclaimed` → `event_outboxer.orphans.reclaimed` Counter.
- etc.

The starter registers it automatically when Micrometer is on the classpath
and the Micrometer autoconfig is active.

## Rationale

### Why not a `MetricsCollector` SPI

That would reinvent Micrometer. Our abstract `Counter`/`Timer`/`Gauge`/`Tags`
is a copy of the Micrometer API. Micrometer users (the majority) pay a
double mapping: Micrometer ↔ our SPI ↔ Micrometer. Worst of all worlds.

### Why not a direct Micrometer dependency in the core

The core should carry the minimum dependencies. Even if
`ObservationRegistry.NOOP` is the default, `micrometer-core` is pulled as a
transitive. That is 100 KB+ plus its transitive deps. We prefer to keep the
core on SLF4J only.

### Upsides of the listener approach

- **The core stays pristine** — only SLF4J.
- **The event bus is reused** — not just metrics, but also:
  - Structured logging (logback/slf4j JSON encoders).
  - Audit trail (a listener writes to a separate `event-outboxer.audit` table).
  - Alerting (a listener pushes to Slack on DISABLED events).
  - Distributed tracing (a listener emits Observation events).
- **Custom listeners** — users write their own bean, implementing the
  methods they care about.
- **db-scheduler uses the same pattern** (`SchedulerListener`) — proven.

### The plain-Java default is not no-op but LoggingListener

`OutboxEngineBuilder` adds a `LoggingOutboxListener` (key events at
INFO/WARN) unless opted out via `includeLoggingListener(false)`. The
Spring starter opts out explicitly and registers only the
`OutboxListener` beans found in the application context — Spring users
who want the logging behavior declare the listener as a bean. There is
no dedicated enable/disable property.

## Consequences

### For users

- Enabling Micrometer metrics: add
  `event-outboxer-metrics-micrometer` to the classpath. No code changes.
- Custom listeners: `@Component class MyListener implements
  OutboxListener {...}` — auto-wired.
- Default logging is informative; noise can be reduced when needed.

### For maintainers

- Every new failure/success/anomaly must invoke the corresponding listener
  method. If there is no suitable method — add one (and record the
  breaking change in the changelog, since it expands the interface).
- `OutboxListenerRegistry` is the single point of invocation. A try-catch
  around each listener prevents a bad listener from breaking the others.
- Records with fields provide a stable binary API.

### Positive consequences

- Clean separation: "what happened" (listener events) vs "how to measure"
  (Micrometer).
- Flexibility: one event → many listeners in parallel.
- The core stays minimal.

### Negative consequences

- 21 methods — a noticeable API surface. Adding a new one is a breaking
  change.
- Listeners must be thread-safe and fast.
- A little more boilerplate when creating a custom listener.

## Related decisions

- [ADR-0007](0007-failure-handler-chain-of-responsibility.md) —
  `HandlerDispatcher` emits the retry / disable / delete listener
  callbacks after storage commit (superseding the original
  chain-decorator design).
- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — general principle:
  "core without infrastructure dependencies".
