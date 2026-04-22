# ADR-0009: Spring ThreadPoolTaskExecutor + TaskDecorator in the starter

## Status

Accepted

## Date

2026-04-20

## Context

Handler pools in db-scheduler are raw `ExecutorService`s:

```java
protected ExecutorService executorService;
protected ExecutorService dueExecutor;
```

That works for simple cases, but our events are almost always the
continuation of an upstream request (REST API → business TX → publish →
handler). This makes propagation essential:
- **MDC**: `traceId`, `spanId`, `requestId`, `userId` — so that the
  handler's logs correlate with the upstream request.
- **Micrometer Observation / OpenTelemetry**: the trace context, so that
  the handler's spans become children of the upstream span.
- **Spring Security**: `SecurityContextHolder` — if the handler needs the
  current user.
- **Baggage / tenant context**: for multi-tenant systems.

With a raw `ExecutorService` every library consumer has to write:
- a `ThreadFactory` for named threads (db-scheduler even has a
  `PrefixingDefaultThreadFactory` — a bespoke solution for an obvious need);
- `Runnable` wrappers that copy MDC/Micrometer context into the worker
  thread.

This is significant DX debt.

## Alternatives considered

- **A. Raw `ExecutorService` as in db-scheduler**: simple, but no
  propagation.
- **B. Spring `TaskExecutor` in core**: the core would depend on Spring,
  breaking the Spring-agnostic principle.
- **C. `java.util.concurrent.Executor` in the core + Spring
  `ThreadPoolTaskExecutor` in the starter**: the core stays clean; the
  starter injects the Spring implementation with a `TaskDecorator`.

## Decision

**Option C was chosen**.

### In the core

The contract is `Function<EventTypeConfig, ExecutorService>` — a
factory that, given a per-type config, returns a plain JDK
`java.util.concurrent.ExecutorService`. See
`OutboxEngineBuilder.handlerExecutorFactory(...)`. No Spring types;
no bespoke wrapper interface.

`ExecutorService` (not plain `Executor`) is required because
`OutboxEngine.drainHandlers` performs a graceful shutdown sequence —
`shutdown()` → `awaitTermination(timeout)` → `shutdownNow()` — which
a plain `Executor` SAM cannot express. See §Implementation notes
(0.1.0) below for why this shape won out over the alternatives.

### In the starter

The starter ships a static utility —
`io.github.bams22.outboxer.spring.executor.HandlerExecutorFactory`
— with two factory methods selected via the
`event-outboxer.handler-executor.type` property:

- `platform()` — builds a fixed-size `ThreadPoolTaskExecutor` with
  a hard-wired `ContextPropagatingTaskDecorator`, then exposes it
  to the core as an `ExecutorService` via `SpringTaskExecutorAdapter`
  (see §Implementation notes (0.1.0) below for why the adapter is
  needed).
- `virtual()` — builds `Executors.newThreadPerTaskExecutor(...)` on
  a virtual-thread factory and wraps it in
  `ContextPropagatingExecutorService` so the same decorator applies.

`OutboxEngineAutoConfiguration` wires the matching factory into
`OutboxEngineBuilder.handlerExecutorFactory(...)`. Pool sizing,
queue capacity, and thread naming come from `EventTypeConfig` /
`OutboxProperties`.

`ContextPropagatingTaskDecorator` (Spring Framework 6.1+ / Spring Boot
3.2+) uses Micrometer's `ContextSnapshotFactory` under the hood and
propagates automatically:
- MDC (via `MdcAccessor`);
- Micrometer Observation (→ OpenTelemetry span context);
- Security context (when `micrometer-context-spring-security` is on the
  classpath);
- Any other thread-local registered via `ContextRegistry`.

### Override points

- `event-outboxer.handler-executor.type=virtual` — switches to the
  virtual-thread variant. Requires JDK 21+ at runtime (baseline is
  Java 17, see [ADR-0017](0017-java-25-and-spring-boot-3-5-baseline.md));
  on JDK 17 the factory fails fast with an actionable error. JDK 25+
  additionally enables JEP 491
  eliminates `synchronized` carrier-pinning, making this opt-in safe
  for JDBC-bound handlers.
- `@Bean TaskDecorator` — register a Spring bean of type
  `org.springframework.core.task.TaskDecorator` and the
  auto-configuration resolves it via `ObjectProvider` and hands it
  to `HandlerExecutorFactory.platform(...)` / `.virtual(...)`. When
  no custom bean exists the default is
  `ContextPropagatingTaskDecorator`. Useful for adding tenant /
  feature-flag context on top of — or entirely in place of — the
  default MDC / Observation / security propagation.
- `@Bean OutboxEngine` — full override if the defaults don't fit;
  replaces the entire engine, including its executor wiring.

### Maintenance executor stays inside the core

Heartbeat, orphan recovery, and watchdog run on a shared
`java.util.concurrent.ScheduledExecutorService` owned by
`MaintenanceScheduler` in `event-outboxer-core`. No Spring bean;
no `TaskDecorator` — background operations must not pollute traces,
and keeping it in the core preserves the Spring-free invariant
([ADR-0010](0010-storage-agnostic-core-via-spi.md)).

### Poller stays inside the core

Each per-type poller runs on a raw
`new Thread(this::loop, "outbox-poller-<eventType>")` (see
`Poller.java`). No executor bean, no `TaskDecorator` — the poller
is background infrastructure, and a dedicated thread keeps its
control-flow (steady-state loop + clean interrupt semantics)
simple.

## Rationale

- **Thread naming for free**: `setThreadNamePrefix(...)` replaces a custom
  `ThreadFactory`. Important for debugging (thread dumps are readable).
- **Out-of-the-box context propagation**: no manual MDC copying, no
  deprecated `@Async` tricks. One `TaskDecorator` solves every case.
- **Graceful shutdown through Spring lifecycle**: `ThreadPoolTaskExecutor`
  implements `DisposableBean`, so `stop()` is called automatically on
  `@PreDestroy`, respecting `spring.lifecycle.timeout-per-shutdown-phase`.
- **Virtual threads via a property**: one-line config flips the handler
  executor to virtual threads. The factory reflects into
  `Thread.ofVirtual()` / `Executors.newThreadPerTaskExecutor` so the
  library baseline stays Java 17; a JDK 21+ runtime engages the real
  APIs, JDK 25+ additionally removes `synchronized` pinning (JEP 491).
- **Core stays Spring-agnostic**: plain-Java users do not get automatic
  propagation, but can register their own decorator (they configure
  everything manually anyway).

## Consequences

### For users

- MDC/tracing propagate "for free": if the application uses Micrometer
  Observation / OpenTelemetry, handlers see the same traceId/spanId as the
  upstream request.
- Thread dumps are readable: `outbox-handler-N`, `outbox-vt-N`,
  `outbox-poller-<eventType>`.
- Custom propagation: replace the whole engine via `@Bean OutboxEngine`
  (a direct `@Bean TaskDecorator` override is a follow-up; see §Override
  points).

### For maintainers

- **The core does not depend on Spring**. Plain-Java usage is supported,
  just without `TaskDecorator` (users can add one themselves).
- The starter owns only the **handler** executor wiring (via
  `HandlerExecutorFactory`). The **poller** (raw `Thread`) and
  **maintenance** (`ScheduledExecutorService`) executors live in the
  core and are Spring-free.
- The handler executor is the only one with a `TaskDecorator`.

### Positive consequences

- Sensible defaults for the majority.
- Standard Spring idiom (`ThreadPoolTaskExecutor` is familiar to anyone).
- Easy customization.

### Negative consequences

- Spring users get slightly different behavior from plain-Java users
  (propagation on vs off). Expected, but must be documented.

## Implementation notes (0.1.0)

The decision above settles *what* to run (Spring
`ThreadPoolTaskExecutor` with `ContextPropagatingTaskDecorator`).
The 0.1.0 implementation answers *how* to expose that to the
Spring-free core without losing decoration or lifecycle.

### Core contract

`OutboxEngineBuilder.handlerExecutorFactory(...)` accepts
`Function<EventTypeConfig, ExecutorService>`. The core uses plain
JDK `ExecutorService` — not `Executor`, not Spring `TaskExecutor`,
and not a bespoke `OutboxExecutor` interface — because the engine
needs the lifecycle trio `shutdown()` / `awaitTermination(...)` /
`shutdownNow()` for graceful drain, and `ExecutorService` already
has exactly that shape.

### Why not submit to `ThreadPoolTaskExecutor` directly

`ThreadPoolTaskExecutor` implements Spring's `TaskExecutor`, not
`java.util.concurrent.ExecutorService`. It exposes its delegate
pool via `getThreadPoolExecutor()`, but submitting there bypasses
the `TaskDecorator` — Spring installs decoration inside TPTE's own
`execute` / `submit` methods, not in the delegate. If the core
drove the delegate directly, MDC / Observation / Security context
would not propagate from the poller thread to handler threads.

### Alternatives

| # | Option                                                                 | Verdict    | Reason                                                                                                      |
| - | ---------------------------------------------------------------------- | ---------- | ----------------------------------------------------------------------------------------------------------- |
| 1 | Change core contract to plain `Executor`                               | Rejected   | Loses `shutdown` / `awaitTermination`; graceful drain breaks.                                               |
| 2 | Change core contract to Spring `TaskExecutor`                          | Rejected   | Violates Spring-free-core invariant ([ADR-0010](0010-storage-agnostic-core-via-spi.md)).                     |
| 3 | Introduce a bespoke `OutboxExecutor` interface                         | Rejected   | Over-engineering; no capability beyond JDK `ExecutorService`, plus one extra adapter per adapter.           |
| 4 | Pass `tpte.getThreadPoolExecutor()` (the underlying `ThreadPoolExecutor`) | Rejected   | Bypasses `TaskDecorator` — no context propagation.                                                           |
| 5 | **`SpringTaskExecutorAdapter` wrapping `ThreadPoolTaskExecutor`**       | **Chosen** | Submission routes through TPTE (decoration runs); lifecycle routes through the underlying `ThreadPoolExecutor`. |

### Virtual threads

`Executors.newThreadPerTaskExecutor(...)` is not backed by a
`ThreadPoolTaskExecutor`, so the TPTE decoration mechanism does
not apply. A parallel wrapper,
`ContextPropagatingExecutorService`, applies the same
`ContextPropagatingTaskDecorator` at submit time so that virtual
handlers see the submitting thread's MDC / Observation / Security
context. `Thread.ofVirtual()` and
`Executors.newThreadPerTaskExecutor(...)` are invoked via reflection
(baseline is Java 17 — see
[ADR-0017](0017-java-25-and-spring-boot-3-5-baseline.md)); a JDK 21+
runtime engages the real APIs, and JDK 25+
eliminates `synchronized` carrier-pinning, so this variant is safe
for JDBC-bound handlers.

### Pointers

- `event-outboxer-spring-boot-starter/.../executor/HandlerExecutorFactory.java` — the two factory methods.
- `event-outboxer-spring-boot-starter/.../executor/SpringTaskExecutorAdapter.java` — TPTE → `ExecutorService` bridge.
- `event-outboxer-spring-boot-starter/.../executor/ContextPropagatingExecutorService.java` — same-decorator wrapper for the virtual variant.

## Related decisions

- [ADR-0004](0004-per-event-type-worker-isolation.md) — one executor per
  type.
- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — general principle:
  Spring only in the starter.
