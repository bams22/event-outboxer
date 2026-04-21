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

The contract is `java.util.concurrent.Executor` (SAM interface):

```java
// Internal lifecycle hook in the core
public interface OutboxExecutor {
    void execute(Runnable task);
    void shutdown(Duration awaitTermination);
}
```

This is just a wrapper around `Executor` + lifecycle. No Spring types.

### In the starter

The autoconfiguration creates a per-type `ThreadPoolTaskExecutor` with a
configured `TaskDecorator`:

```java
@Bean
@ConditionalOnMissingBean(name = "outboxHandlerExecutorFactory")
public OutboxHandlerExecutorFactory outboxHandlerExecutorFactory(
        OutboxProperties props,
        ObjectProvider<TaskDecorator> taskDecorators) {

    return (eventType, cfg) -> {
        ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
        ex.setCorePoolSize(cfg.corePoolSize());
        ex.setMaxPoolSize(cfg.maxPoolSize());
        ex.setQueueCapacity(cfg.queueCapacity());
        ex.setThreadNamePrefix("outbox-handler-" + eventType + "-");
        ex.setTaskDecorator(taskDecorators.getIfAvailable(
            ContextPropagatingTaskDecorator::new));
        ex.setWaitForTasksToCompleteOnShutdown(true);
        ex.setAwaitTerminationSeconds((int) cfg.awaitTerminationSeconds());
        ex.initialize();
        return ex;
    };
}
```

`ContextPropagatingTaskDecorator` (Spring Framework 6.1+ / Spring Boot
3.2+) uses Micrometer's `ContextSnapshotFactory` under the hood and
propagates automatically:
- MDC (via `MdcAccessor`);
- Micrometer Observation (→ OpenTelemetry span context);
- Security context (when `micrometer-context-spring-security` is on the
  classpath);
- Any other thread-local registered via `ContextRegistry`.

### Override points

- `@Bean TaskDecorator` — users can replace the decorator with custom
  propagation.
- `@Bean(name="outboxHandlerExecutorFactory") ...` — full factory override.
- `outbox.handler-executor.type=virtual` — `SimpleAsyncTaskExecutor` with
  virtual threads. On the Java 25 baseline (see
  [ADR-0017](0017-java-25-and-spring-boot-3-5-baseline.md)) virtual
  threads are first-class and JEP 491 eliminates `synchronized`
  carrier-pinning, making this opt-in safe for JDBC-bound handlers.

### Maintenance executor is also a Spring bean

A `ThreadPoolTaskScheduler` with 2–3 threads for heartbeat, orphan
recovery, and watchdog. **No TaskDecorator** — background operations should
not pollute traces.

### Poller executor is a single-thread Spring bean

Each poller is a `SingleThreadTaskExecutor` (or `ThreadPoolTaskExecutor`
with `corePoolSize=1`) with the prefix `outbox-poller-${eventType}`. No
`TaskDecorator` — the poller is background infrastructure.

## Rationale

- **Thread naming for free**: `setThreadNamePrefix(...)` replaces a custom
  `ThreadFactory`. Important for debugging (thread dumps are readable).
- **Out-of-the-box context propagation**: no manual MDC copying, no
  deprecated `@Async` tricks. One `TaskDecorator` solves every case.
- **Graceful shutdown through Spring lifecycle**: `ThreadPoolTaskExecutor`
  implements `DisposableBean`, so `stop()` is called automatically on
  `@PreDestroy`, respecting `spring.lifecycle.timeout-per-shutdown-phase`.
- **Virtual threads via a property**: one-line config flips the handler
  executor to virtual threads (JDK 25 baseline; no pinning caveats).
- **Core stays Spring-agnostic**: plain-Java users do not get automatic
  propagation, but can register their own decorator (they configure
  everything manually anyway).

## Consequences

### For users

- MDC/tracing propagate "for free": if the application uses Micrometer
  Observation / OpenTelemetry, handlers see the same traceId/spanId as the
  upstream request.
- Thread dumps are readable: `outbox-handler-SEND_EMAIL-1`,
  `outbox-poller-UPDATE_CACHE`.
- Custom propagation through `@Bean TaskDecorator`.

### For maintainers

- **The core does not depend on Spring**. Plain-Java usage is supported,
  just without `TaskDecorator` (users can add one themselves).
- The starter has autoconfigurations for three executor flavors: handler
  (per type), poller (per type), maintenance (shared).
- The handler executor is the only one with a `TaskDecorator` by default.

### Positive consequences

- Sensible defaults for the majority.
- Standard Spring idiom (`ThreadPoolTaskExecutor` is familiar to anyone).
- Easy customization.

### Negative consequences

- Spring users get slightly different behavior from plain-Java users
  (propagation on vs off). Expected, but must be documented.

## Related decisions

- [ADR-0004](0004-per-event-type-worker-isolation.md) — one executor per
  type.
- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — general principle:
  Spring only in the starter.
