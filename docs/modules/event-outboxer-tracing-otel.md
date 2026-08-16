# event-outboxer-tracing-otel

OpenTelemetry implementation of the `OutboxTracer` SPI
([ADR-0023](../adr/0023-tracing-spi-port-and-adapters.md)): the trace
active at `publish()` continues into handler execution — a PRODUCER
span per insert, its context stored with the event, a CONSUMER span
per handler attempt. Works with the OTel Java agent and hand-built
SDKs alike.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-tracing-otel` |
| Java package | `io.github.bams22.outboxer.tracing.otel` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `io.opentelemetry:opentelemetry-api` |
| Enable with | module on the classpath (starter auto-detects); `event-outboxer.tracing.enabled: true` is the default |

## What it does

**`OtelOutboxTracer`** (instrumentation scope
`io.github.bams22.event-outboxer`):

- **Publish side** — span `outbox publish <eventType>`, kind
  `PRODUCER`, with OTel messaging semconv attributes
  (`messaging.system=event_outboxer`, `messaging.operation.type=send`,
  `messaging.destination.name=<eventType>`,
  `messaging.message.id=<eventId>`). The span's context **plus the
  caller's baggage** is injected through the SDK's configured
  propagators (honouring `OTEL_PROPAGATORS`, not hardcoded W3C) into a
  flat string map that the engine persists in the event's
  `trace_context` column. Nothing is made current on the publishing
  thread. Coalesced dedup publishes tag the span
  `event_outboxer.coalesced_into=<existingId>`.
- **Handler side** — span `outbox process <eventType>`, kind
  `CONSUMER`, parented by the *stored* context and made current (with
  restored baggage) for the duration of the handler. Extra attributes:
  `event_outboxer.attempt` (1-based) and `event_outboxer.worker.id`.
  Each retry gets a fresh span in the same trace. Handler exceptions
  are recorded (`recordException` + ERROR status).

The engine wraps whatever tracer it gets in `SafeOutboxTracer`, so a
broken tracing backend can never fail a publish or strand an event.
With an unconfigured `OpenTelemetry` the captured map is empty —
graceful degradation to no-op.

**Thread hops inside a handler.** The consumer scope is a plain OTel
`Context`, so work the handler offloads to another thread keeps the
trace only if OTel context propagation covers that hop: the **Java
agent instruments executors** and it just works, and manual setups can
use `Context.taskWrapping(executor)` or `Context.current().wrap(...)`.
Spring's `ContextPropagatingTaskDecorator` / Reactor's
`contextCapture()` do **not** carry it — they copy Micrometer
observations, and an agent-less OTel setup has none. This is a
property of OTel context in a Boot application rather than of this
adapter (the app's own spans behave identically); if it matters, the
[Micrometer adapter](event-outboxer-tracing-micrometer.md) is the one
that rides `ContextSnapshot`.

## When to use it

- Your service is instrumented with the **OTel Java agent** or a
  hand-built OTel SDK, and you are *not* using Spring Boot's
  Micrometer Tracing bridge.
- If Boot Actuator provides `ObservationRegistry`/`Tracer`/`Propagator`
  beans (`micrometer-tracing-bridge-*`), prefer
  [`event-outboxer-tracing-micrometer`](event-outboxer-tracing-micrometer.md)
  — the starter picks it automatically and it wins when both modules
  are present, so the outbox carrier matches every other outbound
  carrier in the app.
- No tracing at all? Add neither module — the engine uses a zero-cost
  no-op tracer.

## How to configure it

### With Spring Boot

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-tracing-otel</artifactId>
</dependency>
```

No properties needed. The starter's `OtelTracingAutoConfiguration`
activates when the OTel API is on the classpath, using an
`OpenTelemetry` bean if one exists, else `GlobalOpenTelemetry.get()`
(the agent case).

- `event-outboxer.tracing.enabled: false` disables auto-detection of
  both tracing adapters.
- A user-defined `OutboxTracer` bean always wins:

  ```java
  @Bean
  OutboxTracer outboxTracer(OpenTelemetry otel) {
      return new OtelOutboxTracer(otel);
  }
  ```

- **Caveat:** `GlobalOpenTelemetry.get()` pins the global instance at
  context refresh. Apps that call `GlobalOpenTelemetry.set(...)` later
  must expose an `OpenTelemetry` bean (or a custom `OutboxTracer`
  bean) instead.

### Without Spring

```java
new OutboxEngineBuilder()
    .tracer(new OtelOutboxTracer(openTelemetry))
    .build();
```

## Related

- [OBSERVABILITY.md §Distributed tracing](../OBSERVABILITY.md#distributed-tracing) — span attribute reference and adapter-choice table.
- [event-outboxer-tracing-micrometer](event-outboxer-tracing-micrometer.md) — the Boot-native alternative.
- [ADR-0023](../adr/0023-tracing-spi-port-and-adapters.md).
