# event-outboxer-tracing-micrometer

Micrometer Tracing implementation of the `OutboxTracer` SPI
([ADR-0023](../adr/0023-tracing-spi-port-and-adapters.md)) — the
adapter to use when Spring Boot Actuator's tracing
(`micrometer-tracing-bridge-otel` or `-brave`) is your instrumentation
layer. Same span model as the
[OTel adapter](event-outboxer-tracing-otel.md): PRODUCER span per
insert, stored context, CONSUMER span per handler attempt.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-tracing-micrometer` |
| Java package | `io.github.bams22.outboxer.tracing.micrometer` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `io.micrometer:micrometer-tracing` |
| Enable with | module on the classpath + Boot-provided `Tracer` and `Propagator` beans |

## What it does

**`MicrometerOutboxTracer(Tracer, Propagator)`** emits the same spans
and `OutboxTraceAttributes` metadata as the OTel adapter, going
through Boot's tracing abstraction instead of the OTel API directly.
Practical deltas:

- **Propagation format follows Boot**:
  `management.tracing.propagation.*` decides whether the stored
  carrier holds `traceparent` (W3C) or `b3` keys, and
  `management.tracing.baggage.remote-fields` controls baggage — the
  outbox hop behaves exactly like every other outbound call in the
  app.
- Attribute values are string tags (Micrometer Tracing has no typed
  tags), and error rendering depends on the bridge (OTel bridge:
  exception event + ERROR status; Brave: error tag).

## When to use it

- **Default for Spring Boot apps using Actuator tracing.** When both
  tracing modules are on the classpath, the starter picks this one —
  it is Boot's first-class abstraction, so the outbox carrier matches
  the rest of the app.
- Use [tracing-otel](event-outboxer-tracing-otel.md) when
  instrumentation comes from the OTel Java agent / a raw OTel SDK and
  no `Tracer`/`Propagator` beans exist.

## How to configure it

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-tracing-micrometer</artifactId>
</dependency>
<!-- plus your usual Boot tracing setup, e.g.: -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-otel</artifactId>
</dependency>
```

No `event-outboxer.*` properties needed — the starter's
`MicrometerTracingAutoConfiguration` activates when Boot provides
`Tracer` and `Propagator` beans.

- `event-outboxer.tracing.enabled: false` disables adapter
  auto-detection (both modules).
- A user-defined `OutboxTracer` bean always wins over both
  auto-configurations.

### Without Spring

```java
new OutboxEngineBuilder()
    .tracer(new MicrometerOutboxTracer(tracer, propagator))
    .build();
```

## Related

- [OBSERVABILITY.md §Distributed tracing](../OBSERVABILITY.md#distributed-tracing) — adapter-choice table and span reference.
- [event-outboxer-tracing-otel](event-outboxer-tracing-otel.md).
- [ADR-0023](../adr/0023-tracing-spi-port-and-adapters.md).
