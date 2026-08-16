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
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `io.micrometer:micrometer-observation`, `io.micrometer:micrometer-tracing` |
| Enable with | module on the classpath + Boot-provided `ObservationRegistry`, `Tracer` and `Propagator` beans |

## What it does

**`MicrometerOutboxTracer(ObservationRegistry, Tracer[, prefix])`**
emits the
same spans and `OutboxTraceAttributes` metadata as the OTel adapter,
but instruments through Micrometer's **Observation API**: a
`SenderContext` observation per publish and a `ReceiverContext`
observation per handler attempt. Boot's propagating tracing handlers
own span creation, kind, parent extraction and carrier injection.
Practical deltas:

- **Propagation format follows Boot**:
  `management.tracing.propagation.*` decides whether the stored
  carrier holds `traceparent` (W3C) or `b3` keys, and
  `management.tracing.baggage.remote-fields` controls baggage — the
  outbox hop behaves exactly like every other outbound call in the
  app.
- **The current *observation* is set around the handler**, not just
  the current span. That is what `ContextPropagatingTaskDecorator`,
  Reactor's `contextCapture()` and `@Async` copy, so a handler that
  offloads work keeps the trace instead of starting a detached one
  (ADR-0023, 2026-08-16 amendment).
- **Four meters come along whether you want them or not.** Boot's
  `DefaultMeterObservationHandler` turns each observation into a
  `Timer` plus a `LongTaskTimer`, so wiring this module registers
  `<prefix>.publish`, `<prefix>.publish.active`, `<prefix>.process`
  and `<prefix>.process.active` — where `<prefix>` is
  `event-outboxer.metrics.prefix`, the same slot
  [metrics-micrometer](event-outboxer-metrics-micrometer.md) uses. The
  timers are tagged with the low-cardinality `messaging.system` /
  `messaging.operation.type` / `messaging.destination.name` plus
  Micrometer's own `error`; ids, attempt and worker are
  high-cardinality — span-only, never timer tags. None of the four
  ship SLO buckets or dashboard panels. See
  [OBSERVABILITY.md §Side-effect meters](../OBSERVABILITY.md#side-effect-meters-from-the-tracing-adapter)
  for what they measure and the `MeterFilter` that removes them.
- **`<prefix>.process` is not a replacement for
  `<prefix>.events.processing_time`.** It wraps only
  `handler.handle(...)` and records every attempt including failures;
  the listener's timer measures claim → finalize on success only, and
  is tagged `event_type`. Keep the listener's as the SLI.
- **The event type becomes a real meter tag** via
  `messaging.destination.name`, so this module inherits the metrics
  module's assumption that event types are a bounded, code-defined
  set. A per-tenant type string would grow the registry without bound.
- **Observations are filterable from the outside.**
  `management.observations.enable.all=false`, or
  `...enable.<prefix>=false` reached for as a way to silence the
  meters, makes the adapter a silent no-op: no spans, empty
  `trace_context`, no error logged. Use a `MeterFilter` for the meters
  instead.
- Attribute values are string tags (Micrometer Tracing has no typed
  tags), and error rendering depends on the bridge (OTel bridge:
  exception event + ERROR status; Brave: error tag).
- Span names come from the observation's contextual name. Boot groups
  tracing handlers into a first-matching composite, so the propagating
  handlers own these contexts and the names are used verbatim. A
  hand-built registry carrying only `DefaultTracingObservationHandler`
  renames them through Micrometer's lower-hyphen convention
  (`outbox publish -order-created`) and never fills the carrier.

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
`ObservationRegistry`, `Tracer` and `Propagator` beans. The tracing
beans are part of the condition because they are what makes Boot
register the propagating tracing handlers; without them the registry
would emit timers and no spans, and the OTel adapter takes over.

- `event-outboxer.tracing.enabled: false` disables adapter
  auto-detection (both modules).
- `event-outboxer.metrics.prefix` renames the observations — and with
  them the four meters above — exactly as it renames the listener's
  metrics. The starter binds it for both modules.
- A user-defined `OutboxTracer` bean always wins over both
  auto-configurations.

### Without Spring

```java
new OutboxEngineBuilder()
    .tracer(new MicrometerOutboxTracer(observationRegistry, tracer))
    .build();
```

The registry must carry the tracing `ObservationHandler`s, grouped in
an `ObservationHandler.FirstMatchingCompositeObservationHandler` the
way Boot does — the propagating pair first, the default one last:

```java
registry.observationConfig()
    .observationHandler(new FirstMatchingCompositeObservationHandler(
        new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
        new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
        new DefaultTracingObservationHandler(tracer)));
```

Order matters: `DefaultTracingObservationHandler` matches every
context, so if it came first the carrier would silently stay empty and
the span names would be mangled through `SpanNameUtil.toLowerHyphen`
(`outbox publish -order-created`). The three-argument constructor
`MicrometerOutboxTracer(registry, tracer, prefix)` sets the
observation/meter prefix outside Spring.

## Related

- [OBSERVABILITY.md §Distributed tracing](../OBSERVABILITY.md#distributed-tracing) — adapter-choice table and span reference.
- [event-outboxer-tracing-otel](event-outboxer-tracing-otel.md).
- [ADR-0023](../adr/0023-tracing-spi-port-and-adapters.md).
