# ADR-0023: Trace continuity via an OutboxTracer SPI port with OpenTelemetry and Micrometer adapters

## Status

Accepted (amended 2026-08-16: the Micrometer adapter instruments
through the Observation API instead of `Tracer` / `Propagator`
directly — see the amendment section under Decision).

## Date

2026-07-27

## Context

The outbox breaks distributed traces by design: `publish()` runs in the
caller's request (with an active trace), the handler runs later on a
poller-fed worker thread that has no relationship to that request. The
carrier for bridging the gap has existed since the first schema
version — every domain record has a `traceContext` map, PostgreSQL
persists it in the `trace_context JSONB` column, `PublishOptions`
documents ambient capture, `EventContext` hands the map to the
handler — but nothing ever *captured* the context at publish time and
nothing *activated* it around handler invocation. The map was inert
data end-to-end.

The existing propagation machinery cannot close this gap:

- The starter's `ContextPropagatingTaskDecorator` (ADR-0009) copies
  the context of the *submitting* thread — the long-lived poller
  daemon, which never carries the publisher's trace. It solves
  thread-hop propagation within one JVM, not the publish → handle hop
  through the database.
- `OutboxListener` (ADR-0013) callbacks are notification-only: no
  around-advice, no way to own a thread-local scope for the duration
  of `handler.handle(...)`, and `onHandlerError` fires after the
  dispatcher already converted the exception to a `Retry` outcome.

Core must stay dependency-free (ADR-0010/0016: only `-api`, `-spi`,
SLF4J), so neither the OpenTelemetry API nor Micrometer Tracing may
appear in `event-outboxer-core`.

## Decision

### New SPI port `OutboxTracer` (in `event-outboxer-spi`)

Two coarse-grained, handle-returning operations — span lifecycle and
scope management stay entirely inside the adapter:

- `PublishSpan startPublishSpan(UUID eventId, String eventType)` —
  PRODUCER span `"outbox publish <eventType>"`, child of the calling
  thread's current context. `PublishSpan.contextToStore()` returns the
  flat carrier map to persist; `coalesced(UUID)` tags the ADR-0021
  coalesce outcome; `error(Throwable)` records a failed insert;
  `close()` ends the span. Nothing is made current — the caller's own
  span stays active.
- `ProcessSpan startProcessSpan(ProcessSpanInfo info)` — extracts the
  parent from the stored map, starts a CONSUMER span
  `"outbox process <eventType>"` and makes it (plus extracted baggage)
  current on the worker thread until `close()`.

`OutboxTracer.NOOP` (a `NoopOutboxTracer`, mirroring
`EntityLocker.NOOP`) keeps every un-traced deployment zero-cost.
`OutboxTraceAttributes` centralizes the attribute keys both adapters
share: OTel messaging semconv (`messaging.system=event_outboxer`,
`messaging.operation.type=send|process`,
`messaging.destination.name`, `messaging.message.id`) plus
outbox-specific `event_outboxer.attempt`, `event_outboxer.worker.id`,
`event_outboxer.coalesced_into`.

### Engine integration (core)

- `SafeOutboxTracer` (core-internal decorator) wraps the configured
  tracer once in `OutboxEngineBuilder.build()`: every port and handle
  method is shielded with catch-and-debug-log, so a broken tracing
  backend can never fail a publish or leave an event in PROCESSING.
- `DefaultOutboxEventPublisher` starts one PRODUCER span per event —
  around `save`/`saveCoalescing` in `publish()`, per request in
  `publishAll()` (batch-path spans stay open until `saveAll`).
  Serialization stays outside the span (caller bug, no event yet).
  **The explicit `PublishOptions.traceContext` override wins** over
  the captured context — this finally implements the documented
  contract; the producer span is still recorded in the caller's trace.
  On coalesce the span ends normally, tagged
  `event_outboxer.coalesced_into=<existingId>`: the surviving row
  keeps the *first* publish's context, and the tag is the trace-side
  breadcrumb explaining why this producer has no consumer child.
- `HandlerDispatcher` opens the CONSUMER span around
  `invokeHandler(...)` **only** — not deserialization or lock
  acquisition (those release without consuming an attempt and are not
  processing attempts), and not finalize (with group commit,
  ADR-batching, `markProcessed` may complete on a different thread;
  the span and its thread-local scope must close on the worker thread
  first). The handler exception is recorded on the span
  (`error(ex)`) before the existing conversion to
  `EventOutcome.Retry` runs. Each retry re-enters the same path and
  gets a fresh span with `attempt = attempts()+1`, parented by the
  same stored context — one trace, one span per attempt.
- `EventContext` still receives the raw stored map: the handler-facing
  API is unchanged.

### Carrier format

Flat string→string only — `FlatMapJson` in the PostgreSQL adapter
rejects nested structures. W3C baggage is stored as the single
comma-joined `baggage` header value; the key set follows the
configured propagator (`b3` instead of `traceparent` is legal). The
nested-`baggage` shape previously shown in STORAGE.md was wrong and
is corrected in the same change.

### Two adapter modules

- `event-outboxer-tracing-otel` — depends only on
  `io.opentelemetry:opentelemetry-api` (version from the Boot BOM's
  `opentelemetry-bom`). Uses the `OpenTelemetry` instance's configured
  propagators (not hardcoded W3C), so the stored keys honour
  `OTEL_PROPAGATORS`. Works with the OTel Java agent
  (`GlobalOpenTelemetry`) and Boot's bridge alike; an unconfigured
  instance degrades to an empty stored map.
- `event-outboxer-tracing-micrometer` — depends on
  `io.micrometer:micrometer-tracing`; propagation format and baggage
  follow Boot's `management.tracing.propagation.*` /
  `baggage.remote-fields`, and both bridges (OTel, Brave) are
  supported. `Span.error()` semantics are bridge-defined; tags are
  strings. *(How it drives Micrometer — `Tracer` + `Propagator`
  directly — is superseded by the 2026-08-16 amendment below; the
  emitted span model is unchanged.)*

Both follow the `event-outboxer-metrics-micrometer` module pattern:
`-api` + `-spi` + the tracing library, `ban-core-in-adapter` enforcer.

### Starter auto-configuration

Two entries in `AutoConfiguration.imports`, both guarded by
`event-outboxer.tracing.enabled` (default `true`) and
`@ConditionalOnMissingBean(OutboxTracer.class)`:

- `MicrometerTracingAutoConfiguration` — `@ConditionalOnBean({Tracer,
  Propagator})`, runs after actuator's tracing auto-configuration.
  (The 2026-08-16 amendment adds `ObservationRegistry` to the
  condition set.)
- `OtelTracingAutoConfiguration` — runs after the micrometer one;
  `OpenTelemetry` bean if present, else `GlobalOpenTelemetry.get()`
  (the Java-agent case).

**Micrometer wins when both adapters are on the classpath**: it is
Boot's first-class tracing abstraction, so the outbox carrier matches
every other outbound carrier the application emits. A user-defined
`OutboxTracer` bean beats both. `OutboxEngineAutoConfiguration`
injects the resolved tracer (`ObjectProvider`, NOOP fallback) into
both the publisher bean and the engine builder.

### The Micrometer adapter instruments through the Observation API (amendment, 2026-08-16)

**Problem.** The original `MicrometerOutboxTracer` created spans with
`tracer.spanBuilder()` and made them current with `tracer.withSpan()`.
That fills the tracing thread-locals but leaves
`ObservationRegistry.getCurrentObservation()` `null`, and the
*observation* is what context propagation actually carries:
`ObservationThreadLocalAccessor` is the only `ThreadLocalAccessor`
registered by default, so `ContextSnapshot` — and with it Spring's
`ContextPropagatingTaskDecorator`, Reactor's `contextCapture()` and
every `@Async` hop — copies nothing when only a raw span is current.
Verified against the real `micrometer-tracing-bridge-otel` + OTel SDK:
work offloaded from inside a handler landed in a **separate trace with
no parent**. Same-thread instrumentation was unaffected (the OTel
bridge falls back to the thread's current context), which is why the
gap is invisible in a `SimpleTracer` test double.

A second, smaller defect had the same root: `Propagator.extract()`
resolves against `Context.current()` on both bridges, so a consumer
span started from an *empty* stored carrier silently adopted whatever
span happened to be current on the worker thread — where the OTel
adapter is explicitly isolated via `Context.root()`.

**Decision.** `MicrometerOutboxTracer(ObservationRegistry, Tracer)`
drives Micrometer's Observation API and lets the registered
`ObservationHandler`s own span creation:

- Publish side: an observation over a `SenderContext` (`Kind.PRODUCER`)
  whose carrier is the map persisted with the event.
  `PropagatingSenderTracingObservationHandler` creates the producer
  span and injects into that carrier during `start()`;
  `contextToStore()` reads it back. The observation is started but
  **never scoped** — the SPI forbids making anything current on the
  caller's thread.
- Handle side: an observation over a `ReceiverContext`
  (`Kind.CONSUMER`) carrying the stored map, so
  `PropagatingReceiverTracingObservationHandler` extracts the parent
  and creates the consumer span. `openScope()` on the worker thread
  makes both the span *and the observation* current until the handle
  closes — which is what fixes the thread-hop loss.
- The worker thread is detached (`tracer.withSpan(null)`, bridge-
  agnostic: OTel resets to `Context.root()`, Brave clears the current
  context) around `observation.start()`, so the consumer parent comes
  from the stored carrier or from nowhere. This is the only reason the
  adapter still needs a `Tracer`. That covers the *span* parent only:
  `Observation.createNotStarted` captures the thread's current
  observation as the observation-tree parent before any of this, so
  the handle side also calls `.parentObservation(null)`. Both are
  needed; neither implies the other.
- `start()` has meter side effects (below) and `openScope()` makes the
  observation current *before* notifying handlers, so both are wrapped:
  a throw on the way out stops the observation and releases the
  registry's thread-local. `SafeOutboxTracer` swallows what the adapter
  rethrows, so nothing else would ever clean up — a half-built handle
  would pin a `LongTaskTimer` sample forever and leak the current
  observation onto a pooled worker thread.
- Span names, kinds and `OutboxTraceAttributes` are unchanged. Names
  move to `contextualName` (`outbox publish <eventType>`), the
  observation name is `<prefix>.publish` / `<prefix>.process` where
  `<prefix>` is the same `event-outboxer.metrics.prefix` slot
  `MicrometerOutboxListener` honours (the observations become meters,
  so they must live in one namespace with the rest), and attributes
  split by cardinality: the three `messaging.*` keys are
  low-cardinality, ids / attempt / worker / `coalesced_into` are
  high-cardinality (span-only). All of them still land on the span as
  string tags.
- The starter's `@ConditionalOnBean` grows `ObservationRegistry`.
  `Tracer` and `Propagator` stay in the condition set as the signal
  that Boot registered the propagating tracing handlers: without them
  the registry would produce timers and no spans, and the OTel adapter
  is the better fallback.

**Consequences.**

- Handler code that offloads work keeps the trace, and any
  Observation-based instrumentation inside the handler nests under the
  consumer span. Both are covered by module tests running the real
  OTel bridge (`MicrometerOutboxTracerOtelBridgeTest`).
- **Four meters appear as a side effect.** Boot's
  `DefaultMeterObservationHandler` turns each observation into a
  `Timer` and, by default, a `LongTaskTimer`, so the module registers
  `<prefix>.publish{,.active}` and `<prefix>.process{,.active}`,
  tagged with the low-cardinality `messaging.*` keys plus Micrometer's
  `error`. They ship no SLO buckets and no dashboard panels. Two
  caveats make them weaker than they look, both documented in
  OBSERVABILITY.md: `<prefix>.process` wraps only `handler.handle(...)`
  and counts every attempt, so it is *not* interchangeable with
  `<prefix>.events.processing_time` (claim → finalize, success only,
  tagged `event_type`); and on the `publishAll` batch path every handle
  stays open until the single batch insert returns, so the publish
  timer's per-event distribution is an artefact. `MeterFilter` removes
  them cleanly.
- **The event type becomes a real meter tag.** `messaging.destination.name`
  is low-cardinality, so the adapter inherits the assumption
  `MicrometerOutboxListener` already makes with `event_type`: event
  types are a bounded, code-defined set.
- **Observations are filterable by name from the outside.** Boot's
  `management.observations.enable.*` can no-op the whole adapter —
  spans and stored carrier included — with nothing logged. The metrics
  escape hatch is a `MeterFilter`; the docs say so explicitly because
  reaching for the observation property to silence the meters would
  silently disable tracing.
- The adapter now compiles against `micrometer-observation` (declared
  explicitly) in addition to `micrometer-tracing`.
- Span naming depends on the handler set: Boot groups tracing handlers
  into a first-matching composite, so the propagating handlers own our
  contexts and the name is used verbatim. A hand-built registry that
  only has `DefaultTracingObservationHandler` names the span through
  `SpanNameUtil.toLowerHyphen` (`outbox publish -order-created`) and
  never injects the carrier. `MicrometerTracingBootWiringTest` in the
  starter boots Boot's own tracing auto-configuration and asserts the
  grouping, so the assumption is checked against the real thing and not
  only against a hand-built registry.
- `MicrometerOutboxTracer(Tracer, Propagator)` is replaced by
  `(ObservationRegistry, Tracer[, prefix])`, not deprecated: the module
  is unreleased (first ships in 0.3.0), so nothing published depends on
  it.
- The OTel adapter is untouched and stays handle-based on the OTel API:
  it targets deployments with no `ObservationRegistry`, and it was
  already isolated from ambient context (`Context.root()` on extract).
  Its consumer scope is a plain OTel `Context`, so a handler's own
  thread hop is covered by the Java agent's executor instrumentation
  (or `Context.taskWrapping`), never by `ContextSnapshot` — which is a
  property of OTel context in a Boot app, not a defect of the adapter,
  and one more reason the starter prefers the Micrometer adapter when
  both are wireable.

## Alternatives considered

1. **`OutboxListener`-based tracing** (the reuse ADR-0013
   anticipated). Rejected: callbacks are fire-and-forget
   notifications — they cannot bracket the handler invocation with a
   thread-local scope, cannot return a span handle, and see handler
   failures only after outcome conversion. Retrofitting an
   around-advice shape onto the listener would break its 21-method
   contract and its "MUST NOT block" semantics.
2. **`TaskDecorator`-based propagation** (extend ADR-0009). Rejected:
   the decorator captures the poller thread's context, which never
   contains the publish-time trace — the parent lives in the database
   row, not in any live thread. A decorator also wraps the whole
   `dispatch()` runnable opaquely and cannot see the `ClaimedEvent`.
3. **Tracing library directly in core.** Rejected: violates the
   dependency-free-core invariant (ADR-0010/0016) and would force one
   tracing ecosystem on every consumer.
4. **Single adapter module only.** Rejected by maintainer decision:
   pure-OTel deployments (Java agent) and Boot-idiomatic
   micrometer-tracing deployments both matter; each gets a first-class
   adapter and the starter arbitrates.

Considered for the 2026-08-16 amendment:

5. **Keep the raw `Tracer` API and document the thread-hop caveat.**
   Rejected: the failure is silent (spans land in a foreign trace, no
   error anywhere) and the fix would be the handler author's, in every
   handler that offloads work. The adapter can close it once.
6. **Callback-shaped SPI** (`tracer.aroundHandle(info, callback)`)
   instead of returning handles, mirroring `Observation.observe(...)`.
   Rejected: a scope can never leak that way, but the port would then
   wrap the engine's own work — a throwing handler could fail a
   publish or leave an event in PROCESSING — and `coalesced(...)` /
   deferred `close()` on the `publishAll` batch path have no callback
   shape. `openScope()` gives the same propagation with the engine
   still in control (and `SafeOutboxTracer` still shielding it).
7. **Observation API in the OTel adapter too.** Not applicable: that
   adapter targets deployments whose instrumentation is the OTel agent
   or a raw SDK, where there is no `ObservationRegistry` and executor
   propagation is handled by the agent.

## Consequences

- Traces continue across the outbox hop: caller span → PRODUCER span →
  per-attempt CONSUMER spans, all in one trace; handler code sees the
  restored context (and baggage) as current, so its own spans nest
  correctly.
- Module count 16 → 18 (ADR-0016 amended).
- New public API surface: `OutboxTracer`, `OutboxTraceAttributes`
  (SPI), `OutboxEngineBuilder.tracer(...)`, constructor overloads on
  `DefaultOutboxEventPublisher` / `HandlerDispatcher`,
  `event-outboxer.tracing.enabled` property. All additive; japicmp
  clean.
- Coalesced publishes (ADR-0021) intentionally do NOT link the second
  publisher's trace to the surviving event beyond the
  `coalesced_into` tag; span links may be added later if operators
  need stronger correlation.
- The poller, maintenance tasks and finalize path remain untraced by
  design (ADR-0009: background operations must not pollute traces).
