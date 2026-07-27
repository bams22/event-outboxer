# ADR-0023: Trace continuity via an OutboxTracer SPI port with OpenTelemetry and Micrometer adapters

## Status

Accepted.

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
  strings.

Both follow the `event-outboxer-metrics-micrometer` module pattern:
`-api` + `-spi` + the tracing library, `ban-core-in-adapter` enforcer.

### Starter auto-configuration

Two entries in `AutoConfiguration.imports`, both guarded by
`event-outboxer.tracing.enabled` (default `true`) and
`@ConditionalOnMissingBean(OutboxTracer.class)`:

- `MicrometerTracingAutoConfiguration` — `@ConditionalOnBean({Tracer,
  Propagator})`, runs after actuator's tracing auto-configuration.
- `OtelTracingAutoConfiguration` — runs after the micrometer one;
  `OpenTelemetry` bean if present, else `GlobalOpenTelemetry.get()`
  (the Java-agent case).

**Micrometer wins when both adapters are on the classpath**: it is
Boot's first-class tracing abstraction, so the outbox carrier matches
every other outbound carrier the application emits. A user-defined
`OutboxTracer` bean beats both. `OutboxEngineAutoConfiguration`
injects the resolved tracer (`ObjectProvider`, NOOP fallback) into
both the publisher bean and the engine builder.

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
