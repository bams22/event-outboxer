/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.tracing.otel;

import io.github.bams22.outboxer.spi.OutboxTraceAttributes;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;

/**
 * {@link OutboxTracer} implementation on the OpenTelemetry API (ADR-0023).
 *
 * <p>Publish side: starts a {@link SpanKind#PRODUCER} span {@code "outbox publish <eventType>"}
 * as a child of the calling thread's current context, and injects {@code
 * Context.current().with(span)} — the producer span's {@code traceparent} plus the caller's
 * baggage — into a flat string map for persistence in the event row. Nothing is made current;
 * the caller's own span stays active.
 *
 * <p>Handle side: extracts the stored map, starts a {@link SpanKind#CONSUMER} span {@code
 * "outbox process <eventType>"} as its child and makes it (baggage included) current on the
 * worker thread until the handle closes.
 *
 * <p>Propagation format follows the {@link OpenTelemetry} instance's configured propagators
 * (default: W3C tracecontext + baggage), so the stored keys match every other carrier the
 * application emits. With an unconfigured/no-op {@code OpenTelemetry} the captured map is empty
 * and the adapter degrades gracefully.
 *
 * <p>Thread-safe; a single instance serves the publisher and every worker thread.
 */
public final class OtelOutboxTracer implements OutboxTracer {

  /**
   * Instrumentation scope name reported on every span emitted by this adapter.
   */
  public static final String INSTRUMENTATION_SCOPE = "io.github.bams22.event-outboxer";

  private final Tracer tracer;
  private final TextMapPropagator propagator;

  public OtelOutboxTracer(OpenTelemetry openTelemetry) {
    Objects.requireNonNull(openTelemetry, "openTelemetry must not be null");
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
  }

  @Override
  public PublishSpan startPublishSpan(UUID eventId, String eventType) {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Span span =
        tracer
            .spanBuilder("outbox publish " + eventType)
            .setSpanKind(SpanKind.PRODUCER)
            .setAttribute(
                OutboxTraceAttributes.MESSAGING_SYSTEM, OutboxTraceAttributes.MESSAGING_SYSTEM_VALUE)
            .setAttribute(
                OutboxTraceAttributes.MESSAGING_OPERATION_TYPE, OutboxTraceAttributes.OPERATION_SEND)
            .setAttribute(OutboxTraceAttributes.MESSAGING_DESTINATION_NAME, eventType)
            .setAttribute(OutboxTraceAttributes.MESSAGING_MESSAGE_ID, eventId.toString())
            .startSpan();
    // Inject current().with(span): the producer span's traceparent PLUS the caller's baggage
    // (baggage lives on the Context, not on the span).
    Map<String, String> carrier = new HashMap<>();
    propagator.inject(Context.current().with(span), carrier, MapSetter.INSTANCE);
    return new OtelPublishSpan(span, Map.copyOf(carrier));
  }

  @Override
  public ProcessSpan startProcessSpan(ProcessSpanInfo info) {
    Objects.requireNonNull(info, "info must not be null");
    Context parent = propagator.extract(Context.root(), info.storedContext(), MapGetter.INSTANCE);
    Span span =
        tracer
            .spanBuilder("outbox process " + info.eventType())
            .setParent(parent)
            .setSpanKind(SpanKind.CONSUMER)
            .setAttribute(
                OutboxTraceAttributes.MESSAGING_SYSTEM, OutboxTraceAttributes.MESSAGING_SYSTEM_VALUE)
            .setAttribute(
                OutboxTraceAttributes.MESSAGING_OPERATION_TYPE,
                OutboxTraceAttributes.OPERATION_PROCESS)
            .setAttribute(OutboxTraceAttributes.MESSAGING_DESTINATION_NAME, info.eventType())
            .setAttribute(OutboxTraceAttributes.MESSAGING_MESSAGE_ID, info.eventId().toString())
            .setAttribute(OutboxTraceAttributes.ATTEMPT, (long) info.attempt())
            .setAttribute(OutboxTraceAttributes.WORKER_ID, info.workerId().value())
            .startSpan();
    // parent.with(span), NOT current().with(span): restores the extracted baggage for the
    // handler alongside the new span.
    Scope scope = parent.with(span).makeCurrent();
    return new OtelProcessSpan(span, scope);
  }

  private static final class OtelPublishSpan implements PublishSpan {

    private final Span span;
    private final Map<String, String> context;
    private final AtomicBoolean closed = new AtomicBoolean();

    private OtelPublishSpan(Span span, Map<String, String> context) {
      this.span = span;
      this.context = context;
    }

    @Override
    public Map<String, String> contextToStore() {
      return context;
    }

    @Override
    public void coalesced(UUID existingEventId) {
      span.setAttribute(OutboxTraceAttributes.COALESCED_INTO, existingEventId.toString());
    }

    @Override
    public void error(Throwable error) {
      span.recordException(error);
      span.setStatus(StatusCode.ERROR, error.toString());
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        span.end();
      }
    }
  }

  private static final class OtelProcessSpan implements ProcessSpan {

    private final Span span;
    private final Scope scope;
    private final AtomicBoolean closed = new AtomicBoolean();

    private OtelProcessSpan(Span span, Scope scope) {
      this.span = span;
      this.scope = scope;
    }

    @Override
    public void error(Throwable error) {
      span.recordException(error);
      span.setStatus(StatusCode.ERROR, error.toString());
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        scope.close();
        span.end();
      }
    }
  }

  private enum MapSetter implements TextMapSetter<Map<String, String>> {
    INSTANCE;

    @Override
    public void set(@Nullable Map<String, String> carrier, String key, String value) {
      if (carrier != null) {
        carrier.put(key, value);
      }
    }
  }

  private enum MapGetter implements TextMapGetter<Map<String, String>> {
    INSTANCE;

    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
      return carrier.keySet();
    }

    @Override
    public @Nullable String get(@Nullable Map<String, String> carrier, String key) {
      return carrier == null ? null : carrier.get(key);
    }
  }
}
