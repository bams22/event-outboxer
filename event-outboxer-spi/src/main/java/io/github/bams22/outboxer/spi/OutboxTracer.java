/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import io.github.bams22.outboxer.domain.WorkerId;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Distributed-tracing port (ADR-0023). The engine calls this around every event insert (the
 * PRODUCER side of the outbox hop) and around every handler invocation (the CONSUMER side);
 * adapter modules bridge it to OpenTelemetry ({@code event-outboxer-tracing-otel}) or Micrometer
 * Tracing ({@code event-outboxer-tracing-micrometer}). {@link #NOOP} keeps the core engine
 * dependency-free when no tracing backend is wired.
 *
 * <p>The carrier between the two sides is the event's {@code traceContext} map — a FLAT
 * string-to-string map (W3C {@code traceparent} / {@code tracestate} / {@code baggage} header
 * values, or whatever the configured propagator emits) persisted with the event row and read back
 * on claim. Values must be plain strings: the PostgreSQL adapter rejects nested structures.
 *
 * <p>Implementations MUST be thread-safe and SHOULD be cheap when no trace is active. The engine
 * additionally shields itself from implementation failures (every call is routed through a
 * defensive wrapper in core), but adapters should still not throw.
 */
public interface OutboxTracer {

  /**
   * Starts a PRODUCER span (conventionally named {@code "outbox publish <eventType>"}) as a child
   * of the calling thread's current trace context. Never returns {@code null}; when tracing is
   * inactive the returned handle must be a functional no-op whose {@link
   * PublishSpan#contextToStore()} is an empty map. Must NOT make anything current on the calling
   * thread — the caller's own span stays active.
   */
  PublishSpan startPublishSpan(UUID eventId, String eventType);

  /**
   * Extracts the parent context from {@link ProcessSpanInfo#storedContext()}, starts a CONSUMER
   * span (conventionally named {@code "outbox process <eventType>"}) as its child, and makes it —
   * along with any extracted baggage — current on the calling thread until {@link
   * ProcessSpan#close()}.
   */
  ProcessSpan startProcessSpan(ProcessSpanInfo info);

  /** No-op tracer used when no tracing adapter is wired. */
  OutboxTracer NOOP = new NoopOutboxTracer();

  /**
   * Handle for the publish-side PRODUCER span. The engine closes it exactly once via
   * try-with-resources; implementations must tolerate double-close.
   */
  interface PublishSpan extends AutoCloseable {

    /**
     * Flat string-to-string carrier (for example {@code traceparent} / {@code tracestate} / {@code
     * baggage}) holding this span's context, to be persisted in the event row's {@code
     * trace_context} column. Immutable; empty when tracing is inactive.
     */
    Map<String, String> contextToStore();

    /**
     * The insert coalesced into an existing PENDING event (ADR-0021) — the new event and its
     * captured context were discarded in favour of {@code existingEventId}. Implementations
     * should tag the span so operators can see why the surviving event's consumer span is not
     * this span's child. Not an error outcome.
     */
    void coalesced(UUID existingEventId);

    /**
     * Records a publish failure on the span (exception plus ERROR status).
     */
    void error(Throwable error);

    /**
     * Ends the span. Idempotent; never makes or leaves anything current.
     */
    @Override
    void close();
  }

  /**
   * Handle for the handle-side CONSUMER span; owns the "current" scope on the thread that called
   * {@link #startProcessSpan(ProcessSpanInfo)}.
   */
  interface ProcessSpan extends AutoCloseable {

    /**
     * Records the handler exception on the span (exception plus ERROR status).
     */
    void error(Throwable error);

    /**
     * Closes the current-scope, then ends the span. Idempotent; must run on the thread that
     * started the span.
     */
    @Override
    void close();
  }

  /**
   * Inputs for the CONSUMER span.
   *
   * @param eventId the event being processed
   * @param eventType the event type (span name suffix and {@code messaging.destination.name})
   * @param attempt 1-based number of the current processing attempt
   * @param workerId the worker executing the handler
   * @param storedContext raw flat carrier map read from the event row; never null (empty map
   *     allowed)
   */
  record ProcessSpanInfo(
      UUID eventId, String eventType, int attempt, WorkerId workerId,
      Map<String, String> storedContext) {

    public ProcessSpanInfo {
      Objects.requireNonNull(eventId, "eventId must not be null");
      Objects.requireNonNull(eventType, "eventType must not be null");
      Objects.requireNonNull(workerId, "workerId must not be null");
      storedContext =
          storedContext == null ? Map.of() : Collections.unmodifiableMap(Map.copyOf(storedContext));
    }
  }
}
