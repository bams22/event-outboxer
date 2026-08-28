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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Distributed-tracing port (ADR-0023). The engine calls this around every event insert (the
 * PRODUCER side of the outbox hop) and around every handler invocation (the CONSUMER side); adapter
 * modules bridge it to OpenTelemetry ({@code event-outboxer-tracing-otel}) or Micrometer Tracing
 * ({@code event-outboxer-tracing-micrometer}). {@link #NOOP} keeps the core engine dependency-free
 * when no tracing backend is wired.
 *
 * <p>The carrier between the two sides is the event's {@code traceContext} map — a FLAT
 * string-to-string map (W3C {@code traceparent} / {@code tracestate} / {@code baggage} header
 * values, or whatever the configured propagator emits) persisted with the event row and read back
 * on claim. Values must be plain strings: the PostgreSQL adapter rejects nested structures.
 *
 * <p>How the CONSUMER span relates to the stored context is decided by the engine at publish time
 * and handed to the adapter as {@link ProcessSpanInfo#propagation()}: {@link Propagation#CHILD} for
 * events processed soon after publication, {@link Propagation#LINK} for events scheduled far enough
 * into the future that a parent-child relationship would stretch one trace across the delay
 * (ADR-0023, 2026-08-28 amendment). Adapters never see the marker the engine persists to carry that
 * decision — they only see the outcome.
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
     * Starts a CONSUMER span (conventionally named {@code "outbox process <eventType>"}) and makes
     * it — along with any baggage extracted from {@link ProcessSpanInfo#storedContext()} — current
     * on the calling thread until {@link ProcessSpan#close()}.
     *
     * <p>With {@link Propagation#CHILD} the span is a child of the stored context. With {@link
     * Propagation#LINK} it is a new root span carrying a span link to the stored context (where the
     * backend supports links) and the attribute {@link OutboxTraceAttributes#PROPAGATION} {@code =}
     * {@link OutboxTraceAttributes#PROPAGATION_LINK}; baggage is restored either way.
     */
    ProcessSpan startProcessSpan(ProcessSpanInfo info);

    /** No-op tracer used when no tracing adapter is wired. */
    OutboxTracer NOOP = new NoopOutboxTracer();

    /** How the CONSUMER span relates to the context stored at publish time. */
    enum Propagation {
        /** The consumer span is a child of the stored context — one trace across the outbox hop. */
        CHILD,
        /**
         * The consumer span is a new root that links to the stored context. Chosen by the engine
         * when the event was scheduled further into the future than the configured link threshold.
         */
        LINK
    }

    /**
     * Handle for the publish-side PRODUCER span. The engine closes it exactly once via
     * try-with-resources; implementations must tolerate double-close.
     */
    interface PublishSpan extends AutoCloseable {

        /**
         * Flat string-to-string carrier (for example {@code traceparent} / {@code tracestate} /
         * {@code baggage}) holding this span's context, to be persisted in the event row's {@code
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
         * The event was scheduled beyond the engine's link threshold, so its consumer span will
         * {@linkplain Propagation#LINK link} to this span instead of descending from it.
         * Implementations should tag the span {@link OutboxTraceAttributes#PROPAGATION} {@code =}
         * {@link OutboxTraceAttributes#PROPAGATION_LINK} so operators can see why this producer has
         * no consumer child. Called at most once, before {@link #close()}. Default: no-op.
         */
        default void linked() {}

        /** Records a publish failure on the span (exception plus ERROR status). */
        void error(Throwable error);

        /** Ends the span. Idempotent; never makes or leaves anything current. */
        @Override
        void close();
    }

    /**
     * Handle for the handle-side CONSUMER span; owns the "current" scope on the thread that called
     * {@link #startProcessSpan(ProcessSpanInfo)}.
     */
    interface ProcessSpan extends AutoCloseable {

        /** Records the handler exception on the span (exception plus ERROR status). */
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
     * @param storedContext flat carrier map read from the event row, with engine-internal markers
     *     already removed; never null (empty map allowed)
     * @param propagation how the span relates to {@code storedContext}; decided by the engine at
     *     publish time
     */
    record ProcessSpanInfo(
            UUID eventId,
            String eventType,
            int attempt,
            WorkerId workerId,
            Map<String, String> storedContext,
            Propagation propagation) {

        public ProcessSpanInfo {
            Objects.requireNonNull(eventId, "eventId must not be null");
            Objects.requireNonNull(eventType, "eventType must not be null");
            Objects.requireNonNull(workerId, "workerId must not be null");
            Objects.requireNonNull(storedContext, "storedContext must not be null");
            Objects.requireNonNull(propagation, "propagation must not be null");
            storedContext = Map.copyOf(storedContext);
        }

        /** Pre-0.4.0 shape: {@link Propagation#CHILD}. */
        public ProcessSpanInfo(
                UUID eventId,
                String eventType,
                int attempt,
                WorkerId workerId,
                Map<String, String> storedContext) {
            this(eventId, eventType, attempt, workerId, storedContext, Propagation.CHILD);
        }
    }
}
