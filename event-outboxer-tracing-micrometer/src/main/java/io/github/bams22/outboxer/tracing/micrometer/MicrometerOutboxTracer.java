/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.tracing.micrometer;

import io.github.bams22.outboxer.spi.OutboxTraceAttributes;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link OutboxTracer} implementation on Micrometer Tracing (ADR-0023).
 *
 * <p>Publish side: starts a {@link Span.Kind#PRODUCER} span {@code "outbox publish <eventType>"} as
 * a child of the current context and injects the span's context into a flat string map for
 * persistence in the event row. Handle side: extracts the stored map via the configured {@link
 * Propagator}, starts a {@link Span.Kind#CONSUMER} span {@code "outbox process <eventType>"} and
 * makes it current on the worker thread until the handle closes.
 *
 * <p>Behavioral notes vs the OpenTelemetry adapter ({@code event-outboxer-tracing-otel}):
 *
 * <ul>
 *   <li>Propagation format follows Spring Boot's {@code management.tracing.propagation.*} settings
 *       — the stored keys may be {@code b3} instead of {@code traceparent}. Values stay flat
 *       strings either way.
 *   <li>Baggage propagation follows the bridge's configuration ({@code
 *       management.tracing.baggage.remote-fields} on Boot).
 *   <li>{@link Span#error(Throwable)} semantics are bridge-dependent: the OTel bridge records an
 *       exception event plus ERROR status, the Brave bridge sets an error tag.
 *   <li>All span attributes are string tags (Micrometer's {@code tag(String, String)}), so {@code
 *       event_outboxer.attempt} appears as a string.
 * </ul>
 *
 * <p>Thread-safe; a single instance serves the publisher and every worker thread.
 */
public final class MicrometerOutboxTracer implements OutboxTracer {

    private final Tracer tracer;
    private final Propagator propagator;

    public MicrometerOutboxTracer(Tracer tracer, Propagator propagator) {
        this.tracer = Objects.requireNonNull(tracer, "tracer must not be null");
        this.propagator = Objects.requireNonNull(propagator, "propagator must not be null");
    }

    @Override
    public PublishSpan startPublishSpan(UUID eventId, String eventType) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Span span =
                tracer.spanBuilder()
                        .name("outbox publish " + eventType)
                        .kind(Span.Kind.PRODUCER)
                        .tag(
                                OutboxTraceAttributes.MESSAGING_SYSTEM,
                                OutboxTraceAttributes.MESSAGING_SYSTEM_VALUE)
                        .tag(
                                OutboxTraceAttributes.MESSAGING_OPERATION_TYPE,
                                OutboxTraceAttributes.OPERATION_SEND)
                        .tag(OutboxTraceAttributes.MESSAGING_DESTINATION_NAME, eventType)
                        .tag(OutboxTraceAttributes.MESSAGING_MESSAGE_ID, eventId.toString())
                        .start();
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(span.context(), carrier, Map::put);
        return new MicrometerPublishSpan(span, Map.copyOf(carrier));
    }

    @Override
    public ProcessSpan startProcessSpan(ProcessSpanInfo info) {
        Objects.requireNonNull(info, "info must not be null");
        Span span =
                propagator
                        .extract(info.storedContext(), Map::get)
                        .name("outbox process " + info.eventType())
                        .kind(Span.Kind.CONSUMER)
                        .tag(
                                OutboxTraceAttributes.MESSAGING_SYSTEM,
                                OutboxTraceAttributes.MESSAGING_SYSTEM_VALUE)
                        .tag(
                                OutboxTraceAttributes.MESSAGING_OPERATION_TYPE,
                                OutboxTraceAttributes.OPERATION_PROCESS)
                        .tag(OutboxTraceAttributes.MESSAGING_DESTINATION_NAME, info.eventType())
                        .tag(OutboxTraceAttributes.MESSAGING_MESSAGE_ID, info.eventId().toString())
                        .tag(OutboxTraceAttributes.ATTEMPT, String.valueOf(info.attempt()))
                        .tag(OutboxTraceAttributes.WORKER_ID, info.workerId().value())
                        .start();
        Tracer.SpanInScope scope = tracer.withSpan(span);
        return new MicrometerProcessSpan(span, scope);
    }

    private static final class MicrometerPublishSpan implements PublishSpan {

        private final Span span;
        private final Map<String, String> context;
        private final AtomicBoolean closed = new AtomicBoolean();

        private MicrometerPublishSpan(Span span, Map<String, String> context) {
            this.span = span;
            this.context = context;
        }

        @Override
        public Map<String, String> contextToStore() {
            return context;
        }

        @Override
        public void coalesced(UUID existingEventId) {
            span.tag(OutboxTraceAttributes.COALESCED_INTO, existingEventId.toString());
        }

        @Override
        public void error(Throwable error) {
            span.error(error);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                span.end();
            }
        }
    }

    private static final class MicrometerProcessSpan implements ProcessSpan {

        private final Span span;
        private final Tracer.SpanInScope scope;
        private final AtomicBoolean closed = new AtomicBoolean();

        private MicrometerProcessSpan(Span span, Tracer.SpanInScope scope) {
            this.span = span;
            this.scope = scope;
        }

        @Override
        public void error(Throwable error) {
            span.error(error);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                scope.close();
                span.end();
            }
        }
    }
}
