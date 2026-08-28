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

import io.github.bams22.outboxer.spi.OutboxTracer;
import io.micrometer.observation.Observation;
import io.micrometer.tracing.Link;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import java.util.Map;

/**
 * Tracing handler for the adapter's handle-side observation (ADR-0023, 2026-08-28 amendment).
 *
 * <p>For {@link OutboxTracer.Propagation#CHILD} it behaves exactly like Micrometer's {@link
 * PropagatingReceiverTracingObservationHandler}: the consumer span is a child of the context the
 * configured {@code Propagator} extracts from the stored carrier. For {@link
 * OutboxTracer.Propagation#LINK} it turns that span into a new root ({@link
 * Span.Builder#setNoParent()}) carrying a {@link Link} to the stored producer context. Because
 * Micrometer's {@code Propagator} only ever extracts into a {@code Span.Builder} — the parent's ids
 * are not readable from it — the link target is parsed from the carrier directly, in the three
 * formats Spring Boot can be configured to emit ({@code traceparent}, single-header {@code b3},
 * multi-header {@code X-B3-*}). A carrier in any other format yields an unlinked root span.
 *
 * <p>The extracted parent is still set on the builder before {@code setNoParent()}: on the OTel
 * bridge that is what keeps the extracted <em>baggage</em> attached to the new span, so the handler
 * sees the publisher's baggage in both modes. The Brave bridge ignores {@code setNoParent()} (and
 * renders links as tags), so on Brave a deferred event keeps the parent-child shape.
 *
 * <p>Registration: the handler only claims {@link OutboxReceiverContext} and must sit <em>ahead
 * of</em> the generic receiver handler in the first-matching group, or the generic handler wins and
 * every consumer span stays a child. The Spring Boot starter registers it as a bean ordered before
 * Boot's {@code RECEIVER_TRACING_OBSERVATION_HANDLER_ORDER}; a hand-built registry adds it as the
 * first member of its tracing composite.
 */
public class OutboxReceiverTracingObservationHandler
        extends PropagatingReceiverTracingObservationHandler<OutboxReceiverContext> {

    private final Tracer tracer;

    public OutboxReceiverTracingObservationHandler(Tracer tracer, Propagator propagator) {
        super(tracer, propagator);
        this.tracer = tracer;
    }

    @Override
    public boolean supportsContext(Observation.Context context) {
        return context instanceof OutboxReceiverContext;
    }

    @Override
    public Span.Builder customizeExtractedSpan(
            OutboxReceiverContext context, Span.Builder builder) {
        if (context.propagation() != OutboxTracer.Propagation.LINK) {
            return builder;
        }
        builder.setNoParent();
        Map<String, String> carrier = context.getCarrier();
        TraceContext target = carrier == null ? null : StoredTraceContexts.parse(tracer, carrier);
        if (target != null) {
            builder.addLink(new Link(target));
        }
        return builder;
    }
}
