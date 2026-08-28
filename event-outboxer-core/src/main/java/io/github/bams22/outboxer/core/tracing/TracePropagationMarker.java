/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.tracing;

import io.github.bams22.outboxer.spi.OutboxTraceAttributes;
import io.github.bams22.outboxer.spi.OutboxTracer;
import java.util.HashMap;
import java.util.Map;

/**
 * The engine-internal marker that carries the publish-time propagation decision (ADR-0023,
 * 2026-08-28 amendment) inside the event row's {@code trace_context} carrier.
 *
 * <p>The publisher decides {@link OutboxTracer.Propagation#LINK} for events scheduled beyond the
 * link threshold and records it as the entry {@code event_outboxer.propagation=link} next to the
 * propagator's own keys — the only place a decision made at publish time can survive until the
 * event is claimed days later, without a schema change. The dispatcher reads and strips the entry
 * before the carrier reaches the tracer or the handler, so propagators, adapters and handler code
 * never see it. Absence means {@link OutboxTracer.Propagation#CHILD}.
 */
public final class TracePropagationMarker {

    /** Carrier key of the marker; deliberately the same string as the span attribute. */
    public static final String KEY = OutboxTraceAttributes.PROPAGATION;

    private TracePropagationMarker() {}

    /**
     * Returns a copy of {@code carrier} with the {@code link} marker added. An empty carrier stays
     * empty: with no stored context there is nothing to link to, and a marker-only map would just
     * be noise in the row.
     */
    public static Map<String, String> markLinked(Map<String, String> carrier) {
        if (carrier.isEmpty()) {
            return carrier;
        }
        Map<String, String> marked = new HashMap<>(carrier);
        marked.put(KEY, OutboxTraceAttributes.PROPAGATION_LINK);
        return Map.copyOf(marked);
    }

    /**
     * Propagation recorded in the stored carrier; {@link OutboxTracer.Propagation#CHILD} unless the
     * {@code link} marker is present.
     */
    public static OutboxTracer.Propagation propagationOf(Map<String, String> carrier) {
        return OutboxTraceAttributes.PROPAGATION_LINK.equals(carrier.get(KEY))
                ? OutboxTracer.Propagation.LINK
                : OutboxTracer.Propagation.CHILD;
    }

    /**
     * The carrier without the marker — what adapters and handlers receive. Returns the same
     * instance when there is nothing to strip.
     */
    public static Map<String, String> strip(Map<String, String> carrier) {
        if (!carrier.containsKey(KEY)) {
            return carrier;
        }
        Map<String, String> stripped = new HashMap<>(carrier);
        stripped.remove(KEY);
        return Map.copyOf(stripped);
    }
}
