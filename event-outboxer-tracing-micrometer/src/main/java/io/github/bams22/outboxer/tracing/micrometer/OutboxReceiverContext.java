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
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.Propagator;
import io.micrometer.observation.transport.ReceiverContext;
import java.util.Map;
import java.util.Objects;

/**
 * The {@link ReceiverContext} of the handle-side observation. A dedicated type so that {@link
 * OutboxReceiverTracingObservationHandler} can claim it ahead of Micrometer's generic {@code
 * PropagatingReceiverTracingObservationHandler} and honour the engine's propagation decision
 * (ADR-0023, 2026-08-28 amendment). Everything else — carrier, getter, kind — is the plain
 * receiver-context shape the generic handler understands, which is also the graceful fallback when
 * the dedicated handler is not registered: the consumer span is then a child of the stored context
 * regardless of {@link #propagation()}.
 */
public final class OutboxReceiverContext extends ReceiverContext<Map<String, String>> {

    private final OutboxTracer.Propagation propagation;

    OutboxReceiverContext(
            Propagator.Getter<Map<String, String>> getter, OutboxTracer.Propagation propagation) {
        super(getter, Kind.CONSUMER);
        this.propagation = Objects.requireNonNull(propagation, "propagation must not be null");
    }

    /**
     * How the consumer span relates to the stored carrier, as decided by the engine at publish
     * time.
     */
    public OutboxTracer.Propagation propagation() {
        return propagation;
    }
}
