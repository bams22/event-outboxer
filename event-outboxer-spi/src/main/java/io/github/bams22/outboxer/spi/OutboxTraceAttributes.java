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

/**
 * Span attribute keys and values shared by every {@link OutboxTracer} adapter, so that the
 * OpenTelemetry and Micrometer modules emit identical metadata (ADR-0023).
 *
 * <p>Standard keys follow the OTel messaging semantic conventions; outbox-specific keys use the
 * {@code event_outboxer.} prefix (matching the metrics prefix of {@code
 * event-outboxer-metrics-micrometer}).
 */
public final class OutboxTraceAttributes {

    /** OTel semconv: the messaging system identifier. */
    public static final String MESSAGING_SYSTEM = "messaging.system";

    /** Value for {@link #MESSAGING_SYSTEM}. */
    public static final String MESSAGING_SYSTEM_VALUE = "event_outboxer";

    /** OTel semconv: {@code send} on the publish side, {@code process} on the handle side. */
    public static final String MESSAGING_OPERATION_TYPE = "messaging.operation.type";

    /** Value for {@link #MESSAGING_OPERATION_TYPE} on the publish side. */
    public static final String OPERATION_SEND = "send";

    /** Value for {@link #MESSAGING_OPERATION_TYPE} on the handle side. */
    public static final String OPERATION_PROCESS = "process";

    /** OTel semconv: the event type acts as the destination name. */
    public static final String MESSAGING_DESTINATION_NAME = "messaging.destination.name";

    /** OTel semconv: the event UUID. */
    public static final String MESSAGING_MESSAGE_ID = "messaging.message.id";

    /** 1-based processing attempt; consumer spans only. */
    public static final String ATTEMPT = "event_outboxer.attempt";

    /** Worker executing the handler; consumer spans only. */
    public static final String WORKER_ID = "event_outboxer.worker.id";

    /**
     * Entity-lock key the handler ran under (ADR-0012); consumer spans of handlers that declare a
     * key only. High-cardinality by nature — the span is where "which key is hot" belongs, metrics
     * must not carry it.
     */
    public static final String LOCK_KEY = "event_outboxer.lock.key";

    /**
     * Milliseconds the handler thread spent acquiring the entity lock, the bounded wait of ADR-0035
     * included; consumer spans of handlers that declare a key only.
     */
    public static final String LOCK_WAIT_MS = "event_outboxer.lock.wait_ms";

    /**
     * UUID of the existing PENDING event a publish coalesced into (ADR-0021); producer spans only.
     */
    public static final String COALESCED_INTO = "event_outboxer.coalesced_into";

    /**
     * How the consumer span relates to the context stored at publish time (ADR-0023, 2026-08-28
     * amendment). Set on both spans of a deferred event, always with the value {@link
     * #PROPAGATION_LINK}; absent on events that keep the default parent-child continuity. The same
     * string is the key of the marker the engine persists in the event row's {@code trace_context}
     * carrier to transport that decision — the engine strips it before the map reaches an adapter
     * or a handler.
     */
    public static final String PROPAGATION = "event_outboxer.propagation";

    /**
     * Value of {@link #PROPAGATION} (span attribute and stored marker alike) for a deferred event
     * whose consumer span links to the producer span instead of descending from it.
     */
    public static final String PROPAGATION_LINK = "link";

    private OutboxTraceAttributes() {}
}
