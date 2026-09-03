/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * A successfully processed event as stored in the opt-in archive table (ADR-0008). Deliberately a
 * separate type from {@link Event}: archived rows have no status, claim or version — modelling them
 * as an {@code Event} would require a synthetic {@code EventStatus} value the store never persists.
 *
 * @param id original event id
 * @param eventType event type
 * @param payload serialized payload as stored
 * @param payloadFormat stable id of the serializer that produced {@code payload} (ADR-0025)
 * @param payloadClass publish-time payload class FQCN, recorded for diagnostics and auditing; never
 *     used to select a deserialization target
 * @param priority publish-time priority
 * @param attempts attempts consumed before the successful run
 * @param createdAt original publish time
 * @param runAt effective run-at of the successful delivery
 * @param lastFailReason last failure reason prior to success, if any
 * @param traceContext propagated trace context
 * @param archivedAt time the row was moved to the archive
 * @param archivedBy worker that finalized the event
 * @param dedupKey coalescing key the event carried in the hot table (ADR-0021), copied for audit
 *     and replay (ADR-0033); the archive enforces no uniqueness on it. {@code null} for key-less
 *     events and for rows archived before migration V008
 */
@Builder
public record ArchivedEvent(
        UUID id,
        String eventType,
        SerializedPayload payload,
        String payloadFormat,
        String payloadClass,
        short priority,
        int attempts,
        Instant createdAt,
        Instant runAt,
        @Nullable String lastFailReason,
        Map<String, String> traceContext,
        Instant archivedAt,
        String archivedBy,
        @Nullable String dedupKey) {

    public ArchivedEvent {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(payloadFormat, "payloadFormat must not be null");
        Objects.requireNonNull(payloadClass, "payloadClass must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(runAt, "runAt must not be null");
        Objects.requireNonNull(traceContext, "traceContext must not be null");
        Objects.requireNonNull(archivedAt, "archivedAt must not be null");
        Objects.requireNonNull(archivedBy, "archivedBy must not be null");
        traceContext = Map.copyOf(traceContext);
    }
}
