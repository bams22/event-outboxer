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
 * Full, storage-agnostic view of a single outbox event as returned by administrative lookup methods
 * such as {@code EventStore.findById(id)}.
 *
 * <p>This is the "read" projection of an event. In contrast, {@link PendingEvent} is what the
 * publisher writes, and {@link ClaimedEvent} is what the engine receives from a claim. Fields not
 * set in a given lifecycle phase are null — for example, {@code claimedBy} and {@code claimedAt}
 * are null while the event is {@link EventStatus#PENDING}; {@code lastFailReason} is null until the
 * first failure.
 *
 * @param id event identifier
 * @param eventType event type string
 * @param payload serialized payload body
 * @param payloadFormat stable id of the serializer that produced {@code payload} (ADR-0025)
 * @param payloadClass publish-time payload class FQCN, recorded for diagnostics and auditing; never
 *     used to select the deserialization target — that is always {@code EventHandler.payloadType()}
 * @param priority priority value
 * @param attempts cumulative number of processing attempts
 * @param status current lifecycle status
 * @param createdAt time the event was originally published
 * @param runAt earliest time the event is/was eligible for claim
 * @param claimedBy worker id that currently owns the row; null unless {@link
 *     EventStatus#PROCESSING}
 * @param claimedAt time the current claim was acquired; null unless {@link EventStatus#PROCESSING}
 * @param lastFailReason human-readable description of the most recent failure; nullable
 * @param traceContext W3C trace/baggage context; never null (empty map allowed)
 * @param version optimistic-concurrency version of the row at the time of read
 */
@Builder
public record Event(
        UUID id,
        String eventType,
        SerializedPayload payload,
        String payloadFormat,
        String payloadClass,
        short priority,
        int attempts,
        EventStatus status,
        Instant createdAt,
        Instant runAt,
        @Nullable WorkerId claimedBy,
        @Nullable Instant claimedAt,
        @Nullable String lastFailReason,
        Map<String, String> traceContext,
        long version,
        @Nullable String dedupKey) {

    public Event {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(payloadFormat, "payloadFormat must not be null");
        Objects.requireNonNull(payloadClass, "payloadClass must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(runAt, "runAt must not be null");
        if (attempts < 0) {
            throw new IllegalArgumentException("attempts must not be negative, got " + attempts);
        }
        if (version < 0) {
            throw new IllegalArgumentException("version must not be negative, got " + version);
        }
        Objects.requireNonNull(traceContext, "traceContext must not be null");
        traceContext = Map.copyOf(traceContext);
    }
}
