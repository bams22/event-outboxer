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
 * An event prepared for insertion into the outbox by {@code OutboxEventPublisher}. The publisher
 * builds a {@code PendingEvent} from the caller's payload (which the publisher serializes) and
 * passes it to {@code EventStore.save(...)}.
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>{@code payload} is already serialized at this point — serialization happens in the
 *       publisher, not in the storage adapter (see ADR-0011, ADR-0025).
 *   <li>There is no {@code lockKey} field: lock keys are derived at handle time by {@code
 *       EventHandler.extractLockKey(payload)} and are not persisted (see ADR-0012).
 *   <li>The {@code traceContext} carries W3C {@code traceparent}, optional {@code tracestate}, and
 *       an optional baggage map — the storage adapter persists it as a JSONB column for continuity
 *       of distributed traces across publish → handle.
 * </ul>
 *
 * @param id stable event identifier (the publisher must generate it, not the storage)
 * @param eventType string that binds the event to a registered {@code EventHandler}
 * @param payload serialized payload body
 * @param payloadFormat stable id of the serializer that produced {@code payload} (for example
 *     {@code "jackson-json"}); the dispatcher selects the deserializer by this value (ADR-0025)
 * @param payloadClass publish-time payload class FQCN, recorded for diagnostics and auditing; never
 *     used to select the deserialization target — that is always {@code EventHandler.payloadType()}
 * @param priority higher values are picked first; zero by default
 * @param runAt earliest wall-clock time the event is eligible for claim
 * @param traceContext optional W3C trace/baggage context; never null (empty map allowed)
 * @param dedupKey optional coalescing key: at most one PENDING event per {@code (eventType,
 *     dedupKey)} at a time (ADR-0021); {@code null} = no coalescing
 */
@Builder
public record PendingEvent(
    UUID id,
    String eventType,
    SerializedPayload payload,
    String payloadFormat,
    String payloadClass,
    short priority,
    Instant runAt,
    Map<String, String> traceContext,
    @Nullable String dedupKey) {

  public PendingEvent {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    if (eventType.isBlank()) {
      throw new IllegalArgumentException("eventType must not be blank");
    }
    Objects.requireNonNull(payload, "payload must not be null");
    Objects.requireNonNull(payloadFormat, "payloadFormat must not be null");
    if (payloadFormat.isBlank() || payloadFormat.length() > 64) {
      throw new IllegalArgumentException(
          "payloadFormat must be non-blank and at most 64 characters");
    }
    Objects.requireNonNull(payloadClass, "payloadClass must not be null");
    Objects.requireNonNull(runAt, "runAt must not be null");
    Objects.requireNonNull(traceContext, "traceContext must not be null");
    traceContext = Map.copyOf(traceContext);
    if (dedupKey != null && (dedupKey.isBlank() || dedupKey.length() > 256)) {
      throw new IllegalArgumentException(
          "dedupKey must be non-blank and at most 256 characters when set");
    }
  }
}
