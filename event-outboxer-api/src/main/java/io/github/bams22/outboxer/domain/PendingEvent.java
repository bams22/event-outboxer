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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.Builder;

/**
 * An event prepared for insertion into the outbox by {@code OutboxEventPublisher}. The publisher
 * builds a {@code PendingEvent} from the caller's payload (which the publisher serializes) and
 * passes it to {@code EventStore.save(...)}.
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>{@code payload} is already serialized to a string at this point — serialization happens in
 *       the publisher, not in the storage adapter (see ADR-0011).
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
 * @param payloadClass fully-qualified Java class name for strict deserialization on the worker
 * @param priority higher values are picked first; zero by default
 * @param runAt earliest wall-clock time the event is eligible for claim
 * @param traceContext optional W3C trace/baggage context; never null (empty map allowed)
 */
@Builder
public record PendingEvent(
    UUID id,
    String eventType,
    String payload,
    String payloadClass,
    short priority,
    Instant runAt,
    Map<String, String> traceContext) {

  public PendingEvent {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    if (eventType.isBlank()) {
      throw new IllegalArgumentException("eventType must not be blank");
    }
    Objects.requireNonNull(payload, "payload must not be null");
    Objects.requireNonNull(payloadClass, "payloadClass must not be null");
    Objects.requireNonNull(runAt, "runAt must not be null");
    traceContext =
        traceContext == null ? Map.of() : Collections.unmodifiableMap(Map.copyOf(traceContext));
  }
}
