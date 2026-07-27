/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.handle;

import io.github.bams22.outboxer.domain.WorkerId;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable context passed into {@code EventHandler.handle(ctx, payload)}. Carries metadata about
 * the event being processed — useful for logging, idempotency, and distributed tracing — but never
 * the payload itself (the payload arrives as the second argument to {@code handle}).
 *
 * @param eventId unique identifier of the event
 * @param eventType event type string that selected this handler
 * @param attempt 1-based attempt counter, including the current invocation
 * @param createdAt time the event was originally published
 * @param claimedAt time the worker claimed it for this attempt
 * @param workerId id of the worker JVM running the handler
 * @param traceContext restored W3C trace/baggage context (never null, empty map allowed)
 */
public record EventContext(
    UUID eventId,
    String eventType,
    int attempt,
    Instant createdAt,
    Instant claimedAt,
    WorkerId workerId,
    Map<String, String> traceContext) {

  public EventContext {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(createdAt, "createdAt must not be null");
    Objects.requireNonNull(claimedAt, "claimedAt must not be null");
    Objects.requireNonNull(workerId, "workerId must not be null");
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be >= 1, got " + attempt);
    }
    traceContext =
        traceContext == null ? Map.of() : Collections.unmodifiableMap(Map.copyOf(traceContext));
  }
}
