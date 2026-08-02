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

/**
 * An event that has just been claimed by a worker. Returned from {@code
 * EventStore.claim(ClaimRequest)} and consumed by the core engine's dispatcher.
 *
 * <p>{@link #claimedVersion} is the {@code version} value recorded on the row <em>at the moment of
 * claim</em>. It must be echoed back to finalize methods ({@code markProcessed}, {@code
 * markForRetry}, {@code markDisabled}, {@code forceReclaim}) so that the storage can apply
 * optimistic concurrency control (see ADR-0014). If another actor — orphan recovery, a watchdog, or
 * a force reclaim — bumped the row's version in the meantime, the finalize operation returns {@code
 * false} and the engine treats it as an expected at-least-once race (see ADR-0015).
 *
 * @param id event identifier
 * @param eventType event type string
 * @param payload serialized payload body (same form as {@code PendingEvent.payload})
 * @param payloadFormat stable id of the serializer that produced {@code payload}; the dispatcher
 *     selects the deserializer by this value (ADR-0025)
 * @param payloadClass publish-time payload class FQCN, recorded for diagnostics and auditing; never
 *     used to select the deserialization target — that is always {@code EventHandler.payloadType()}
 * @param priority priority value as stored
 * @param attempts number of processing attempts, including the current one
 * @param createdAt time the event was originally published
 * @param claimedAt time this claim was acquired (used by the watchdog)
 * @param traceContext restored W3C trace/baggage context; never null (empty map allowed)
 * @param claimedVersion version value recorded on the row at the moment of claim; used for
 *     optimistic concurrency control when finalizing
 */
public record ClaimedEvent(
    UUID id,
    String eventType,
    SerializedPayload payload,
    String payloadFormat,
    String payloadClass,
    short priority,
    int attempts,
    Instant createdAt,
    Instant claimedAt,
    Map<String, String> traceContext,
    long claimedVersion) {

  public ClaimedEvent {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(payload, "payload must not be null");
    Objects.requireNonNull(payloadFormat, "payloadFormat must not be null");
    Objects.requireNonNull(payloadClass, "payloadClass must not be null");
    Objects.requireNonNull(createdAt, "createdAt must not be null");
    Objects.requireNonNull(claimedAt, "claimedAt must not be null");
    if (attempts < 0) {
      throw new IllegalArgumentException("attempts must not be negative, got " + attempts);
    }
    if (claimedVersion < 0) {
      throw new IllegalArgumentException(
          "claimedVersion must not be negative, got " + claimedVersion);
    }
    Objects.requireNonNull(traceContext, "traceContext must not be null");
    traceContext = Map.copyOf(traceContext);
  }
}
