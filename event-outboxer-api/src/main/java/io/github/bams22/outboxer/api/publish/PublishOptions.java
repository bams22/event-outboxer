/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.publish;

import io.github.bams22.outboxer.domain.EventType;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Optional per-call tuning for {@link OutboxEventPublisher#publish(EventType, Object,
 * PublishOptions)}. All fields are nullable; {@code null} means "use the engine default".
 *
 * <p>There is no {@code lockKey} field here on purpose: lock keys are derived from the payload at
 * handle time by {@code EventHandler.extractLockKey(payload)} and are not stored alongside the
 * event (see ADR-0012).
 *
 * @param runAt earliest time the event may be claimed; defaults to {@code now}
 * @param priority explicit priority; defaults to 0
 * @param traceContext W3C traceparent/baggage to attach; normally the publisher captures this from
 *     the current MDC/Observation context, but callers may override it
 * @param dedupKey coalescing key (ADR-0021): at most one PENDING event per {@code (eventType,
 *     dedupKey)} exists at a time. A publish that finds a PENDING event with the same key returns
 *     that event's id instead of inserting — with the guarantee that the coalesced-into event will
 *     only be handled AFTER the current transaction commits, so the handler always sees this
 *     transaction's changes. Events already PROCESSING do not coalesce (a new event is inserted and
 *     runs afterwards with fresh data); DISABLED events do not block the key. This is work
 *     coalescing ("single in-flight per key"), NOT exactly-once: once the event is processed the
 *     key is free again, and handler idempotency remains required (ADR-0015). Max 256 characters
 */
@Builder
public record PublishOptions(
        @Nullable Instant runAt,
        @Nullable Short priority,
        @Nullable Map<String, String> traceContext,
        @Nullable String dedupKey) {

    public PublishOptions {
        traceContext =
                traceContext == null ? null : Collections.unmodifiableMap(Map.copyOf(traceContext));
        if (dedupKey != null) {
            if (dedupKey.isBlank()) {
                throw new IllegalArgumentException("dedupKey must not be blank when set");
            }
            if (dedupKey.length() > 256) {
                throw new IllegalArgumentException(
                        "dedupKey must be at most 256 characters, got " + dedupKey.length());
            }
        }
    }

    /** Canonical empty instance — all defaults. */
    public static PublishOptions defaults() {
        return new PublishOptions(null, null, null, null);
    }
}
