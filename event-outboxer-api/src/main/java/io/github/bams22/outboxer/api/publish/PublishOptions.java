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
 * @param dedupKey coalescing key (ADR-0037): duplicates of a {@code (eventType, dedupKey)} are
 *     collapsed when the type is claimed, not when they are published. Every keyed publish inserts
 *     its own row and returns its own id. The next claim of the type keeps one representative among
 *     the due {@code PENDING} rows of the key (highest priority, then oldest {@code runAt}) and
 *     sweeps the others; a swept row never reaches a handler, and {@code
 *     OutboxListener.onEventCoalesced} reports it together with the representative's id. The
 *     representative's handler starts only after every swept publish has committed, so it sees
 *     those transactions' changes. Only due {@code PENDING} rows take part: a publish while the
 *     key's event is {@code PROCESSING} yields a fresh row that runs afterwards with fresh data, a
 *     row with a future {@code runAt} is a separate intent (publish a few seconds ahead to fold a
 *     burst into one deferred run), and {@code DISABLED} events do not block the key. This is
 *     best-effort work coalescing, NOT exactly-once: duplicates claimed concurrently by different
 *     workers run separately, and handler idempotency remains required (ADR-0015). Max 256
 *     characters
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
