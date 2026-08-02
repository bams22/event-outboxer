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

import io.github.bams22.outboxer.domain.WorkerId;
import java.util.Objects;

/**
 * Parameters passed to {@link EventStore#claim(ClaimRequest)} when a per-event-type poller tries to
 * grab a batch of events for processing.
 *
 * <p>The engine polls <strong>per event type</strong> (see ADR-0004), so each {@code ClaimRequest}
 * carries the {@code eventType} that should be targeted by the underlying SQL query. The {@code
 * workerId} identifies the JVM that would own any claimed rows; the adapter writes it into the
 * {@code claimed_by} column and uses it to scope finalize operations through optimistic concurrency
 * control (see ADR-0014).
 *
 * @param eventType non-blank event-type string bound to a registered handler
 * @param workerId identifier of the worker performing the claim
 * @param limit maximum number of events to return in this batch; must be positive
 */
public record ClaimRequest(String eventType, WorkerId workerId, int limit) {

    public ClaimRequest {
        Objects.requireNonNull(eventType, "eventType must not be null");
        if (eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }
        Objects.requireNonNull(workerId, "workerId must not be null");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
    }
}
