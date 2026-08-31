/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.observer;

import java.util.Objects;
import java.util.UUID;

/**
 * Payload of {@link OutboxListener#onEventCoalesced(EventCoalescedInfo)} — fired when a publish
 * carrying a dedup key coalesced into an existing {@code PENDING} event of the same {@code (type,
 * key)} instead of inserting a new row (ADR-0021). Fires instead of {@code onEventPublished}:
 * exactly one of the two callbacks is emitted per publish request. Note: the caller's transaction
 * may still roll back, in which case the coalescing never becomes visible to the engine.
 *
 * <p>{@code dedupKey} is caller-supplied free-form text — use it for logging and correlation, never
 * as a metric tag (unbounded cardinality). On the tracing side the same fact is recorded as the
 * {@code event_outboxer.coalesced_into} span attribute; this callback complements it with an
 * aggregate, per-type signal.
 *
 * @param existingEventId identifier of the already-pending event the publish coalesced into
 * @param eventType event type string
 * @param dedupKey the dedup key that matched
 */
public record EventCoalescedInfo(UUID existingEventId, String eventType, String dedupKey) {

    public EventCoalescedInfo {
        Objects.requireNonNull(existingEventId, "existingEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(dedupKey, "dedupKey must not be null");
    }
}
