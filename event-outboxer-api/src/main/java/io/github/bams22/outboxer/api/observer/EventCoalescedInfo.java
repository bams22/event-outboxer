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
 * Payload of {@link OutboxListener#onEventCoalesced(EventCoalescedInfo)} — fired by the dispatcher
 * for every keyed event the claim statement swept as a duplicate of another due event with the same
 * {@code (type, dedupKey)} (ADR-0037). The swept event never reaches a handler; the
 * representative's run covers it, and it started after the swept publish had committed. The
 * callback fires after the claim and before the representative's handler, once per swept event.
 *
 * <p>{@code dedupKey} is caller-supplied free-form text — use it for logging and correlation, never
 * as a metric tag (unbounded cardinality). On the tracing side the same fact is recorded on the
 * representative's consumer span as {@code event_outboxer.coalesced_count} plus one span link per
 * swept event; this callback complements it with an aggregate, per-type signal.
 *
 * @param eventId identifier of the swept event — the publish that was collapsed
 * @param coalescedIntoEventId identifier of the representative whose run covers it
 * @param eventType event type string
 * @param dedupKey the dedup key that matched
 */
public record EventCoalescedInfo(
        UUID eventId, UUID coalescedIntoEventId, String eventType, String dedupKey) {

    public EventCoalescedInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(coalescedIntoEventId, "coalescedIntoEventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(dedupKey, "dedupKey must not be null");
    }
}
