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

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A keyed event the claim statement swept as a duplicate of the {@link ClaimedEvent} it is attached
 * to (ADR-0037): it never reaches a handler, the representative's run covers it. Only the id and
 * the trace context survive — enough for {@code OutboxListener.onEventCoalesced} and for the
 * consumer span to link back to the publish that was collapsed.
 *
 * @param id the swept event's id
 * @param traceContext the swept event's stored trace context; never null (empty map allowed)
 */
public record CoalescedEvent(UUID id, Map<String, String> traceContext) {

    public CoalescedEvent {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(traceContext, "traceContext must not be null");
        traceContext = Map.copyOf(traceContext);
    }
}
