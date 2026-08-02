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
 * Payload of {@link OutboxListener#onEventDeleted(EventDeletedInfo)} — fired when the failure
 * handler chain decided to delete the event outright (rare; used by workloads that consider stored
 * failures worthless).
 *
 * @param eventId identifier of the deleted event
 * @param eventType event type string
 * @param attempts attempt counter at the time of deletion
 * @param reason human-readable reason written to logs/metrics
 */
public record EventDeletedInfo(UUID eventId, String eventType, int attempts, String reason) {

    public EventDeletedInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
    }
}
