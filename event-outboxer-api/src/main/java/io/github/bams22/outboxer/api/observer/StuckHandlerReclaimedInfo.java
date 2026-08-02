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

import io.github.bams22.outboxer.domain.WorkerId;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Payload of {@link OutboxListener#onStuckHandlerReclaimed(StuckHandlerReclaimedInfo)} — fired when
 * the watchdog forcibly reclaimed an event whose handler had been running longer than {@code
 * handlerMaxRuntime}. The physical worker thread is unavoidably leaked in the JVM (see ADR-0005);
 * the event itself is returned to PENDING with {@code attempts + 1}.
 *
 * @param eventId id of the reclaimed event
 * @param eventType event type string
 * @param elapsed how long the handler had been running
 * @param workerId worker that owned the stuck claim
 */
public record StuckHandlerReclaimedInfo(
        UUID eventId, String eventType, Duration elapsed, WorkerId workerId) {

    public StuckHandlerReclaimedInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(elapsed, "elapsed must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
    }
}
