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
 * handlerMaxRuntime}. The event itself is returned to PENDING with {@code attempts + 1}.
 *
 * <p>The handler thread is asked to stop via an interrupt (unless {@code interruptStuckHandler} is
 * disabled for the type), but nothing can force it: a handler blocked on a socket without a read
 * timeout keeps holding its slot of the type's handler pool. Whether it actually yielded is
 * reported separately by {@link OutboxListener#onHandlerAbandoned(HandlerAbandonedInfo)}.
 *
 * @param eventId id of the reclaimed event
 * @param eventType event type string
 * @param elapsed how long the handler had been running
 * @param workerId worker that owned the stuck claim
 * @param interrupted whether an interrupt was delivered to the dispatching thread
 */
public record StuckHandlerReclaimedInfo(
        UUID eventId, String eventType, Duration elapsed, WorkerId workerId, boolean interrupted) {

    public StuckHandlerReclaimedInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(elapsed, "elapsed must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
    }
}
