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
 * Payload of {@link OutboxListener#onHandlerAbandoned(HandlerAbandonedInfo)} — fired once per
 * dispatch that was still running {@code abandonedHandlerGrace} after the watchdog force-reclaimed
 * its row.
 *
 * <p>This is the honest counterpart of {@link StuckHandlerReclaimedInfo}: the event is safely back
 * in PENDING, but this JVM has lost a thread of the type's handler pool until the handler returns
 * on its own. Repeated firings for one event type mean that type will eventually stop processing
 * altogether — every pool slot ends up held by a dispatch nobody can stop.
 *
 * <p>{@link #interrupted()} says which of the two shapes it is. {@code true}: the handler ignored
 * an interrupt, the usual cause being blocking I/O with no timeout configured on the client — the
 * fix belongs in that client. {@code false}: the type set {@code interruptStuckHandler=false}, so
 * the thread was never asked to stop; the handler is simply slower than {@code handlerMaxRuntime},
 * which is the trade-off that setting buys.
 *
 * @param eventId id of the event whose dispatch is still running
 * @param eventType event type string
 * @param workerId worker whose pool slot is held
 * @param threadName name of the thread that did not yield
 * @param elapsed total time since the dispatch started
 * @param interrupted whether an interrupt had been delivered to that thread — {@code false} means
 *     the type opted out of interrupting stuck handlers, not that the interrupt failed
 */
public record HandlerAbandonedInfo(
        UUID eventId,
        String eventType,
        WorkerId workerId,
        String threadName,
        Duration elapsed,
        boolean interrupted) {

    public HandlerAbandonedInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(threadName, "threadName must not be null");
        Objects.requireNonNull(elapsed, "elapsed must not be null");
    }
}
