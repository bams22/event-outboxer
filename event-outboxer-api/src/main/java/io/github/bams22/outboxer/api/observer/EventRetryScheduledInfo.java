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

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Payload of {@link OutboxListener#onEventRetryScheduled(EventRetryScheduledInfo)} — fired when the
 * failure-handler chain decided to retry the event, and the store acknowledged the re-schedule.
 *
 * @param eventId identifier of the event
 * @param eventType event type string
 * @param attempts attempt counter after this failure (i.e. includes the failed attempt)
 * @param nextRunAt wall-clock time of the next scheduled attempt
 * @param trigger bounded classification of what caused the re-schedule — safe as a metric tag,
 *     unlike the free-form {@code reason}
 * @param reason human-readable reason written to {@code last_fail_reason}
 * @param cause the exception that triggered the retry, or null when retry was explicit
 */
public record EventRetryScheduledInfo(
        UUID eventId,
        String eventType,
        int attempts,
        Instant nextRunAt,
        Trigger trigger,
        String reason,
        @Nullable Throwable cause) {

    public EventRetryScheduledInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(nextRunAt, "nextRunAt must not be null");
        Objects.requireNonNull(trigger, "trigger must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
    }

    /**
     * What caused the retry to be scheduled. A closed set by design: the free-form {@code reason}
     * string may embed exception messages and must never become a metric tag, while these values
     * are safe for tag cardinality.
     */
    public enum Trigger {
        /**
         * The failure-handler chain returned a {@code RetryAt} decision after a handler failure.
         */
        FAILURE_DECISION,
        /** The entity lock for the event's lock key was busy; the event was released for later. */
        LOCK_BUSY,
        /** No handler is registered for the event type and the policy is {@code SKIP}. */
        UNKNOWN_HANDLER,
        /** The per-type handler executor rejected the dispatch (pool and queue saturated). */
        DISPATCH_REJECTED
    }
}
