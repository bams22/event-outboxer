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
import org.jspecify.annotations.Nullable;

/**
 * Payload of {@link OutboxListener#onEventDisabled(EventDisabledInfo)} — fired when the failure
 * handler chain decided to disable the event (retry exhausted, explicit {@code Fail}, or
 * unrecoverable dispatch error). DISABLED events are retained in the outbox for investigation.
 *
 * @param eventId identifier of the disabled event
 * @param eventType event type string
 * @param attempts attempt counter after the final failure
 * @param trigger bounded classification of what caused the disable — safe as a metric tag, unlike
 *     the free-form {@code reason}
 * @param reason human-readable reason written to {@code last_fail_reason}
 * @param cause the exception that triggered the disable, or null when disable was explicit
 */
public record EventDisabledInfo(
        UUID eventId,
        String eventType,
        int attempts,
        Trigger trigger,
        String reason,
        @Nullable Throwable cause) {

    public EventDisabledInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(trigger, "trigger must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
    }

    /**
     * What caused the event to be disabled. A closed set by design: the free-form {@code reason}
     * string may embed exception messages and must never become a metric tag, while these values
     * are safe for tag cardinality.
     */
    public enum Trigger {
        /**
         * The failure-handler chain returned a {@code Disable} decision (e.g. retries exhausted).
         */
        FAILURE_DECISION,
        /** A failure handler itself threw; the engine fell back to disabling the event. */
        FAILURE_HANDLER_ERROR,
        /** No handler is registered for the event type and the policy is {@code DISABLE}. */
        UNKNOWN_HANDLER
    }
}
