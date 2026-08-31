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

import java.time.Duration;
import java.util.Objects;

/**
 * Payload of {@link OutboxListener#onPollCompleted(PollCompletedInfo)} — fired after every claim
 * attempt of a per-type poller, including polls that returned no events. Storage errors during the
 * claim surface as {@code StorageErrorInfo} instead and do not fire this callback.
 *
 * @param eventType event type of the poller
 * @param requested the claim batch limit passed to the store
 * @param claimed number of events actually claimed; {@code 0} for an empty poll
 * @param duration wall time of the claim query itself, recorded for empty polls too
 */
public record PollCompletedInfo(String eventType, int requested, int claimed, Duration duration) {

    public PollCompletedInfo {
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        if (requested < 0) {
            throw new IllegalArgumentException("requested must not be negative");
        }
        if (claimed < 0) {
            throw new IllegalArgumentException("claimed must not be negative");
        }
    }
}
