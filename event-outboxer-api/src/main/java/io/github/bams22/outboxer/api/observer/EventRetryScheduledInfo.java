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
 * @param reason human-readable reason written to {@code last_fail_reason}
 * @param cause the exception that triggered the retry, or null when retry was explicit
 */
public record EventRetryScheduledInfo(
    UUID eventId,
    String eventType,
    int attempts,
    Instant nextRunAt,
    String reason,
    @Nullable Throwable cause) {

  public EventRetryScheduledInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(nextRunAt, "nextRunAt must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
  }
}
