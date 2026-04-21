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
 * @param reason human-readable reason written to {@code last_fail_reason}
 * @param cause the exception that triggered the disable, or null when disable was explicit
 */
public record EventDisabledInfo(
    UUID eventId, String eventType, int attempts, String reason, @Nullable Throwable cause) {

  public EventDisabledInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
  }
}
