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
 * Payload of {@link OutboxListener#onEventSkipped(EventSkippedInfo)} — fired when a handler
 * returned {@code EventOutcome.Skip}, signalling that the business effect was already applied
 * (typical idempotency check).
 *
 * @param eventId identifier of the skipped event
 * @param eventType event type string
 * @param reason human-readable reason supplied by the handler
 */
public record EventSkippedInfo(UUID eventId, String eventType, String reason) {

  public EventSkippedInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
  }
}
