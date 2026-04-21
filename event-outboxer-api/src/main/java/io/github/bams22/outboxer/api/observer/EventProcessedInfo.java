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
import java.util.UUID;

/**
 * Payload of {@link OutboxListener#onEventProcessed(EventProcessedInfo)} — fired when a handler
 * returned {@code EventOutcome.Success} and the storage acknowledged the finalization.
 *
 * @param eventId identifier of the processed event
 * @param eventType event type string
 * @param attempts attempt number on which it succeeded (1 means first try)
 * @param duration time spent inside {@code handler.handle(...)}
 */
public record EventProcessedInfo(UUID eventId, String eventType, int attempts, Duration duration) {

  public EventProcessedInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(duration, "duration must not be null");
  }
}
