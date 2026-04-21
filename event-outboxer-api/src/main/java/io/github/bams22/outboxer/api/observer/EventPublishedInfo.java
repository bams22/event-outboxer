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

/**
 * Payload of {@link OutboxListener#onEventPublished(EventPublishedInfo)} — fired after the
 * publisher inserts a {@code PendingEvent} into the outbox. Emitted <em>before</em> the caller's
 * transaction commits, so a listener cannot assume the event will actually be visible (if the
 * transaction rolls back, the event is never persisted).
 *
 * @param eventId identifier of the just-published event
 * @param eventType event type string
 * @param createdAt time the event was published
 * @param runAt earliest time the event will be eligible for claim
 * @param priority priority value
 */
public record EventPublishedInfo(
    UUID eventId, String eventType, Instant createdAt, Instant runAt, short priority) {

  public EventPublishedInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(createdAt, "createdAt must not be null");
    Objects.requireNonNull(runAt, "runAt must not be null");
  }
}
