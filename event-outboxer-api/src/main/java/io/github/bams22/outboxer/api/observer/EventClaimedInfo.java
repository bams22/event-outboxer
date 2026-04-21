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
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Payload of {@link OutboxListener#onEventClaimed(EventClaimedInfo)} — fired when the engine has
 * successfully claimed an event from the store and is about to dispatch it to a handler.
 *
 * @param eventId identifier of the claimed event
 * @param eventType event type string
 * @param attempts number of processing attempts so far, including the one starting now
 * @param claimedAt time the claim was acquired
 * @param workerId the worker that holds the claim
 */
public record EventClaimedInfo(
    UUID eventId, String eventType, int attempts, Instant claimedAt, WorkerId workerId) {

  public EventClaimedInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(claimedAt, "claimedAt must not be null");
    Objects.requireNonNull(workerId, "workerId must not be null");
  }
}
