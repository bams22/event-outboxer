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
 * Payload of {@link OutboxListener#onUnknownEventType(UnknownEventTypeInfo)} — fired when a
 * claimed event has an {@code eventType} without a registered handler. The configured
 * {@code UnknownHandlerPolicy} decides what actually happens to the event (skip, disable, or
 * fail).
 *
 * @param eventId identifier of the event
 * @param eventType the unknown event type
 */
public record UnknownEventTypeInfo(UUID eventId, String eventType) {

  public UnknownEventTypeInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
  }
}
