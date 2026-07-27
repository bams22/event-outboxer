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
 * Payload of {@link OutboxListener#onEventSerializationError(SerializationErrorInfo)} — fired when
 * the engine cannot deserialize a claimed event into the handler's payload type.
 *
 * @param eventId identifier of the event
 * @param eventType event type string
 * @param payloadClass fully-qualified class name the engine tried to deserialize into
 * @param cause underlying exception from the serializer
 */
public record SerializationErrorInfo(
    UUID eventId, String eventType, String payloadClass, Throwable cause) {

  public SerializationErrorInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(payloadClass, "payloadClass must not be null");
    Objects.requireNonNull(cause, "cause must not be null");
  }
}
