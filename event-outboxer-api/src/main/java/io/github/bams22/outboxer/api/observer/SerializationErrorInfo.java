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
 * the engine cannot deserialize a claimed event into the handler's payload type, or when no
 * serializer is registered for the event's stored payload format (ADR-0025).
 *
 * <p>{@code storedPayloadClass} and {@code targetType} diverge exactly in the interesting case: a
 * DTO renamed or moved between publish and handle. The former is what the publisher recorded, the
 * latter is what the engine actually tried to deserialize into.
 *
 * @param eventId identifier of the event
 * @param eventType event type string
 * @param payloadFormat stable serializer id stored with the event (for example {@code
 *     "jackson-json"})
 * @param storedPayloadClass payload class FQCN recorded at publish time (diagnostics)
 * @param targetType fully-qualified name of {@code EventHandler.payloadType()} — the class the
 *     engine tried to deserialize into
 * @param cause underlying exception from the serializer or the registry lookup
 */
public record SerializationErrorInfo(
        UUID eventId,
        String eventType,
        String payloadFormat,
        String storedPayloadClass,
        String targetType,
        Throwable cause) {

    public SerializationErrorInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(payloadFormat, "payloadFormat must not be null");
        Objects.requireNonNull(storedPayloadClass, "storedPayloadClass must not be null");
        Objects.requireNonNull(targetType, "targetType must not be null");
        Objects.requireNonNull(cause, "cause must not be null");
    }
}
