/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;

/**
 * Port that converts handler payloads to and from the string form persisted in the outbox.
 *
 * <p>The publisher calls {@link #serialize(Object)} before handing the event to {@code
 * EventStore.save(...)}; the dispatcher calls {@link #deserialize(String, Class)} before invoking
 * {@code EventHandler.handle(ctx, payload)} on the claimed event. The engine itself never inspects
 * the serialized form — it only copies it verbatim through the storage layer (see ADR-0011).
 *
 * <p>MVP ships one implementation: {@code JacksonEventSerializer} in {@code
 * event-outboxer-serializer-jackson}. Implementations must be thread-safe; the engine shares a
 * single instance across all event types.
 */
public interface EventSerializer {

  /**
   * Serialize a payload DTO to the string form stored in the outbox. Both the string itself and
   * the payload's fully-qualified class name (obtained via {@code payload.getClass().getName()})
   * are persisted so that {@link #deserialize(String, Class)} can later reconstruct the DTO with
   * strict type checking.
   *
   * @throws PublishSerializationException if the payload cannot be represented in the serializer's
   *     format
   */
  String serialize(Object payload);

  /**
   * Reconstruct a payload DTO from its string form.
   *
   * <p>The adapter must enforce strict type matching: the serialized document must correspond to
   * the provided {@code type}. For Jackson, this means honouring
   * {@code FAIL_ON_UNKNOWN_PROPERTIES} / {@code FAIL_ON_NULL_FOR_PRIMITIVES} so that silent schema
   * drift cannot corrupt handler inputs.
   *
   * @throws PayloadDeserializationException if the stored document cannot be parsed into {@code
   *     type}
   */
  <T> T deserialize(String payload, Class<T> type);
}
