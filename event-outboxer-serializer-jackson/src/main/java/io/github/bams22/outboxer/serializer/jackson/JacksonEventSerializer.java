/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.serializer.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;
import io.github.bams22.outboxer.spi.EventSerializer;
import java.util.Objects;

/**
 * Jackson-backed {@link EventSerializer}. The adapter holds an {@link ObjectMapper} supplied by the
 * caller — it never builds one internally so that callers in Spring setups can share Boot's primary
 * {@code ObjectMapper} and customizations applied there.
 *
 * <p>Payloads travel in the text lane of {@link SerializedPayload}: valid JSON that storage
 * adapters may persist in a structured column such as PostgreSQL {@code JSONB} (ADR-0025).
 *
 * <p>See {@link JacksonObjectMapperFactory} for the factory the starter falls back to when neither
 * a qualified nor a primary {@code ObjectMapper} bean is available.
 */
public final class JacksonEventSerializer implements EventSerializer {

  /**
   * Stable format id persisted with every event written by this serializer (ADR-0025). Never
   * renamed — stored events reference it forever.
   */
  public static final String FORMAT = "jackson-json";

  private final ObjectMapper mapper;

  public JacksonEventSerializer(ObjectMapper mapper) {
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
  }

  @Override
  public String format() {
    return FORMAT;
  }

  @Override
  public SerializedPayload serialize(Object payload) {
    Objects.requireNonNull(payload, "payload must not be null");
    try {
      return SerializedPayload.ofText(mapper.writeValueAsString(payload));
    } catch (JsonProcessingException ex) {
      throw new PublishSerializationException(
          "failed to serialize payload of type " + payload.getClass().getName(), ex);
    }
  }

  @Override
  public <T> T deserialize(SerializedPayload payload, Class<T> type) {
    Objects.requireNonNull(payload, "payload must not be null");
    Objects.requireNonNull(type, "type must not be null");
    try {
      return mapper.readValue(payload.requireText(), type);
    } catch (JsonProcessingException ex) {
      throw new PayloadDeserializationException(
          "failed to deserialize payload as " + type.getName(), ex);
    }
  }

  /** The underlying {@link ObjectMapper} — exposed for diagnostics. */
  public ObjectMapper objectMapper() {
    return mapper;
  }
}
