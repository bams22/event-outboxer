/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.relay.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bams22.outboxer.domain.SerializedPayload;
import java.util.Objects;

/**
 * Default {@link StreamPayloadEncoder}: JSON text via a caller-supplied {@link ObjectMapper}. Never
 * builds a mapper internally — the Spring auto-configuration resolves one through the same chain as
 * the Jackson event serializer ({@code outboxObjectMapper} bean, then the primary mapper, then
 * {@code JacksonObjectMapperFactory.defaults()}), and plain-Java setups inject their own.
 */
public final class JacksonStreamPayloadEncoder implements StreamPayloadEncoder {

    private final ObjectMapper objectMapper;

    public JacksonStreamPayloadEncoder(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    @Override
    public SerializedPayload encode(Object payload) {
        try {
            return SerializedPayload.ofText(objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new StreamEncodingException(
                    "failed to encode payload of type " + payload.getClass().getName(), e);
        }
    }
}
