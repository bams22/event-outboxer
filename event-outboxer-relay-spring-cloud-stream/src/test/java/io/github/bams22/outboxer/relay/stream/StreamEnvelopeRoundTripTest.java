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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bams22.outboxer.serializer.jackson.JacksonObjectMapperFactory;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Pins the envelope's persisted-schema contract (ADR-0032): stored relay events are JSON produced
 * by the engine's default mapper, and rows written by other module versions must keep
 * deserializing. If one of these tests breaks, the change is a schema break — do not adjust the
 * test, adjust the change.
 */
class StreamEnvelopeRoundTripTest {

    private final ObjectMapper mapper = JacksonObjectMapperFactory.defaults();

    @Test
    void textEnvelopeRoundTrips() throws Exception {
        StreamEnvelope original =
                StreamEnvelope.builder()
                        .binding("orders-out")
                        .key("k1")
                        .headers(Map.of("x-app", "1"))
                        .contentType("application/json")
                        .textPayload("{\"orderId\":\"o-1\"}")
                        .build();

        StreamEnvelope restored =
                mapper.readValue(mapper.writeValueAsString(original), StreamEnvelope.class);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    void binaryEnvelopeRoundTripsThroughBase64() throws Exception {
        StreamEnvelope original =
                StreamEnvelope.builder()
                        .binding("orders-out")
                        .headers(Map.of())
                        .contentType("application/octet-stream")
                        .binaryPayload(new byte[] {0, 1, 2, (byte) 0xFF})
                        .build();

        String json = mapper.writeValueAsString(original);
        StreamEnvelope restored = mapper.readValue(json, StreamEnvelope.class);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    void persistedFieldNamesAreStable() throws Exception {
        StreamEnvelope envelope =
                StreamEnvelope.builder()
                        .binding("orders-out")
                        .key("k1")
                        .contentType("application/json")
                        .textPayload("{}")
                        .build();

        String json = mapper.writeValueAsString(envelope);

        assertThat(json)
                .contains("\"binding\"")
                .contains("\"key\"")
                .contains("\"headers\"")
                .contains("\"contentType\"")
                .contains("\"textPayload\"")
                .contains("\"binaryPayload\"");
    }

    @Test
    void unknownFieldFromNewerWriterIsTolerated() throws Exception {
        String json =
                """
                {"binding":"orders-out","key":null,"headers":{},"contentType":"application/json",
                 "textPayload":"{}","binaryPayload":null,"fieldFromTheFuture":42}
                """;

        StreamEnvelope restored = mapper.readValue(json, StreamEnvelope.class);

        assertThat(restored.binding()).isEqualTo("orders-out");
        assertThat(restored.textPayload()).isEqualTo("{}");
    }

    @Test
    void missingOptionalFieldsDeserializeAsAbsent() throws Exception {
        String json =
                """
                {"binding":"orders-out","headers":{},"contentType":"application/json",
                 "textPayload":"{}"}
                """;

        StreamEnvelope restored = mapper.readValue(json, StreamEnvelope.class);

        assertThat(restored.key()).isNull();
        assertThat(restored.binaryPayload()).isNull();
        assertThat(restored.textPayload()).isEqualTo("{}");
    }

    @Test
    void binaryLaneIsByteExact() throws Exception {
        byte[] bytes = "жёсткие байты".getBytes(StandardCharsets.UTF_8);
        StreamEnvelope original =
                StreamEnvelope.builder()
                        .binding("b")
                        .contentType("application/octet-stream")
                        .binaryPayload(bytes)
                        .build();

        StreamEnvelope restored =
                mapper.readValue(mapper.writeValueAsString(original), StreamEnvelope.class);

        assertThat(restored.binaryPayload()).isEqualTo(bytes);
    }
}
