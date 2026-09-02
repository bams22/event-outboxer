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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.serializer.jackson.JacksonObjectMapperFactory;
import org.junit.jupiter.api.Test;

class JacksonStreamPayloadEncoderTest {

    private final JacksonStreamPayloadEncoder encoder =
            new JacksonStreamPayloadEncoder(JacksonObjectMapperFactory.defaults());

    private record Payload(String orderId, int quantity) {}

    @Test
    void encodesDtoToJsonTextLane() {
        SerializedPayload wire = encoder.encode(new Payload("o-1", 3));

        assertThat(wire.isText()).isTrue();
        assertThat(wire.requireText()).contains("\"orderId\":\"o-1\"").contains("\"quantity\":3");
    }

    @Test
    void reportsJsonContentType() {
        assertThat(encoder.contentType()).isEqualTo("application/json");
    }

    @Test
    void wrapsJacksonFailureInStreamEncodingException() {
        Object selfReferential =
                new Object() {
                    @SuppressWarnings("unused")
                    public Object getSelf() {
                        return this;
                    }
                };

        assertThatThrownBy(() -> encoder.encode(selfReferential))
                .isInstanceOf(StreamEncodingException.class)
                .hasMessageContaining("OUTBOX-105")
                .hasCauseInstanceOf(JsonProcessingException.class);
    }
}
