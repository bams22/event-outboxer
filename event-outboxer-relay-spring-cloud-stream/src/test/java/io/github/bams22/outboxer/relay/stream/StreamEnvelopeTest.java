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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StreamEnvelopeTest {

    private static StreamEnvelope.StreamEnvelopeBuilder valid() {
        return StreamEnvelope.builder()
                .binding("orders-out")
                .key("k1")
                .headers(Map.of("x-app", "1"))
                .contentType("application/json")
                .textPayload("{\"a\":1}");
    }

    @Test
    void buildsWithTextLane() {
        StreamEnvelope envelope = valid().build();

        assertThat(envelope.binding()).isEqualTo("orders-out");
        assertThat(envelope.key()).isEqualTo("k1");
        assertThat(envelope.headers()).containsExactlyEntriesOf(Map.of("x-app", "1"));
        assertThat(envelope.textPayload()).isEqualTo("{\"a\":1}");
        assertThat(envelope.binaryPayload()).isNull();
    }

    @Test
    void buildsWithBinaryLane() {
        byte[] bytes = "raw".getBytes(StandardCharsets.UTF_8);
        StreamEnvelope envelope = valid().textPayload(null).binaryPayload(bytes).build();

        assertThat(envelope.textPayload()).isNull();
        assertThat(envelope.binaryPayload()).isEqualTo(bytes);
    }

    @Test
    void rejectsBothLanesSet() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> valid().binaryPayload(new byte[] {1}).build())
                .withMessageContaining("exactly one of textPayload/binaryPayload");
    }

    @Test
    void rejectsNoLaneSet() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> valid().textPayload(null).build())
                .withMessageContaining("exactly one of textPayload/binaryPayload");
    }

    @Test
    void rejectsNullOrBlankBinding() {
        assertThatNullPointerException()
                .isThrownBy(() -> valid().binding(null).build())
                .withMessageContaining("binding");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> valid().binding("  ").build())
                .withMessageContaining("binding must not be blank");
    }

    @Test
    void rejectsNullOrBlankContentType() {
        assertThatNullPointerException()
                .isThrownBy(() -> valid().contentType(null).build())
                .withMessageContaining("contentType");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> valid().contentType("").build())
                .withMessageContaining("contentType must not be blank");
    }

    @Test
    void normalizesNullHeadersToEmptyImmutableMap() {
        StreamEnvelope envelope = valid().headers(null).build();

        assertThat(envelope.headers()).isEmpty();
        assertThatThrownBy(() -> envelope.headers().put("k", "v"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void copiesHeadersDefensively() {
        Map<String, String> mutable = new HashMap<>(Map.of("a", "1"));
        StreamEnvelope envelope = valid().headers(mutable).build();

        mutable.put("b", "2");

        assertThat(envelope.headers()).containsOnlyKeys("a");
    }

    @Test
    void binaryLaneEqualityComparesContents() {
        StreamEnvelope left = valid().textPayload(null).binaryPayload(new byte[] {1, 2}).build();
        StreamEnvelope right = valid().textPayload(null).binaryPayload(new byte[] {1, 2}).build();

        assertThat(left).isEqualTo(right).hasSameHashCodeAs(right);
    }

    @Test
    void toStringNeverDumpsPayloadOrHeaderContent() {
        StreamEnvelope envelope = valid().headers(Map.of("secret", "value")).build();

        assertThat(envelope.toString())
                .doesNotContain("{\"a\":1}")
                .doesNotContain("secret")
                .contains("orders-out");
    }

    @Test
    void eventTypeConstantIsTheReservedName() {
        assertThat(StreamEnvelope.EVENT_TYPE.name()).isEqualTo("outboxer-stream-relay");
        assertThat(StreamEnvelope.EVENT_TYPE.payloadType()).isEqualTo(StreamEnvelope.class);
    }
}
