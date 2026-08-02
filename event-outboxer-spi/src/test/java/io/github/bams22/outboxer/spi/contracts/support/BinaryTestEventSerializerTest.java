/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi.contracts.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class BinaryTestEventSerializerTest {

    private final BinaryTestEventSerializer serializer = new BinaryTestEventSerializer();

    @Test
    void roundTripsThroughTheBinaryLane() {
        BinaryTestPayload dto = new BinaryTestPayload("héllo-байт", 42);

        SerializedPayload serialized = serializer.serialize(dto);

        assertThat(serialized.isText()).isFalse();
        byte[] bytes = serialized.requireBytes();
        assertThat(bytes[0]).isEqualTo((byte) 0x00);
        assertThat(bytes[1]).isEqualTo((byte) 0xFF);
        assertThat(new String(bytes, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8))
                .as("payload must not survive a UTF-8 decode/encode round-trip")
                .isNotEqualTo(bytes);
        assertThat(serializer.deserialize(serialized, BinaryTestPayload.class)).isEqualTo(dto);
    }

    @Test
    void rejectsCorruptedBytes() {
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        SerializedPayload.ofBytes(new byte[] {1, 2, 3}),
                                        BinaryTestPayload.class))
                .isInstanceOf(PayloadDeserializationException.class);
    }
}
