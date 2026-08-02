/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SerializedPayloadTest {

    @Test
    void textLaneRoundTrip() {
        SerializedPayload p = SerializedPayload.ofText("{\"x\":1}");
        assertThat(p.isText()).isTrue();
        assertThat(p.requireText()).isEqualTo("{\"x\":1}");
        assertThat(p.bytes()).isNull();
        assertThatThrownBy(p::requireBytes).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void bytesLaneRoundTrip() {
        byte[] raw = {0x00, (byte) 0xFF, 0x42};
        SerializedPayload p = SerializedPayload.ofBytes(raw);
        assertThat(p.isText()).isFalse();
        assertThat(p.requireBytes()).containsExactly(0x00, 0xFF, 0x42);
        assertThat(p.text()).isNull();
        assertThatThrownBy(p::requireText).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsBothNullAndBothSet() {
        assertThatThrownBy(() -> new SerializedPayload(null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SerializedPayload("{}", new byte[] {1}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bytesEqualityIsContentBased() {
        SerializedPayload a = SerializedPayload.ofBytes(new byte[] {1, 2, 3});
        SerializedPayload b = SerializedPayload.ofBytes(new byte[] {1, 2, 3});
        SerializedPayload c = SerializedPayload.ofBytes(new byte[] {1, 2, 4});
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(c);
        assertThat(SerializedPayload.ofText("{}")).isEqualTo(SerializedPayload.ofText("{}"));
        assertThat(SerializedPayload.ofText("{}")).isNotEqualTo(a);
    }

    @Test
    void toStringNeverDumpsContent() {
        assertThat(SerializedPayload.ofText("secret").toString()).doesNotContain("secret");
        assertThat(SerializedPayload.ofBytes("secret".getBytes()).toString())
                .doesNotContain("secret");
    }
}
