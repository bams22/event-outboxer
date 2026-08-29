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

class EventTypeTest {

    @Test
    void ofCarriesNameAndPayloadClass() {
        EventType<String> type = EventType.of("ORDER", String.class);

        assertThat(type.name()).isEqualTo("ORDER");
        assertThat(type.payloadType()).isEqualTo(String.class);
        assertThat(type).hasToString("ORDER");
        assertThat(type).isEqualTo(new EventType<>("ORDER", String.class));
    }

    @Test
    void untypedAcceptsAnyPayload() {
        EventType<Object> type = EventType.untyped("DYNAMIC");

        assertThat(type.payloadType()).isEqualTo(Object.class);
        assertThat(type.payloadType().isInstance(42)).isTrue();
    }

    @Test
    void rejectsBlankOrOverlongNameAndNullClass() {
        assertThatThrownBy(() -> EventType.of("", String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
        assertThatThrownBy(() -> EventType.of("   ", String.class))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventType.of("x".repeat(129), String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("128");
        assertThatThrownBy(() -> EventType.of(null, String.class))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> EventType.of("ORDER", null))
                .isInstanceOf(NullPointerException.class);
        assertThat(EventType.of("x".repeat(128), String.class).name()).hasSize(128);
    }
}
