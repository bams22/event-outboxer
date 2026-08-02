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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.InvariantViolationException;
import io.github.bams22.outboxer.domain.exception.UnknownPayloadFormatException;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventSerializerRegistryTest {

    private static EventSerializer serializer(String format) {
        return new EventSerializer() {
            @Override
            public String format() {
                return format;
            }

            @Override
            public SerializedPayload serialize(Object payload) {
                return SerializedPayload.ofText(String.valueOf(payload));
            }

            @Override
            public <T> T deserialize(SerializedPayload payload, Class<T> type) {
                return type.cast(payload.requireText());
            }
        };
    }

    @Test
    void findAndRequireRouteByFormat() {
        EventSerializer json = serializer("test-json");
        EventSerializer binary = serializer("test-binary");
        EventSerializerRegistry registry = EventSerializerRegistry.of(List.of(json, binary));

        assertThat(registry.formats()).containsExactly("test-json", "test-binary");
        assertThat(registry.find("test-json")).containsSame(json);
        assertThat(registry.find("nope")).isEmpty();
        assertThat(registry.require("test-binary")).isSameAs(binary);
    }

    @Test
    void requireUnknownFormatThrowsOutbox203() {
        EventSerializerRegistry registry = EventSerializerRegistry.of(List.of(serializer("a")));

        assertThatThrownBy(() -> registry.require("no-such-format"))
                .isInstanceOf(UnknownPayloadFormatException.class)
                .hasMessageContaining("OUTBOX-203")
                .hasMessageContaining("no-such-format")
                .hasMessageContaining("a");
    }

    @Test
    void rejectsDuplicateFormats() {
        assertThatThrownBy(
                        () ->
                                EventSerializerRegistry.of(
                                        List.of(serializer("dup"), serializer("dup"))))
                .isInstanceOf(InvariantViolationException.class)
                .hasMessageContaining("dup");
    }

    @Test
    void rejectsBlankAndOversizedFormats() {
        assertThatThrownBy(() -> EventSerializerRegistry.of(List.of(serializer(" "))))
                .isInstanceOf(InvariantViolationException.class);
        assertThatThrownBy(() -> EventSerializerRegistry.of(List.of(serializer("x".repeat(65)))))
                .isInstanceOf(InvariantViolationException.class);
    }

    @Test
    void rejectsEmptyRegistry() {
        assertThatThrownBy(() -> EventSerializerRegistry.of(List.of()))
                .isInstanceOf(InvariantViolationException.class);
    }
}
