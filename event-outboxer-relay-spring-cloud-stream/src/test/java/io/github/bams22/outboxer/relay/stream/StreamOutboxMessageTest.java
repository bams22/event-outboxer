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

import io.github.bams22.outboxer.api.publish.PublishOptions;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StreamOutboxMessageTest {

    private record Payload(String value) {}

    @Test
    void ofBuildsMinimalMessage() {
        StreamOutboxMessage message = StreamOutboxMessage.of("orders-out", "k1", new Payload("v"));

        assertThat(message.binding()).isEqualTo("orders-out");
        assertThat(message.key()).isEqualTo("k1");
        assertThat(message.headers()).isEmpty();
        assertThat(message.contentType()).isNull();
        assertThat(message.options()).isNull();
    }

    @Test
    void rejectsBlankBindingAndNullPayload() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> StreamOutboxMessage.of(" ", null, new Payload("v")))
                .withMessageContaining("binding must not be blank");
        assertThatNullPointerException()
                .isThrownBy(() -> StreamOutboxMessage.of("b", null, null))
                .withMessageContaining("payload");
    }

    @Test
    void rejectsBlankContentTypeWhenSet() {
        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                StreamOutboxMessage.builder()
                                        .binding("b")
                                        .payload(new Payload("v"))
                                        .contentType(" ")
                                        .build())
                .withMessageContaining("contentType must not be blank when set");
    }

    @Test
    void copiesHeadersDefensively() {
        Map<String, String> mutable = new HashMap<>(Map.of("a", "1"));
        StreamOutboxMessage message =
                StreamOutboxMessage.builder()
                        .binding("b")
                        .payload(new Payload("v"))
                        .headers(mutable)
                        .build();

        mutable.put("b", "2");

        assertThat(message.headers()).containsOnlyKeys("a");
    }

    @Test
    void carriesPublishOptions() {
        PublishOptions options = PublishOptions.builder().dedupKey("dk").build();
        StreamOutboxMessage message =
                StreamOutboxMessage.builder()
                        .binding("b")
                        .payload(new Payload("v"))
                        .options(options)
                        .build();

        assertThat(message.options()).isSameAs(options);
    }
}
