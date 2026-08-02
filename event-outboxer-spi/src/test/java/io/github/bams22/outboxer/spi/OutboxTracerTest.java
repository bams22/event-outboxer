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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.WorkerId;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class OutboxTracerTest {

    private static final WorkerId WORKER = new WorkerId("worker-1");

    @Nested
    class Noop {

        @Test
        void publishSpanIsFunctionalNoop() {
            OutboxTracer.PublishSpan span =
                    OutboxTracer.NOOP.startPublishSpan(UUID.randomUUID(), "T");

            assertThat(span).isNotNull();
            assertThat(span.contextToStore()).isEmpty();
            assertThatCode(
                            () -> {
                                span.coalesced(UUID.randomUUID());
                                span.error(new RuntimeException("x"));
                                span.close();
                                span.close(); // idempotent
                            })
                    .doesNotThrowAnyException();
        }

        @Test
        void processSpanIsFunctionalNoop() {
            OutboxTracer.ProcessSpan span =
                    OutboxTracer.NOOP.startProcessSpan(
                            new OutboxTracer.ProcessSpanInfo(
                                    UUID.randomUUID(), "T", 1, WORKER, Map.of()));

            assertThat(span).isNotNull();
            assertThatCode(
                            () -> {
                                span.error(new RuntimeException("x"));
                                span.close();
                                span.close(); // idempotent
                            })
                    .doesNotThrowAnyException();
        }

        @Test
        void rejectsNullArguments() {
            assertThatNullPointerException()
                    .isThrownBy(() -> OutboxTracer.NOOP.startPublishSpan(null, "T"));
            assertThatNullPointerException()
                    .isThrownBy(() -> OutboxTracer.NOOP.startPublishSpan(UUID.randomUUID(), null));
            assertThatNullPointerException()
                    .isThrownBy(() -> OutboxTracer.NOOP.startProcessSpan(null));
        }
    }

    @Nested
    class ProcessSpanInfoContract {

        @Test
        void rejectsNullStoredContext() {
            assertThatThrownBy(
                            () ->
                                    new OutboxTracer.ProcessSpanInfo(
                                            UUID.randomUUID(), "T", 1, WORKER, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("storedContext");
        }

        @Test
        void storedContextIsDefensivelyCopiedAndUnmodifiable() {
            Map<String, String> source = new HashMap<>();
            source.put("traceparent", "00-abc-def-01");
            var info = new OutboxTracer.ProcessSpanInfo(UUID.randomUUID(), "T", 1, WORKER, source);

            source.put("mutated", "yes");

            assertThat(info.storedContext()).containsOnlyKeys("traceparent");
            assertThatThrownBy(() -> info.storedContext().put("k", "v"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void rejectsNullRequiredComponents() {
            UUID id = UUID.randomUUID();
            assertThatNullPointerException()
                    .isThrownBy(
                            () -> new OutboxTracer.ProcessSpanInfo(null, "T", 1, WORKER, Map.of()));
            assertThatNullPointerException()
                    .isThrownBy(
                            () -> new OutboxTracer.ProcessSpanInfo(id, null, 1, WORKER, Map.of()));
            assertThatNullPointerException()
                    .isThrownBy(() -> new OutboxTracer.ProcessSpanInfo(id, "T", 1, null, Map.of()));
        }
    }
}
