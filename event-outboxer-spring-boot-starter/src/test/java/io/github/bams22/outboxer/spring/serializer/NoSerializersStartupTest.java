/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.serializer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.NoEventSerializersException;
import io.github.bams22.outboxer.serializer.jackson.JacksonEventSerializer;
import io.github.bams22.outboxer.serializer.protobuf.ProtobufEventSerializer;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spring.OutboxEngineAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.NoOpLockAutoConfiguration;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * JSON via Jackson ships with the starter (ADR-0016, amendment 2026-08-29). Hiding {@link
 * JacksonEventSerializer} from the class loader reproduces what excluding the module from the
 * starter does: {@code JacksonSerializerAutoConfiguration} backs off and the roster is empty unless
 * another serializer takes its place.
 */
class NoSerializersStartupTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    JacksonSerializerAutoConfiguration.class,
                                    ProtobufSerializerAutoConfiguration.class,
                                    NoOpLockAutoConfiguration.class,
                                    OutboxEngineAutoConfiguration.class))
                    .withUserConfiguration(OutboxInMemoryTestConfiguration.class, HandlerOnly.class)
                    .withPropertyValues("event-outboxer.publisher.no-transaction-policy=IGNORE");

    @Test
    @DisplayName("Jackson excluded, nothing added → NoEventSerializersException")
    void jacksonExcludedWithoutReplacementFails() {
        runner.withClassLoader(
                        new FilteredClassLoader(
                                JacksonEventSerializer.class, ProtobufEventSerializer.class))
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasRootCauseInstanceOf(NoEventSerializersException.class)
                                    .rootCause()
                                    .hasMessageContaining("event-outboxer-serializer-jackson")
                                    .hasMessageContaining("event-outboxer-serializer-protobuf");
                        });
    }

    @Test
    @DisplayName("Jackson excluded, protobuf module present → protobuf writes with zero config")
    void jacksonExcludedProtobufWrites() {
        runner.withClassLoader(new FilteredClassLoader(JacksonEventSerializer.class))
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(OutboxSerializers.class).write())
                                    .isInstanceOf(ProtobufEventSerializer.class);
                        });
    }

    @Test
    @DisplayName("Jackson excluded, custom EventSerializer bean → it writes")
    void jacksonExcludedCustomBeanWrites() {
        EventSerializer custom = new StubSerializer();
        runner.withClassLoader(
                        new FilteredClassLoader(
                                JacksonEventSerializer.class, ProtobufEventSerializer.class))
                .withBean("mySerializer", EventSerializer.class, () -> custom)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(OutboxSerializers.class).write())
                                    .isSameAs(custom);
                        });
    }

    @Test
    @DisplayName("failure analyzer names the exclusion, the protobuf module and write-format")
    void analyzer() {
        FailureAnalysis analysis =
                new OutboxSerializerFailureAnalyzer()
                        .analyze(
                                new IllegalStateException(
                                        "wrapped", new NoEventSerializersException("empty")));

        assertThat(analysis).isNotNull();
        assertThat(analysis.getDescription()).contains("EventSerializer");
        assertThat(analysis.getAction())
                .contains("event-outboxer-serializer-jackson")
                .contains("event-outboxer-serializer-protobuf")
                .contains("event-outboxer.serializer.write-format");
        assertThat(new OutboxSerializerFailureAnalyzer().analyze(new IllegalStateException("x")))
                .isNull();
    }

    @Configuration(proxyBeanMethods = false)
    static class HandlerOnly {

        @Bean
        EventHandler<String> noopHandler() {
            return new EventHandler<>() {
                @Override
                public String eventType() {
                    return "NOOP";
                }

                @Override
                public Class<String> payloadType() {
                    return String.class;
                }

                @Override
                public EventOutcome handle(EventContext ctx, String payload) {
                    return EventOutcome.Success.INSTANCE;
                }
            };
        }
    }

    /** Minimal user-defined serializer — never invoked, only registered. */
    static final class StubSerializer implements EventSerializer {
        @Override
        public String format() {
            return "stub";
        }

        @Override
        public SerializedPayload serialize(Object payload) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T deserialize(SerializedPayload payload, Class<T> type) {
            throw new UnsupportedOperationException();
        }
    }
}
