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

import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.Message;
import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.serializer.jackson.JacksonEventSerializer;
import io.github.bams22.outboxer.serializer.protobuf.ProtobufEventSerializer;
import io.github.bams22.outboxer.spring.OutboxEngineAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.NoOpLockAutoConfiguration;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Protobuf autoconfiguration next to Jackson (ADR-0026): the protobuf bean registers additively and
 * never steals the writer role — Jackson's {@code outboxEventSerializer}-named bean keeps winning
 * (ADR-0025 rule 3) until {@code write-format=protobuf} says otherwise; in protobuf-only setups the
 * single bean writes with zero config (rule 2).
 */
class ProtobufSerializerResolutionTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    JacksonSerializerAutoConfiguration.class,
                                    ProtobufSerializerAutoConfiguration.class,
                                    NoOpLockAutoConfiguration.class,
                                    OutboxEngineAutoConfiguration.class))
                    .withUserConfiguration(
                            OutboxInMemoryTestConfiguration.class, HandlerOnly.class);

    @Test
    @DisplayName("both modules, zero config → Jackson writes, protobuf registers read-only")
    void protobufBeanRegistersNextToJacksonAsReadOnly() {
        runner.run(
                ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx).hasSingleBean(ProtobufEventSerializer.class);
                    OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                    assertThat(serializers.write()).isInstanceOf(JacksonEventSerializer.class);
                    assertThat(serializers.readOnly())
                            .singleElement()
                            .isInstanceOf(ProtobufEventSerializer.class);
                });
    }

    @Test
    @DisplayName("write-format=protobuf switches the writer; Jackson becomes read-only")
    void writeFormatProtobufSelectsTheProtobufWriter() {
        runner.withPropertyValues("event-outboxer.serializer.write-format=protobuf")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                            assertThat(serializers.write().format()).isEqualTo("protobuf");
                            assertThat(serializers.readOnly())
                                    .singleElement()
                                    .isInstanceOf(JacksonEventSerializer.class);
                        });
    }

    @Test
    @DisplayName("write-format-per-type moves one event type to protobuf; Jackson keeps the rest")
    void perTypeProtobufOverrideKeepsJacksonAsDefaultWriter() {
        // The gradual-migration setup (ADR-0025 amendment): one event type writes protobuf while
        // every other type — and the default writer — stays Jackson.
        runner.withPropertyValues(
                        "event-outboxer.serializer.write-format-per-type.ORDER_CREATED=protobuf")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                            assertThat(serializers.write())
                                    .isInstanceOf(JacksonEventSerializer.class);
                            assertThat(serializers.writePerType().get("ORDER_CREATED"))
                                    .isInstanceOf(ProtobufEventSerializer.class);
                        });
    }

    @Test
    @DisplayName("a user-defined ProtobufEventSerializer backs off the auto-configured one")
    void userDefinedProtobufSerializerBacksOffTheAutoConfiguredOne() {
        // Without the type-based backoff two beans would share the "protobuf" format id and fail
        // registry construction at startup.
        runner.withUserConfiguration(CustomProtobufSerializer.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).hasSingleBean(ProtobufEventSerializer.class);
                            assertThat(ctx.getBean(ProtobufEventSerializer.class))
                                    .isSameAs(ctx.getBean(CustomProtobufSerializer.class).instance);
                        });
    }

    @Test
    @DisplayName("protobuf-java absent from the classpath → clean backoff, Jackson writes")
    void backsOffWhenProtobufNotOnClasspath() {
        runner.withClassLoader(new FilteredClassLoader(Message.class))
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).doesNotHaveBean("outboxProtobufEventSerializer");
                            OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                            assertThat(serializers.write())
                                    .isInstanceOf(JacksonEventSerializer.class);
                            assertThat(serializers.readOnly()).isEmpty();
                        });
    }

    @Test
    @DisplayName(
            "protobuf-only setup (Jackson module excluded from the starter) → zero-config writer")
    void protobufOnlySetupWritesProtobufZeroConfig() {
        new ApplicationContextRunner()
                .withConfiguration(
                        AutoConfigurations.of(
                                ProtobufSerializerAutoConfiguration.class,
                                NoOpLockAutoConfiguration.class,
                                OutboxEngineAutoConfiguration.class))
                .withUserConfiguration(OutboxInMemoryTestConfiguration.class, HandlerOnly.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                            assertThat(serializers.write())
                                    .isInstanceOf(ProtobufEventSerializer.class);
                            assertThat(serializers.write().format()).isEqualTo("protobuf");
                            assertThat(serializers.readOnly()).isEmpty();
                        });
    }

    @Configuration
    static class CustomProtobufSerializer {

        final ProtobufEventSerializer instance =
                new ProtobufEventSerializer(ExtensionRegistryLite.getEmptyRegistry());

        @Bean
        ProtobufEventSerializer customProtobufSerializer() {
            return instance;
        }
    }

    @Configuration
    static class HandlerOnly {

        @Bean
        EventHandler<String> handler() {
            return new EventHandler<String>() {
                @Override
                public EventType<String> type() {
                    return EventType.of("T", String.class);
                }

                @Override
                public EventOutcome handle(EventContext ctx, String payload) {
                    return EventOutcome.success();
                }
            };
        }
    }
}
