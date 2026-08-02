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
import io.github.bams22.outboxer.serializer.jackson.JacksonEventSerializer;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spring.OutboxEngineAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.NoOpLockAutoConfiguration;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Write-serializer resolution (ADR-0025): single bean wins by default, {@code
 * event-outboxer.serializer.write-format} arbitrates multiple beans, the documented {@code
 * outboxEventSerializer} override keeps winning, and an unresolvable roster fails startup.
 */
class SerializerResolutionTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    JacksonSerializerAutoConfiguration.class,
                                    NoOpLockAutoConfiguration.class,
                                    OutboxEngineAutoConfiguration.class))
                    .withUserConfiguration(
                            OutboxInMemoryTestConfiguration.class, HandlerOnly.class);

    @Test
    @DisplayName("zero config → the auto-configured Jackson serializer writes")
    void singleBeanIsTheWriteSerializer() {
        runner.run(
                ctx -> {
                    assertThat(ctx).hasNotFailed();
                    OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                    assertThat(serializers.write()).isInstanceOf(JacksonEventSerializer.class);
                    assertThat(serializers.write().format()).isEqualTo("jackson-json");
                    assertThat(serializers.readOnly()).isEmpty();
                });
    }

    @Test
    @DisplayName("write-format picks the writer among multiple beans; the rest become read-only")
    void writeFormatSelectsAmongMultipleBeans() {
        runner.withUserConfiguration(ExtraSerializer.class)
                .withPropertyValues("event-outboxer.serializer.write-format=test-extra")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                            assertThat(serializers.write().format()).isEqualTo("test-extra");
                            assertThat(serializers.readOnly())
                                    .singleElement()
                                    .isInstanceOf(JacksonEventSerializer.class);
                        });
    }

    @Test
    @DisplayName("unknown write-format fails startup listing the registered formats")
    void unknownWriteFormatFailsFast() {
        runner.withPropertyValues("event-outboxer.serializer.write-format=no-such-format")
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .rootCause()
                                    .hasMessageContaining("no-such-format")
                                    .hasMessageContaining("jackson-json");
                        });
    }

    @Test
    @DisplayName("write-format-per-type routes listed event types to their serializer")
    void writeFormatPerTypeMapsTypeToSerializer() {
        runner.withUserConfiguration(ExtraSerializer.class)
                .withPropertyValues(
                        "event-outboxer.serializer.write-format-per-type.ORDER_CREATED=test-extra")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                            assertThat(serializers.write())
                                    .isInstanceOf(JacksonEventSerializer.class);
                            assertThat(serializers.writePerType())
                                    .containsOnlyKeys("ORDER_CREATED");
                            assertThat(serializers.writePerType().get("ORDER_CREATED").format())
                                    .isEqualTo("test-extra");
                        });
    }

    @Test
    @DisplayName("unknown per-type write format fails startup listing the registered formats")
    void unknownPerTypeWriteFormatFailsFast() {
        runner.withPropertyValues(
                        "event-outboxer.serializer.write-format-per-type.ORDER_CREATED=no-such-format")
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .rootCause()
                                    .hasMessageContaining("write-format-per-type.ORDER_CREATED")
                                    .hasMessageContaining("jackson-json");
                        });
    }

    @Test
    @DisplayName("multiple beans without write-format: the outboxEventSerializer override wins")
    void overrideBeanNameWinsAmongMultipleBeans() {
        // The user override replaces the auto-configured bean (@ConditionalOnMissingBean by name)
        // AND out-arbitrates the extra read-only serializer purely by its documented name.
        runner.withUserConfiguration(NamedOverride.class, ExtraSerializer.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                            assertThat(serializers.write().format()).isEqualTo("test-override");
                        });
    }

    @Test
    @DisplayName("extra read-only serializers do not silently steal the writer role")
    void extraSerializersKeepTheAutoConfiguredWriter() {
        // Migration scenario: registering yesterday's serializer for reads must not change what
        // writes — the auto-configured outboxEventSerializer stays the writer (resolution rule 3).
        runner.withUserConfiguration(ExtraSerializer.class, SecondExtraSerializer.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            OutboxSerializers serializers = ctx.getBean(OutboxSerializers.class);
                            assertThat(serializers.write())
                                    .isInstanceOf(JacksonEventSerializer.class);
                            assertThat(serializers.readOnly()).hasSize(2);
                        });
    }

    @Test
    @DisplayName("multiple anonymous beans without write-format fail startup with guidance")
    void ambiguousRosterFailsFast() {
        // Without the Jackson auto-configuration there is no outboxEventSerializer-named bean, so
        // two anonymous serializers cannot be arbitrated — startup must fail actionably.
        new ApplicationContextRunner()
                .withConfiguration(
                        AutoConfigurations.of(
                                NoOpLockAutoConfiguration.class,
                                OutboxEngineAutoConfiguration.class))
                .withUserConfiguration(
                        OutboxInMemoryTestConfiguration.class,
                        HandlerOnly.class,
                        ExtraSerializer.class,
                        SecondExtraSerializer.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .rootCause()
                                    .hasMessageContaining("write-format");
                        });
    }

    private static EventSerializer stub(String format) {
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

    @Configuration
    static class ExtraSerializer {

        @Bean
        EventSerializer extraSerializer() {
            return stub("test-extra");
        }
    }

    @Configuration
    static class SecondExtraSerializer {

        @Bean
        EventSerializer secondExtraSerializer() {
            return stub("test-extra-2");
        }
    }

    @Configuration
    static class NamedOverride {

        @Bean
        EventSerializer outboxEventSerializer() {
            return stub("test-override");
        }
    }

    @Configuration
    static class HandlerOnly {

        @Bean
        EventHandler<String> handler() {
            return new EventHandler<String>() {
                @Override
                public String eventType() {
                    return "T";
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
}
