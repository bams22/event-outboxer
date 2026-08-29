/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.spring.lock.NoOpLockAutoConfiguration;
import io.github.bams22.outboxer.spring.serializer.JacksonSerializerAutoConfiguration;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * How {@code FailureHandler} beans reach the engine (ADR-0030): the {@code @OutboxFailureHandler}
 * qualifier, the legacy bean names, conflicts, and the warning for beans that claim no slot.
 */
@ExtendWith(OutputCaptureExtension.class)
class FailureHandlerBeansTest {

    private static final FailureHandler<Object> DISABLE =
            ctx -> new FailureDecision.Disable("test");
    private static final FailureHandler<Object> DELETE = ctx -> new FailureDecision.Delete("test");

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    JacksonSerializerAutoConfiguration.class,
                                    NoOpLockAutoConfiguration.class,
                                    OutboxEngineAutoConfiguration.class))
                    .withUserConfiguration(OutboxInMemoryTestConfiguration.class, HandlerOnly.class)
                    .withPropertyValues("event-outboxer.publisher.no-transaction-policy=IGNORE");

    @Test
    @DisplayName("@OutboxFailureHandler without a value is the global chain")
    void annotatedGlobal() {
        runner.withUserConfiguration(AnnotatedGlobal.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            FailureHandlerBeans beans =
                                    FailureHandlerBeans.collect(ctx.getBeanFactory());
                            assertThat(beans.global()).isSameAs(DISABLE);
                            assertThat(beans.globalSource()).isEqualTo("globalFailures");
                            assertThat(beans.perType()).isEmpty();
                            assertThat(beans.unregistered()).isEmpty();
                        });
    }

    @Test
    @DisplayName("@OutboxFailureHandler({A, B}) registers the bean for both types")
    void annotatedPerType() {
        runner.withUserConfiguration(AnnotatedPerType.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            FailureHandlerBeans beans =
                                    FailureHandlerBeans.collect(ctx.getBeanFactory());
                            assertThat(beans.global()).isNull();
                            assertThat(beans.perType())
                                    .containsOnlyKeys("SEND_EMAIL", "SEND_SMS")
                                    .allSatisfy(
                                            (type, chain) -> assertThat(chain).isSameAs(DELETE));
                            assertThat(beans.perTypeSources())
                                    .containsEntry("SEND_EMAIL", "notificationFailures");
                        });
    }

    @Test
    @DisplayName("the legacy bean names keep working")
    void legacyNames() {
        runner.withUserConfiguration(LegacyNames.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            FailureHandlerBeans beans =
                                    FailureHandlerBeans.collect(ctx.getBeanFactory());
                            assertThat(beans.global()).isSameAs(DISABLE);
                            assertThat(beans.perType()).containsOnlyKeys("ORDER");
                            assertThat(beans.perTypeSources())
                                    .containsEntry("ORDER", "outboxPerTypeFailureHandlers[ORDER]");
                            assertThat(beans.unregistered()).isEmpty();
                        });
    }

    @Test
    @DisplayName("two global claims fail startup naming both beans")
    void twoGlobals() {
        runner.withUserConfiguration(AnnotatedGlobal.class, LegacyNames.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasRootCauseInstanceOf(
                                            AmbiguousOutboxFailureHandlerException.class)
                                    .hasStackTraceContaining("globalFailures")
                                    .hasStackTraceContaining("outboxDefaultFailureHandler")
                                    .hasStackTraceContaining("exactly one");
                        });
    }

    @Test
    @DisplayName("an annotated per-type bean and a legacy map entry for the same type conflict")
    void perTypeConflict() {
        runner.withUserConfiguration(AnnotatedOrder.class, LegacyNames.class)
                .withPropertyValues("event-outboxer.publish-only=true")
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasRootCauseInstanceOf(
                                            AmbiguousOutboxFailureHandlerException.class)
                                    .hasStackTraceContaining("'ORDER'")
                                    .hasStackTraceContaining("orderFailures")
                                    .hasStackTraceContaining("outboxPerTypeFailureHandlers[ORDER]");
                        });
    }

    @Test
    @DisplayName("a bare FailureHandler bean starts the context but is reported, not used")
    void unregisteredBeanWarns(CapturedOutput output) {
        runner.withUserConfiguration(Unregistered.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            FailureHandlerBeans beans =
                                    FailureHandlerBeans.collect(ctx.getBeanFactory());
                            assertThat(beans.global()).isNull();
                            assertThat(beans.unregistered()).containsExactly("someFailures");
                        });
        assertThat(output)
                .contains("FailureHandler beans [someFailures]")
                .contains("EventHandler.failureHandler()");
    }

    @Test
    @DisplayName("the failure analyzer explains a conflict and ignores other failures")
    void analyzer() {
        FailureAnalysis analysis =
                new OutboxFailureHandlerFailureAnalyzer()
                        .analyze(
                                new IllegalStateException(
                                        "wrapped",
                                        AmbiguousOutboxFailureHandlerException.multiplePerType(
                                                "ORDER", java.util.List.of("a", "b"))));

        assertThat(analysis).isNotNull();
        assertThat(analysis.getDescription()).contains("'ORDER'").contains("[a, b]");
        assertThat(analysis.getAction())
                .contains("@OutboxFailureHandler")
                .contains("event-outboxer.event-types.overrides.TYPE.failure");
        assertThat(
                        new OutboxFailureHandlerFailureAnalyzer()
                                .analyze(new IllegalStateException("x")))
                .isNull();
    }

    @Configuration(proxyBeanMethods = false)
    static class HandlerOnly {

        @Bean
        EventHandler<String> orderHandler() {
            return new EventHandler<>() {
                @Override
                public EventType<String> type() {
                    return EventType.of("ORDER", String.class);
                }

                @Override
                public EventOutcome handle(EventContext ctx, String payload) {
                    return EventOutcome.success();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AnnotatedGlobal {

        @Bean
        @OutboxFailureHandler
        FailureHandler<Object> globalFailures() {
            return DISABLE;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AnnotatedPerType {

        @Bean
        @OutboxFailureHandler({"SEND_EMAIL", "SEND_SMS"})
        FailureHandler<Object> notificationFailures() {
            return DELETE;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AnnotatedOrder {

        @Bean
        @OutboxFailureHandler("ORDER")
        FailureHandler<Object> orderFailures() {
            return DELETE;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class LegacyNames {

        @Bean("outboxDefaultFailureHandler")
        FailureHandler<Object> legacyDefault() {
            return DISABLE;
        }

        @Bean("outboxPerTypeFailureHandlers")
        Map<String, FailureHandler<?>> legacyPerType() {
            return Map.of("ORDER", DELETE);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Unregistered {

        @Bean
        FailureHandler<Object> someFailures() {
            return DISABLE;
        }
    }
}
