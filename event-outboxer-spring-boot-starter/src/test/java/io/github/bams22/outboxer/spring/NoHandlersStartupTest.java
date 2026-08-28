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

import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.domain.exception.NoEventHandlersException;
import io.github.bams22.outboxer.spring.lock.NoOpLockAutoConfiguration;
import io.github.bams22.outboxer.spring.serializer.JacksonSerializerAutoConfiguration;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Without an {@code EventHandler} bean the starter fails fast unless {@code
 * event-outboxer.publish-only=true} (ADR-0029); the failure analyzer names both ways out.
 */
class NoHandlersStartupTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    JacksonSerializerAutoConfiguration.class,
                                    NoOpLockAutoConfiguration.class,
                                    OutboxEngineAutoConfiguration.class))
                    .withUserConfiguration(OutboxInMemoryTestConfiguration.class)
                    .withPropertyValues("event-outboxer.publisher.no-transaction-policy=IGNORE");

    @Test
    @DisplayName("no handler beans and no flag → startup fails with NoEventHandlersException")
    void failsWithoutHandlers() {
        runner.run(
                ctx -> {
                    assertThat(ctx).hasFailed();
                    assertThat(ctx.getStartupFailure())
                            .hasRootCauseInstanceOf(NoEventHandlersException.class);
                });
    }

    @Test
    @DisplayName("event-outboxer.publish-only=true → the engine starts without pollers")
    void publishOnlyStarts() {
        runner.withPropertyValues("event-outboxer.publish-only=true")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(OutboxEngine.class).state())
                                    .isEqualTo(OutboxEngine.State.RUNNING);
                        });
    }

    @Test
    @DisplayName("failure analyzer names the handler bean and the publish-only property")
    void analyzer() {
        FailureAnalysis analysis =
                new OutboxHandlersFailureAnalyzer()
                        .analyze(
                                new IllegalStateException(
                                        "wrapped", new NoEventHandlersException("no handlers")));

        assertThat(analysis).isNotNull();
        assertThat(analysis.getDescription()).contains("EventHandler");
        assertThat(analysis.getAction())
                .contains("EventHandler<T>")
                .contains("event-outboxer.publish-only=true");
        assertThat(new OutboxHandlersFailureAnalyzer().analyze(new IllegalStateException("x")))
                .isNull();
    }
}
