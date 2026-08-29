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
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * The YAML failure policy end to end (ADR-0030): per-type overrides drive how an always-failing
 * handler ends up, and a per-type bean beats the YAML override for its type.
 */
@SpringBootTest(
        classes = FailurePolicyEndToEndTest.TestApp.class,
        properties = {
            "event-outboxer.publisher.no-transaction-policy=IGNORE",
            "event-outboxer.event-types.defaults.poll-min-interval=10ms",
            "event-outboxer.event-types.defaults.poll-max-interval=30ms",
            "event-outboxer.event-types.defaults.handler-pool-size=1",
            // ORDER: disable on the first failure.
            "event-outboxer.event-types.overrides.ORDER.failure.strategy=none",
            // NOTIFY: two quick attempts, then disable.
            "event-outboxer.event-types.overrides.NOTIFY.failure.strategy=fixed",
            "event-outboxer.event-types.overrides.NOTIFY.failure.fixed-delay=10ms",
            "event-outboxer.event-types.overrides.NOTIFY.failure.max-attempts=2",
            // AUDIT: YAML says disable, but the per-type bean below says delete — the bean wins.
            "event-outboxer.event-types.overrides.AUDIT.failure.strategy=none"
        })
@Import(OutboxInMemoryTestConfiguration.class)
class FailurePolicyEndToEndTest {

    @Autowired OutboxEventPublisher publisher;
    @Autowired EventStore store;
    @Autowired FailingHandler orderHandler;
    @Autowired FailingHandler notifyHandler;
    @Autowired FailingHandler auditHandler;

    @Test
    @DisplayName("strategy none → DISABLED after a single attempt")
    void noneDisablesImmediately() {
        UUID id = publisher.publish(EventType.of("ORDER", String.class), "o-1");

        await().atMost(Duration.ofSeconds(5)).until(() -> status(id) == EventStatus.DISABLED);
        assertThat(orderHandler.attempts()).isEqualTo(1);
    }

    @Test
    @DisplayName("strategy fixed with max-attempts 2 → DISABLED after exactly two attempts")
    void fixedRetriesThenDisables() {
        UUID id = publisher.publish(EventType.of("NOTIFY", String.class), "n-1");

        await().atMost(Duration.ofSeconds(5)).until(() -> status(id) == EventStatus.DISABLED);
        assertThat(notifyHandler.attempts()).isEqualTo(2);
    }

    @Test
    @DisplayName("a per-type @OutboxFailureHandler bean wins over the YAML override for its type")
    void perTypeBeanBeatsYaml() {
        UUID id = publisher.publish(EventType.of("AUDIT", String.class), "a-1");

        // Delete, not Disable: the bean's decision, not the YAML strategy=none.
        await().atMost(Duration.ofSeconds(5)).until(() -> store.findById(id).isEmpty());
        assertThat(auditHandler.attempts()).isEqualTo(1);
    }

    private EventStatus status(UUID id) {
        return store.findById(id).map(Event::status).orElse(null);
    }

    /** Fails on every attempt and counts them. */
    static final class FailingHandler implements EventHandler<String> {
        private final String type;
        private final AtomicInteger attempts = new AtomicInteger();

        FailingHandler(String type) {
            this.type = type;
        }

        @Override
        public EventType<String> type() {
            return EventType.of(type, String.class);
        }

        @Override
        public EventOutcome handle(EventContext ctx, String payload) {
            attempts.incrementAndGet();
            throw new IllegalStateException("always fails");
        }

        int attempts() {
            return attempts.get();
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(
            exclude = {
                DataSourceAutoConfiguration.class,
                DataSourceTransactionManagerAutoConfiguration.class,
                JdbcTemplateAutoConfiguration.class
            })
    static class TestApp {

        @Bean
        FailingHandler orderHandler() {
            return new FailingHandler("ORDER");
        }

        @Bean
        FailingHandler notifyHandler() {
            return new FailingHandler("NOTIFY");
        }

        @Bean
        FailingHandler auditHandler() {
            return new FailingHandler("AUDIT");
        }

        @Bean
        @OutboxFailureHandler("AUDIT")
        FailureHandler<Object> auditFailures() {
            return ctx -> new FailureDecision.Delete("audit events are never retried");
        }
    }
}
