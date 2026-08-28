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

import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
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
 * An application with the starter on the classpath but no {@code EventHandler} bean — a service
 * that only emits events, or one whose handlers are deployed elsewhere — boots as a publish-only
 * node once it says so with {@code event-outboxer.publish-only=true} (ADR-0029).
 */
@SpringBootTest(
        classes = InMemoryStarterPublishOnlyTest.TestApp.class,
        properties = {
            "event-outboxer.publish-only=true",
            "event-outboxer.publisher.no-transaction-policy=IGNORE",
            "event-outboxer.event-types.defaults.poll-min-interval=20ms",
            "event-outboxer.event-types.defaults.poll-max-interval=50ms"
        })
@Import(OutboxInMemoryTestConfiguration.class)
class InMemoryStarterPublishOnlyTest {

    @Autowired OutboxEventPublisher publisher;
    @Autowired EventStore store;
    @Autowired OutboxEngine engine;
    @Autowired MeterRegistry meterRegistry;

    @Test
    @DisplayName("the engine runs, publishes, and leaves events for another instance to process")
    void publishOnlyNode() throws InterruptedException {
        assertThat(engine.state()).isEqualTo(OutboxEngine.State.RUNNING);

        UUID id = publisher.publish("ORDER", "order-1");
        Thread.sleep(150); // several poll intervals — nothing must claim it

        assertThat(store.findById(id))
                .isPresent()
                .get()
                .satisfies(e -> assertThat(e.status()).isEqualTo(EventStatus.PENDING));
        assertThat(
                        meterRegistry
                                .get("event_outboxer.engine.state")
                                .tag("state", "running")
                                .gauge()
                                .value())
                .isEqualTo(1.0);
        assertThat(meterRegistry.find("event_outboxer.events.backlog").gauges()).isEmpty();
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
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
