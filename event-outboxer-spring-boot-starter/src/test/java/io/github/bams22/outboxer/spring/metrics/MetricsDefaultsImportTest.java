/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.UUID;
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
 * Verifies the shipped {@code META-INF/event-outboxer/metrics-defaults.yml}: importing it via
 * {@code spring.config.import} must make every event-outboxer timer publish the five client-side
 * percentiles (p50/p75/p90/p95/p99). This is an end-to-end check that the property keys in the YAML
 * actually bind — a typo in a meter name there fails silently otherwise.
 */
@SpringBootTest(
        classes = MetricsDefaultsImportTest.TestApp.class,
        properties = {
            "spring.config.import=classpath:META-INF/event-outboxer/metrics-defaults.yml",
            "event-outboxer.publisher.no-transaction-policy=IGNORE",
            "event-outboxer.event-types.defaults.poll-min-interval=20ms",
            "event-outboxer.event-types.defaults.poll-max-interval=50ms"
        })
@Import(OutboxInMemoryTestConfiguration.class)
class MetricsDefaultsImportTest {

    @Autowired OutboxEventPublisher publisher;
    @Autowired EventStore store;
    @Autowired MeterRegistry meterRegistry;

    @Test
    void importedDefaultsPublishPercentilesOnTimers() {
        UUID id = publisher.publish("ORDER", new OrderCreated("ord-1"));

        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> store.findById(id).isEmpty());

        ValueAtPercentile[] percentiles =
                meterRegistry
                        .get("event_outboxer.events.processing_time")
                        .tag("event_type", "ORDER")
                        .timer()
                        .takeSnapshot()
                        .percentileValues();

        assertThat(percentiles)
                .extracting(ValueAtPercentile::percentile)
                .containsExactly(0.5, 0.75, 0.90, 0.95, 0.99);

        assertThat(
                        meterRegistry
                                .get("event_outboxer.events.queue_time")
                                .tag("event_type", "ORDER")
                                .timer()
                                .takeSnapshot()
                                .percentileValues())
                .hasSize(5);
    }

    record OrderCreated(String orderId) {}

    static class OrderHandler implements EventHandler<OrderCreated> {
        @Override
        public String eventType() {
            return "ORDER";
        }

        @Override
        public Class<OrderCreated> payloadType() {
            return OrderCreated.class;
        }

        @Override
        public EventOutcome handle(EventContext ctx, OrderCreated payload) {
            return EventOutcome.Success.INSTANCE;
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
        OrderHandler orderHandler() {
            return new OrderHandler();
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
