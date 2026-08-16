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

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.CountAtBucket;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
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
 * Verifies {@code OutboxMetricsDefaultsEnvironmentPostProcessor} end to end: the SLO defaults
 * shipped in {@code META-INF/event-outboxer/metrics-defaults.yml} are applied automatically, lose
 * to application-set properties (addLast semantics), and can be switched off entirely. Exercising a
 * real context catches silent failures — a meter-name typo in the YAML or a broken {@code
 * spring.factories} registration would otherwise go unnoticed.
 */
class MetricsDistributionDefaultsTest {

    private static final String TIMER = "event_outboxer.events.processing_time";

    private static Timer timer(MeterRegistry registry, String name) {
        return registry.timer(name, "event_type", "ORDER");
    }

    @Nested
    @SpringBootTest(
            classes = TestApp.class,
            properties = "event-outboxer.publisher.no-transaction-policy=IGNORE")
    @Import(io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration.class)
    class DefaultsApplied {

        @Autowired MeterRegistry registry;

        @Test
        void timersGetTheShippedSloBuckets() {
            CountAtBucket[] processing = timer(registry, TIMER).takeSnapshot().histogramCounts();
            assertThat(processing).hasSize(21);
            assertThat(processing[0].bucket(TimeUnit.MILLISECONDS)).isEqualTo(10.0);
            // The tail must cover the default handler-max-runtime budget (5m) and one
            // bucket beyond it.
            assertThat(processing[20].bucket(TimeUnit.MINUTES)).isEqualTo(10.0);

            CountAtBucket[] queue =
                    timer(registry, "event_outboxer.events.queue_time")
                            .takeSnapshot()
                            .histogramCounts();
            assertThat(queue).hasSize(23);
            // Retried events wait from the original publish — the tail reaches 1h.
            assertThat(queue[22].bucket(TimeUnit.MINUTES)).isEqualTo(60.0);
            CountAtBucket[] stuck =
                    timer(registry, "event_outboxer.handler.stuck_time")
                            .takeSnapshot()
                            .histogramCounts();
            assertThat(stuck).hasSize(8);
            assertThat(stuck[0].bucket(TimeUnit.SECONDS)).isEqualTo(30.0);

            // SLO buckets only — the defaults deliberately publish no client-side percentiles.
            assertThat(timer(registry, TIMER).takeSnapshot().percentileValues()).isEmpty();
        }
    }

    @Nested
    @SpringBootTest(
            classes = TestApp.class,
            properties = {
                "event-outboxer.publisher.no-transaction-policy=IGNORE",
                "management.metrics.distribution.slo.event_outboxer.events.processing_time=1s"
            })
    @Import(io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration.class)
    class ApplicationOverrideWins {

        @Autowired MeterRegistry registry;

        @Test
        void applicationSetSloReplacesTheDefaultGrid() {
            CountAtBucket[] buckets = timer(registry, TIMER).takeSnapshot().histogramCounts();
            assertThat(buckets).hasSize(1);
            assertThat(buckets[0].bucket(TimeUnit.SECONDS)).isEqualTo(1.0);
        }
    }

    @Nested
    @SpringBootTest(
            classes = TestApp.class,
            properties = {
                "event-outboxer.publisher.no-transaction-policy=IGNORE",
                "event-outboxer.metrics.distribution-defaults.enabled=false"
            })
    @Import(io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration.class)
    class OptOutDisablesDefaults {

        @Autowired MeterRegistry registry;

        @Test
        void noBucketsWhenDisabled() {
            assertThat(timer(registry, TIMER).takeSnapshot().histogramCounts()).isEmpty();
        }
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
