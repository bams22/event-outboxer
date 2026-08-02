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
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

@SpringBootTest(
        classes = InMemoryStarterSmokeTest.TestApp.class,
        properties = {
            "event-outboxer.publisher.no-transaction-policy=IGNORE",
            "event-outboxer.event-types.defaults.poll-min-interval=20ms",
            "event-outboxer.event-types.defaults.poll-max-interval=50ms",
            "event-outboxer.event-types.defaults.handler-pool-size=2",
            "event-outboxer.maintenance.heartbeat-interval=200ms",
            "event-outboxer.maintenance.dead-threshold=1s",
            "event-outboxer.maintenance.orphan-recovery-interval=500ms",
            "event-outboxer.maintenance.watchdog-interval=500ms"
        })
@Import(OutboxInMemoryTestConfiguration.class)
class InMemoryStarterSmokeTest {

    @Autowired OutboxEventPublisher publisher;
    @Autowired EventStore store;
    @Autowired OutboxEngine engine;
    @Autowired RecordingHandler handler;
    @Autowired MeterRegistry meterRegistry;

    @Test
    void publishedEventIsProcessed() {
        assertThat(engine.state()).isEqualTo(OutboxEngine.State.RUNNING);

        UUID id = publisher.publish("ORDER", new OrderCreated("ord-1", 3));

        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> store.findById(id).isEmpty());

        assertThat(handler.invocationCount()).isGreaterThanOrEqualTo(1);
        assertThat(handler.seen()).containsKey("ord-1");
    }

    @Test
    void engineStateGaugesReflectRunningState() {
        assertThat(engine.state()).isEqualTo(OutboxEngine.State.RUNNING);

        assertThat(
                        meterRegistry
                                .get("event_outboxer.engine.state")
                                .tag("state", "running")
                                .gauge()
                                .value())
                .isEqualTo(1.0);
        assertThat(
                        meterRegistry
                                .get("event_outboxer.engine.state")
                                .tag("state", "stopped")
                                .gauge()
                                .value())
                .isEqualTo(0.0);
        assertThat(
                        meterRegistry
                                .get("event_outboxer.engine.state")
                                .tag("state", "stopping")
                                .gauge()
                                .value())
                .isEqualTo(0.0);
    }

    @Test
    void backlogGaugesAreRegisteredPerEventType() {
        // Gauges are registered eagerly at context refresh; values are pulled on scrape. The
        // per-type
        // rows exist before any event is published — initial values are 0.
        assertThat(
                        meterRegistry
                                .get("event_outboxer.events.pending")
                                .tag("event_type", "ORDER")
                                .gauge())
                .isNotNull();
        assertThat(
                        meterRegistry
                                .get("event_outboxer.events.processing")
                                .tag("event_type", "ORDER")
                                .gauge())
                .isNotNull();
        assertThat(
                        meterRegistry
                                .get("event_outboxer.events.disabled")
                                .tag("event_type", "ORDER")
                                .gauge())
                .isNotNull();
        assertThat(
                        meterRegistry
                                .get("event_outboxer.events.oldest_pending_age_seconds")
                                .tag("event_type", "ORDER")
                                .gauge())
                .isNotNull();

        // Snapshot-wide (no event_type tag).
        assertThat(meterRegistry.get("event_outboxer.events.oldest_claimed_age_seconds").gauge())
                .isNotNull();

        // Values are finite doubles — i.e. the gauge supplier runs cleanly with an empty store.
        assertThat(
                        meterRegistry
                                .get("event_outboxer.events.pending")
                                .tag("event_type", "ORDER")
                                .gauge()
                                .value())
                .isNotNaN();
        assertThat(
                        meterRegistry
                                .get("event_outboxer.events.oldest_claimed_age_seconds")
                                .gauge()
                                .value())
                .isNotNaN();
    }

    record OrderCreated(String orderId, int count) {}

    static class RecordingHandler implements EventHandler<OrderCreated> {
        private final AtomicInteger invocations = new AtomicInteger();
        private final ConcurrentHashMap<String, Integer> seen = new ConcurrentHashMap<>();

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
            invocations.incrementAndGet();
            seen.put(payload.orderId(), payload.count());
            return EventOutcome.Success.INSTANCE;
        }

        int invocationCount() {
            return invocations.get();
        }

        ConcurrentHashMap<String, Integer> seen() {
            return seen;
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
        RecordingHandler recordingHandler() {
            return new RecordingHandler();
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
