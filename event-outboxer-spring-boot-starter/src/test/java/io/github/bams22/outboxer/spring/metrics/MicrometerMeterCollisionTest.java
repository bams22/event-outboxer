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
import io.github.bams22.outboxer.api.observer.EventDisabledInfo;
import io.github.bams22.outboxer.metrics.micrometer.MicrometerOutboxListener;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the meter name collision between the starter's backlog gauges and the
 * listener's counters: both used to register {@code event_outboxer.events.disabled} (gauge vs
 * counter) with identical tags. The eager gauge registration won, and the lazy counter registration
 * threw {@code IllegalArgumentException} on the first {@code onEventDisabled} — silently swallowed
 * by {@code OutboxListenerRegistry}, so disable-rate never recorded in a Spring app.
 *
 * <p>Registers both meter sources against one {@link SimpleMeterRegistry}, exactly as a Spring
 * context would, and asserts they coexist.
 */
class MicrometerMeterCollisionTest {

    @Test
    void backlogGaugesAndListenerCountersCoexistInOneRegistry() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerAutoConfiguration config = new MicrometerAutoConfiguration();

        // Eager gauge registration, as done at context refresh.
        config.outboxBacklogGauges(
                registry,
                new InMemoryEventStore(),
                Clock.system(),
                List.of(new OrderHandler()),
                new OutboxProperties());

        // Lazy counter registration on the first disable — must not collide with the gauges.
        MicrometerOutboxListener listener = new MicrometerOutboxListener(registry);
        listener.onEventDisabled(
                new EventDisabledInfo(UUID.randomUUID(), "ORDER", 5, "retries exhausted", null));

        assertThat(
                        registry.get("event_outboxer.events.disabled")
                                .tag("event_type", "ORDER")
                                .counter()
                                .count())
                .isEqualTo(1.0);
        assertThat(
                        registry.get("event_outboxer.events.backlog")
                                .tag("event_type", "ORDER")
                                .tag("status", "disabled")
                                .gauge()
                                .value())
                .isEqualTo(0.0);
    }

    record OrderCreated(String orderId) {}

    static final class OrderHandler implements EventHandler<OrderCreated> {

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
}
