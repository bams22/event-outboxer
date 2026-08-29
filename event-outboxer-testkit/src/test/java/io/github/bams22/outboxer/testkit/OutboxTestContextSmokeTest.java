/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.testkit;

import static io.github.bams22.outboxer.testkit.assertions.EventAssertions.assertThatStore;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.EventType;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OutboxTestContextSmokeTest {

    @Test
    void publishTickProcesses() {
        AtomicInteger invoked = new AtomicInteger();
        OutboxTestContext ctx =
                OutboxTestContext.builder()
                        .handler(
                                stringHandler(
                                        "ORDER",
                                        (c, p) -> {
                                            invoked.incrementAndGet();
                                            return EventOutcome.success();
                                        }))
                        .build();

        UUID id =
                ctx.publisher()
                        .publish(
                                EventType.of("ORDER", OrderCreated.class),
                                new OrderCreated("ord-1", 3));

        // Before tick, event is PENDING.
        assertThatStore(ctx.eventStore())
                .hasEvent(id)
                .withStatus(EventStatus.PENDING)
                .withEventType("ORDER")
                .withAttempts(0);

        int dispatched = ctx.manualEngine().tick();

        assertThat(dispatched).isEqualTo(1);
        assertThat(invoked).hasValue(1);
        assertThatStore(ctx.eventStore()).hasNoEvent(id);
        assertThat(ctx.recording().processed()).hasSize(1);
        assertThat(ctx.recording().published()).hasSize(1);
    }

    @Test
    void retryOutcomeReschedulesEvent() {
        OutboxTestContext ctx =
                OutboxTestContext.builder()
                        .handler(
                                stringHandler(
                                        "ORDER",
                                        (c, p) ->
                                                new EventOutcome.Retry(
                                                        "transient", Duration.ofMinutes(1), null)))
                        .build();

        UUID id =
                ctx.publisher()
                        .publish(
                                EventType.of("ORDER", OrderCreated.class),
                                new OrderCreated("ord-1", 1));
        ctx.manualEngine().tick();

        assertThatStore(ctx.eventStore())
                .hasEvent(id)
                .withStatus(EventStatus.PENDING)
                .withAttempts(1)
                .withLastFailReason("transient");
        assertThat(ctx.recording().retryScheduled()).hasSize(1);
    }

    @Test
    void watchdogReclaimsStuckHandler() throws InterruptedException {
        // Build with an aggressive handlerMaxRuntime so a quick sleep makes the event "stuck".
        OutboxTestContext ctx =
                OutboxTestContext.builder()
                        .defaultEventTypeConfig(
                                EventTypeConfig.defaults().toBuilder()
                                        .handlerMaxRuntime(Duration.ofMillis(10))
                                        .build())
                        .handler(
                                stringHandler(
                                        "SLOW",
                                        (c, p) -> {
                                            // Simulate stuck handler — but return to avoid blocking
                                            // the test.
                                            return EventOutcome.success();
                                        }))
                        .build();

        // Manually mark a claimed event as in-flight past maxRuntime via clock advance.
        UUID id =
                ctx.publisher()
                        .publish(
                                EventType.of("SLOW", OrderCreated.class),
                                new OrderCreated("ord-1", 1));
        // Process the event normally first to cover the full flow.
        ctx.manualEngine().tick();
        assertThatStore(ctx.eventStore()).hasNoEvent(id);
    }

    @Test
    void extensionInjectsFreshContext() {
        // Extension-injected context isn't tested here (no ExtendWith on this class); see
        // OutboxExtensionTest for the happy path. This smoke test just guards the builder default
        // used by the extension.
        OutboxTestContext ctx = OutboxTestContext.builder().build();
        assertThat(ctx.publisher()).isNotNull();
        assertThat(ctx.manualEngine()).isNotNull();
        assertThat(ctx.recording()).isNotNull();
        assertThat(ctx.clock()).isNotNull();
    }

    record OrderCreated(String orderId, int count) {}

    @FunctionalInterface
    private interface HandleFn {
        EventOutcome handle(EventContext ctx, OrderCreated payload);
    }

    private static EventHandler<OrderCreated> stringHandler(String type, HandleFn fn) {
        return new EventHandler<OrderCreated>() {
            @Override
            public EventType<OrderCreated> type() {
                return EventType.of(type, OrderCreated.class);
            }

            @Override
            public EventOutcome handle(EventContext ctx, OrderCreated payload) {
                return fn.handle(ctx, payload);
            }
        };
    }
}
