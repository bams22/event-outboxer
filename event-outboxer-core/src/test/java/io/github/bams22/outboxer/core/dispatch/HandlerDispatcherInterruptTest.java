/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.builtin.FailureHandlers;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.support.ForwardingEventStore;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EventSerializerRegistry;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A watchdog interrupt must die with the handler it was aimed at. Everything the dispatch does
 * afterwards — finalize, lock release, and then the next event on the same pool thread — has to run
 * on a thread with a clean interrupt status: an interrupted finalize can kill a pooled JDBC
 * connection (and, under group-commit batching, take other events' finalizes down with it), and an
 * interrupted lock release would leave the entity lock to expire on its TTL.
 */
class HandlerDispatcherInterruptTest {

    private static final WorkerId WORKER = new WorkerId("interrupt-test-worker");
    private static final String TYPE = "INT";

    private final InMemoryEventStore delegate = new InMemoryEventStore();
    private final AtomicBoolean interruptedDuringFinalize = new AtomicBoolean();
    private final AtomicBoolean interruptedDuringLockRelease = new AtomicBoolean();

    private final EventStore store =
            new ForwardingEventStore(delegate) {
                @Override
                public boolean markForRetry(
                        UUID id,
                        WorkerId workerId,
                        long claimedVersion,
                        String reason,
                        Instant runAt) {
                    interruptedDuringFinalize.set(Thread.currentThread().isInterrupted());
                    return delegate.markForRetry(id, workerId, claimedVersion, reason, runAt);
                }
            };

    private final EntityLocker locker =
            (key, ttl) ->
                    Optional.of(
                            () ->
                                    interruptedDuringLockRelease.set(
                                            Thread.currentThread().isInterrupted()));

    @Test
    @DisplayName(
            "a watchdog interrupt never reaches the finalize, the lock release or the next event")
    void interruptIsConsumedWhenTheHandlerUnwinds() throws Exception {
        CountDownLatch handlerBlocked = new CountDownLatch(1);
        CountDownLatch neverOpens = new CountDownLatch(1);
        InFlightRegistry inFlight = new InFlightRegistry();
        HandlerDispatcher dispatcher =
                dispatcher(
                        inFlight,
                        (ctx, payload) -> {
                            handlerBlocked.countDown();
                            try {
                                neverOpens.await();
                            } catch (InterruptedException e) {
                                // The textbook reaction, and the one that matters here: await()
                                // cleared the flag, the handler puts it back before unwinding.
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("handler interrupted", e);
                            }
                            return EventOutcome.Success.INSTANCE;
                        });

        ClaimedEvent claimed = saveAndClaim();
        AtomicBoolean interruptedAfterDispatch = new AtomicBoolean();
        Thread pool =
                new Thread(
                        () -> {
                            dispatcher.dispatch(claimed);
                            // Stands in for the pool thread picking up the next event.
                            interruptedAfterDispatch.set(Thread.currentThread().isInterrupted());
                        },
                        "outbox-INT-1");
        pool.start();
        assertThat(handlerBlocked.await(5, TimeUnit.SECONDS)).isTrue();

        // What WatchdogTask does after a successful forceReclaim.
        InFlightRegistry.Entry entry = inFlight.snapshot().iterator().next();
        assertThat(entry.handle().interruptIfActive()).isTrue();

        pool.join(Duration.ofSeconds(5).toMillis());
        assertThat(pool.isAlive()).isFalse();

        assertThat(interruptedDuringFinalize).isFalse();
        assertThat(interruptedDuringLockRelease).isFalse();
        assertThat(interruptedAfterDispatch).isFalse();
        assertThat(inFlight.size()).isZero();
        assertThat(inFlight.abandonedCount()).isZero();
    }

    private HandlerDispatcher dispatcher(InFlightRegistry inFlight, Handler body) {
        EventHandler<String> handler =
                new EventHandler<String>() {
                    @Override
                    public String eventType() {
                        return TYPE;
                    }

                    @Override
                    public Class<String> payloadType() {
                        return String.class;
                    }

                    @Override
                    public String extractLockKey(String payload) {
                        return "lock:" + payload;
                    }

                    @Override
                    public EventOutcome handle(EventContext ctx, String payload) {
                        return body.handle(ctx, payload);
                    }
                };
        return new HandlerDispatcher(
                store,
                locker,
                EventSerializerRegistry.of(List.of(new StringEventSerializer())),
                new EventHandlerResolver(List.of(handler)),
                new FailureHandlerResolver(Map.of(), FailureHandlers.defaults()),
                inFlight,
                new OutboxListener() {},
                Clock.system(),
                WORKER,
                new EventTypeConfigProvider(EventTypeConfig.defaults(), Map.of()),
                DispatcherConfig.defaults());
    }

    private ClaimedEvent saveAndClaim() {
        delegate.save(
                PendingEvent.builder()
                        .id(UUID.randomUUID())
                        .eventType(TYPE)
                        .payload(SerializedPayload.ofText("\"p\""))
                        .payloadFormat(StringEventSerializer.FORMAT)
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(Instant.now().minusSeconds(1))
                        .traceContext(Map.of())
                        .build());
        return delegate.claim(new ClaimRequest(TYPE, WORKER, 10)).get(0);
    }

    @FunctionalInterface
    private interface Handler {
        EventOutcome handle(EventContext ctx, String payload);
    }
}
