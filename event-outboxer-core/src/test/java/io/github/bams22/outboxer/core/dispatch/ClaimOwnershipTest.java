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

import io.github.bams22.outboxer.api.handle.*;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.*;
import io.github.bams22.outboxer.spi.*;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class ClaimOwnershipTest {
    final AtomicReference<Instant> now =
            new AtomicReference<>(Instant.parse("2026-10-31T00:00:00Z"));
    final InMemoryEventStore store = new InMemoryEventStore(now::get);
    final WorkerId worker = new WorkerId("local");
    final InFlightRegistry registry = new InFlightRegistry();
    final AtomicInteger handled = new AtomicInteger();

    ClaimedEvent claim(WorkerId owner) {
        store.save(
                PendingEvent.builder()
                        .id(UUID.randomUUID())
                        .eventType("T")
                        .payload(SerializedPayload.ofText("value"))
                        .payloadFormat(StringEventSerializer.FORMAT)
                        .payloadClass(String.class.getName())
                        .runAt(now.get())
                        .traceContext(Map.of())
                        .build());
        return store.claim(new ClaimRequest("T", owner, 1)).getFirst();
    }

    HandlerDispatcher dispatcher(boolean brokenKey) {
        return dispatcher(brokenKey, io.github.bams22.outboxer.api.observer.OutboxListener.NOOP);
    }

    HandlerDispatcher dispatcher(
            boolean brokenKey, io.github.bams22.outboxer.api.observer.OutboxListener listener) {
        EventHandler<String> handler =
                new EventHandler<>() {
                    public EventType<String> type() {
                        return EventType.of("T", String.class);
                    }

                    public String extractLockKey(String payload) {
                        if (brokenKey) throw new IllegalArgumentException("bad key");
                        return null;
                    }

                    public EventOutcome handle(EventContext ctx, String payload) {
                        handled.incrementAndGet();
                        return EventOutcome.success();
                    }

                    public FailureHandler<String> failureHandler() {
                        return ctx -> new FailureDecision.Disable("key extraction failed");
                    }
                };
        return HandlerDispatcher.builder()
                .store(store)
                .workerId(worker)
                .inFlight(registry)
                .listener(listener)
                .clock(now::get)
                .handlerResolver(new EventHandlerResolver(List.of(handler)))
                .serializerRegistry(
                        EventSerializerRegistry.of(List.of(new StringEventSerializer())))
                .build();
    }

    @Test
    void onlyAbandonedLocalClaimsAreSweptRegardlessOfQueueAgeOrPeerTimeout() {
        var queued = claim(worker);
        var running = claim(worker);
        var abandoned = claim(worker);
        var peer = claim(new WorkerId("peer"));
        registry.queued(queued);
        var entry =
                new InFlightRegistry.Entry(
                        running.id(),
                        "T",
                        worker,
                        running.claimedVersion(),
                        now.get(),
                        new DispatchHandle(Thread.currentThread()));
        registry.register(entry);
        now.updateAndGet(t -> t.plus(Duration.ofDays(1)));
        assertThat(registry.sweepAbandoned(store, Duration.ofSeconds(1), 100, worker)).isEqualTo(1);
        assertThat(store.findById(abandoned.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PENDING);
        for (var claim : List.of(queued, running, peer)) {
            assertThat(store.findById(claim.id()).orElseThrow().version())
                    .isEqualTo(claim.claimedVersion());
        }
        dispatcher(false).dispatch(queued);
        assertThat(handled).hasValue(1);
        registry.unregister(entry);
    }

    @Test
    void staleQueuedTaskDoesNotRunAfterReclaim() {
        var claim = claim(worker);
        var dispatcher = dispatcher(false);
        dispatcher.queued(claim);
        store.forceReclaim(claim.id(), worker, claim.claimedVersion(), now.get());
        dispatcher.dispatch(claim);
        assertThat(handled).hasValue(0);
        assertThat(registry.size()).isZero();
    }

    @Test
    void staleDispatchCannotEraseNewerAttemptFromWatchdog() {
        var old = claim(worker);
        store.forceReclaim(old.id(), worker, old.claimedVersion(), now.get());
        var fresh = store.claim(new ClaimRequest("T", worker, 1)).getFirst();
        var entry =
                new InFlightRegistry.Entry(
                        fresh.id(),
                        "T",
                        worker,
                        fresh.claimedVersion(),
                        now.get(),
                        new DispatchHandle(Thread.currentThread()));
        registry.register(entry);
        dispatcher(false).dispatch(old);
        assertThat(registry.snapshot()).containsExactly(entry);
        assertThat(handled).hasValue(0);
        registry.unregister(entry);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void watchdogSeesBlockedClaimAndCoalescingCallbacks(boolean coalescing) throws Exception {
        var original = claim(worker);
        var claimed =
                new ClaimedEvent(
                        original.id(),
                        original.eventType(),
                        original.payload(),
                        original.payloadFormat(),
                        original.payloadClass(),
                        original.priority(),
                        original.attempts(),
                        original.createdAt(),
                        original.claimedAt(),
                        original.traceContext(),
                        original.claimedVersion(),
                        "key",
                        List.of(new CoalescedEvent(UUID.randomUUID(), Map.of())));
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var listener =
                new io.github.bams22.outboxer.api.observer.OutboxListener() {
                    void block() {
                        entered.countDown();
                        boolean done = false;
                        while (!done) {
                            try {
                                release.await();
                                done = true;
                            } catch (InterruptedException ignored) {
                                /* Simulate uninterruptible client code. */
                            }
                        }
                    }

                    public void onEventClaimed(
                            io.github.bams22.outboxer.api.observer.EventClaimedInfo info) {
                        if (!coalescing) block();
                    }

                    public void onEventCoalesced(
                            io.github.bams22.outboxer.api.observer.EventCoalescedInfo info) {
                        if (coalescing) block();
                    }
                };
        var dispatcher = dispatcher(false, listener);
        dispatcher.queued(claimed);
        var task =
                new java.util.concurrent.FutureTask<Void>(
                        () -> {
                            dispatcher.dispatch(claimed);
                            return null;
                        });
        var thread = Thread.ofPlatform().start(task);
        try {
            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            now.updateAndGet(t -> t.plus(Duration.ofDays(1)));
            io.github.bams22.outboxer.core.maintenance.WatchdogTask.builder()
                    .store(store)
                    .inFlight(registry)
                    .clock(now::get)
                    .build()
                    .run();
            assertThat(store.findById(claimed.id()).orElseThrow().status())
                    .isEqualTo(EventStatus.PENDING);
            assertThat(registry.abandonedCount()).isEqualTo(1);
        } finally {
            release.countDown();
            task.get(5, java.util.concurrent.TimeUnit.SECONDS);
            thread.join(5000);
        }
        assertThat(handled).hasValue(0);
        assertThat(registry.size()).isZero();
        assertThat(registry.abandonedCount()).isZero();
    }

    @Test
    void throwingCallbackCleansBothQueueAndRunningRegistration() {
        var claimed = claim(worker);
        var dispatcher =
                dispatcher(
                        false,
                        new io.github.bams22.outboxer.api.observer.OutboxListener() {
                            public void onEventClaimed(
                                    io.github.bams22.outboxer.api.observer.EventClaimedInfo info) {
                                throw new IllegalStateException("listener failed");
                            }
                        });
        dispatcher.queued(claimed);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> dispatcher.dispatch(claimed))
                .isInstanceOf(IllegalStateException.class);
        assertThat(registry.size()).isZero();
        now.updateAndGet(t -> t.plus(Duration.ofDays(1)));
        assertThat(registry.sweepAbandoned(store, Duration.ofMinutes(10), 100, worker))
                .isEqualTo(1);
    }

    @Test
    void keyExtractionErrorUsesFailurePolicyImmediately() {
        var claim = claim(worker);
        dispatcher(true).dispatch(claim);
        assertThat(handled).hasValue(0);
        assertThat(store.findById(claim.id()).orElseThrow().status())
                .isEqualTo(EventStatus.DISABLED);
        assertThat(registry.size()).isZero();
    }
}
