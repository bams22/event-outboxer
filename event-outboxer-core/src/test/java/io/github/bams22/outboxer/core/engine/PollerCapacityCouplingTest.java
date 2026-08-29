/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.observer.DispatchRejectedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.PollCompletedInfo;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The poller↔executor coupling: capacity-aware claiming, full-batch immediate re-poll and the
 * capacity-available wake. Together they remove the {@code claimBatchSize / pollMinInterval}
 * throughput ceiling and the claim/release churn under overload.
 */
class PollerCapacityCouplingTest {

    private InMemoryEventStore store;
    private InMemoryWorkerRegistry registry;
    private OutboxEngine engine;

    @BeforeEach
    void setup() {
        store = new InMemoryEventStore();
        registry = new InMemoryWorkerRegistry();
    }

    @AfterEach
    void teardown() {
        if (engine != null && engine.isLifecycleActive()) {
            engine.stop(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("throughput is not capped by claimBatchSize / pollMinInterval any more")
    void fullBatchTriggersImmediateRepoll() {
        AtomicInteger done = new AtomicInteger();
        // Deliberately hostile settings: batch 2 per poll, poll interval 5s. The old timer-only
        // loop would need ~10 polls × 5s ≈ 50s for 20 events; full-batch re-poll must drain them
        // in a few seconds.
        engine =
                engineWith(
                                cfg ->
                                        cfg.claimBatchSize(2)
                                                .handlerPoolSize(4)
                                                .handlerQueueCapacity(0)
                                                .pollMinInterval(Duration.ofSeconds(5))
                                                .pollMaxInterval(Duration.ofSeconds(10)))
                        .handler(
                                handler(
                                        "BULK",
                                        (ctx, payload) -> {
                                            done.incrementAndGet();
                                            return EventOutcome.success();
                                        }))
                        .build();
        engine.start();

        for (int i = 0; i < 20; i++) {
            engine.publisher().publish(EventType.of("BULK", String.class), "e-" + i);
        }

        await().atMost(Duration.ofSeconds(3)).until(() -> done.get() >= 20);
    }

    @Test
    @DisplayName(
            "saturated executor: poller stops claiming — no rejected dispatches, no release churn")
    void saturationStopsClaimingInsteadOfChurning() throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger done = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        OutboxListener rejectionCounter =
                new OutboxListener() {
                    @Override
                    public void onDispatchRejected(DispatchRejectedInfo info) {
                        rejected.incrementAndGet();
                    }
                };
        engine =
                engineWith(
                                cfg ->
                                        cfg.claimBatchSize(5)
                                                .handlerPoolSize(1)
                                                .handlerQueueCapacity(0)
                                                .pollMinInterval(Duration.ofMillis(10))
                                                .pollMaxInterval(Duration.ofMillis(50)))
                        .listener(rejectionCounter)
                        .handler(
                                handler(
                                        "SAT",
                                        (ctx, payload) -> {
                                            awaitQuietly(gate);
                                            done.incrementAndGet();
                                            return EventOutcome.success();
                                        }))
                        .build();
        engine.start();

        java.util.List<UUID> ids = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ids.add(engine.publisher().publish(EventType.of("SAT", String.class), "s-" + i));
        }

        // Give the poller many poll cycles worth of time while the single slot is blocked.
        Thread.sleep(500);

        // Capacity-aware claiming: exactly one event claimed (in the handler), the other four
        // untouched in PENDING with zero attempts and a pristine version — no claim/release
        // write churn while saturated. This is the strict guarantee of the fix: the old loop
        // would have produced dozens of claim/reject/release cycles in these 500ms.
        OutboxMetricsSnapshot snap = store.metricsSnapshot();
        assertThat(snap.totalProcessing()).isEqualTo(1);
        assertThat(snap.totalPending()).isEqualTo(4);
        assertThat(rejected.get()).isZero();
        long pendingUntouched =
                ids.stream()
                        .map(id -> store.findById(id).orElseThrow())
                        .filter(e -> e.status() == EventStatus.PENDING)
                        .filter(e -> e.attempts() == 0 && e.version() == 0L)
                        .count();
        assertThat(pendingUntouched).isEqualTo(4);

        gate.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> done.get() >= 5);
        // No zero-rejection assertion for the drain phase: with a synchronous handoff (queue=0)
        // there is an inherent race between the in-flight decrement and the executor's worker
        // returning to take() — a capacity-wake claim can occasionally lose it. That is exactly
        // what the releaseRejected safety net is for; the invariant that matters is that nothing
        // is lost or left claimed.
        assertThat(store.metricsSnapshot().totalProcessing()).isZero();
        assertThat(store.metricsSnapshot().totalPending()).isZero();
    }

    @Test
    @DisplayName(
            "capacity-available wake: next event starts right after a slot frees, not after the"
                    + " poll interval")
    void capacityWakeCutsThePollInterval() {
        AtomicInteger done = new AtomicInteger();
        engine =
                engineWith(
                                cfg ->
                                        cfg.claimBatchSize(1)
                                                .handlerPoolSize(1)
                                                .handlerQueueCapacity(0)
                                                .pollMinInterval(Duration.ofSeconds(5))
                                                .pollMaxInterval(Duration.ofSeconds(10)))
                        .handler(
                                handler(
                                        "CHAIN",
                                        (ctx, payload) -> {
                                            sleepQuietly(200);
                                            done.incrementAndGet();
                                            return EventOutcome.success();
                                        }))
                        .build();
        engine.start();

        long start = System.nanoTime();
        engine.publisher().publish(EventType.of("CHAIN", String.class), "first");
        engine.publisher().publish(EventType.of("CHAIN", String.class), "second");

        // Two sequential 200ms handlers through a single slot. Timer-only polling would need a
        // full 5s interval between them; the capacity wake must chain them back to back.
        await().atMost(Duration.ofSeconds(3)).until(() -> done.get() >= 2);
        long tookMillis = (System.nanoTime() - start) / 1_000_000;
        assertThat(tookMillis).isLessThan(3000);
    }

    @Test
    @DisplayName(
            "claimMinFree: the poller does not top up the queue until free capacity reaches the"
                    + " refill threshold, then refills in one claim")
    void refillThresholdGatesClaiming() throws Exception {
        // 1 thread + 4 queued = budget 5; refill only once 4 slots are free (queue drained).
        Semaphore permits = new Semaphore(0);
        AtomicInteger done = new AtomicInteger();
        AtomicInteger polls = new AtomicInteger();
        OutboxListener pollCounter =
                new OutboxListener() {
                    @Override
                    public void onPollCompleted(PollCompletedInfo info) {
                        if (info.claimed() > 0) {
                            polls.incrementAndGet();
                        }
                    }
                };
        engine =
                engineWith(
                                cfg ->
                                        cfg.claimBatchSize(10)
                                                .handlerPoolSize(1)
                                                .handlerQueueCapacity(4)
                                                .claimMinFree(4)
                                                .pollMinInterval(Duration.ofMillis(10))
                                                .pollMaxInterval(Duration.ofMillis(50)))
                        .listener(pollCounter)
                        .handler(
                                handler(
                                        "REFILL",
                                        (ctx, payload) -> {
                                            permits.acquireUninterruptibly();
                                            done.incrementAndGet();
                                            return EventOutcome.success();
                                        }))
                        .build();
        // Publish before starting so the per-publish after-commit wake cannot split the first
        // claim: on start the idle executor has free = 5 >= 4 → the first 5 events are claimed in
        // one go (1 running, 4 queued); the remaining 3 stay PENDING because free capacity is 0.
        for (int i = 0; i < 8; i++) {
            engine.publisher().publish(EventType.of("REFILL", String.class), "r-" + i);
        }
        engine.start();
        await().atMost(Duration.ofSeconds(2))
                .until(() -> store.metricsSnapshot().totalProcessing() == 5);
        assertThat(store.metricsSnapshot().totalPending()).isEqualTo(3);
        assertThat(polls.get()).isEqualTo(1);

        // Three completions free 3 slots — below the threshold of 4 — so across many poll
        // intervals nothing is claimed: no one-row top-ups while the queue is still above the
        // low watermark.
        permits.release(3);
        await().atMost(Duration.ofSeconds(2)).until(() -> done.get() == 3);
        Thread.sleep(300);
        assertThat(store.metricsSnapshot().totalPending()).isEqualTo(3);
        assertThat(store.metricsSnapshot().totalProcessing()).isEqualTo(2);
        assertThat(polls.get()).isEqualTo(1);

        // The fourth completion crosses the threshold: one refill claim takes all 3 at once.
        permits.release(1);
        await().atMost(Duration.ofSeconds(2))
                .until(() -> store.metricsSnapshot().totalPending() == 0);
        assertThat(polls.get()).isEqualTo(2);

        permits.release(4);
        await().atMost(Duration.ofSeconds(5)).until(() -> done.get() >= 8);
        assertThat(store.metricsSnapshot().totalProcessing()).isZero();
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private interface TypeCfg {
        EventTypeConfig.EventTypeConfigBuilder apply(EventTypeConfig.EventTypeConfigBuilder b);
    }

    private OutboxEngineBuilder engineWith(TypeCfg customize) {
        EventTypeConfig cfg =
                customize
                        .apply(
                                EventTypeConfig.defaults().toBuilder()
                                        .handlerMaxRuntime(Duration.ofSeconds(30)))
                        .build();
        MaintenanceConfig maintenance =
                MaintenanceConfig.builder()
                        .heartbeatInterval(Duration.ofMillis(200))
                        .deadThreshold(Duration.ofSeconds(5))
                        .orphanRecoveryInterval(Duration.ofSeconds(60))
                        .watchdogInterval(Duration.ofSeconds(1))
                        .abandonedHandlerGrace(Duration.ofSeconds(1))
                        .reclaimBatchSize(10)
                        .shutdownTimeout(Duration.ofSeconds(2))
                        .staleClaimSweepInterval(Duration.ofMinutes(5))
                        .build();
        return new OutboxEngineBuilder()
                .eventStore(store)
                .workerRegistry(registry)
                .eventSerializer(new StringEventSerializer())
                .defaultEventTypeConfig(cfg)
                .maintenance(maintenance)
                .noTransactionPolicy(NoTransactionPolicy.IGNORE)
                .includeLoggingListener(false);
    }

    @FunctionalInterface
    private interface HandleFn {
        EventOutcome apply(EventContext ctx, String payload);
    }

    private static EventHandler<String> handler(String type, HandleFn fn) {
        return new EventHandler<String>() {
            @Override
            public EventType<String> type() {
                return EventType.of(type, String.class);
            }

            @Override
            public EventOutcome handle(EventContext ctx, String payload) {
                return fn.apply(ctx, payload);
            }
        };
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
