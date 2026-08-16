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
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
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
                                            return EventOutcome.Success.INSTANCE;
                                        }))
                        .build();
        engine.start();

        for (int i = 0; i < 20; i++) {
            engine.publisher().publish("BULK", "e-" + i);
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
                                            return EventOutcome.Success.INSTANCE;
                                        }))
                        .build();
        engine.start();

        java.util.List<UUID> ids = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ids.add(engine.publisher().publish("SAT", "s-" + i));
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
                                            return EventOutcome.Success.INSTANCE;
                                        }))
                        .build();
        engine.start();

        long start = System.nanoTime();
        engine.publisher().publish("CHAIN", "first");
        engine.publisher().publish("CHAIN", "second");

        // Two sequential 200ms handlers through a single slot. Timer-only polling would need a
        // full 5s interval between them; the capacity wake must chain them back to back.
        await().atMost(Duration.ofSeconds(3)).until(() -> done.get() >= 2);
        long tookMillis = (System.nanoTime() - start) / 1_000_000;
        assertThat(tookMillis).isLessThan(3000);
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
            public String eventType() {
                return type;
            }

            @Override
            public Class<String> payloadType() {
                return String.class;
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
