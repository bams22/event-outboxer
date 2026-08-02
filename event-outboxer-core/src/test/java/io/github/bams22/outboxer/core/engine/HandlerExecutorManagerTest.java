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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.polling.HandlerExecutorGate;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * In-flight accounting of {@link HandlerExecutorManager}: capacity budget, rejection rollback, the
 * saturated→free wake edge, and generation isolation across drain/restart.
 */
class HandlerExecutorManagerTest {

    private static final String TYPE = "T";

    private HandlerExecutorManager manager;

    private HandlerExecutorManager manager(int poolSize, int queueCapacity) {
        EventTypeConfig cfg =
                EventTypeConfig.defaults().toBuilder()
                        .handlerPoolSize(poolSize)
                        .handlerQueueCapacity(queueCapacity)
                        .build();
        manager =
                new HandlerExecutorManager(
                        HandlerExecutorManagerTest::fixedPool, Map.of(TYPE, cfg));
        return manager;
    }

    /** Mirrors the builder's default factory: fixed pool + bounded queue + AbortPolicy. */
    private static java.util.concurrent.ExecutorService fixedPool(EventTypeConfig cfg) {
        java.util.concurrent.BlockingQueue<Runnable> queue =
                cfg.handlerQueueCapacity() == 0
                        ? new java.util.concurrent.SynchronousQueue<>()
                        : new java.util.concurrent.ArrayBlockingQueue<>(cfg.handlerQueueCapacity());
        return new java.util.concurrent.ThreadPoolExecutor(
                cfg.handlerPoolSize(),
                cfg.handlerPoolSize(),
                0L,
                java.util.concurrent.TimeUnit.MILLISECONDS,
                queue,
                new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
    }

    @AfterEach
    void teardown() {
        if (manager != null) {
            manager.drain(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("freeCapacity = poolSize + queueCapacity when idle, 0 when stopped")
    void capacityBudget() {
        HandlerExecutorManager m = manager(2, 3);
        HandlerExecutorGate gate = m.executorFor(TYPE);

        assertThat(gate.freeCapacity()).isZero(); // not started yet

        m.start();
        assertThat(gate.freeCapacity()).isEqualTo(5);

        m.drain(Duration.ofSeconds(1));
        assertThat(gate.freeCapacity()).isZero();
    }

    @Test
    @DisplayName("submissions consume capacity; completions restore it")
    void accountingFollowsExecution() throws Exception {
        HandlerExecutorManager m = manager(1, 1);
        HandlerExecutorGate gate = m.executorFor(TYPE);
        m.start();

        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        gate.execute(
                () -> {
                    started.countDown();
                    awaitQuietly(release);
                });
        started.await();
        gate.execute(() -> {});

        assertThat(gate.freeCapacity()).isZero();

        release.countDown();
        await().atMost(Duration.ofSeconds(2)).until(() -> gate.freeCapacity() == 2);
    }

    @Test
    @DisplayName("rejected submission rolls its increment back")
    void rejectionRollsBack() throws Exception {
        HandlerExecutorManager m = manager(1, 0);
        HandlerExecutorGate gate = m.executorFor(TYPE);
        m.start();

        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        gate.execute(
                () -> {
                    started.countDown();
                    awaitQuietly(release);
                });
        started.await();
        assertThat(gate.freeCapacity()).isZero();

        // Force the race the capacity check normally prevents: submit into a full executor.
        assertThatThrownBy(() -> gate.execute(() -> {}))
                .isInstanceOf(RejectedExecutionException.class);
        assertThat(gate.freeCapacity()).isZero(); // not negative, not phantom-inflated

        release.countDown();
        await().atMost(Duration.ofSeconds(2)).until(() -> gate.freeCapacity() == 1);
    }

    @Test
    @DisplayName("capacity callback fires exactly on the saturated→free transition")
    void wakeOnlyOnTransition() throws Exception {
        HandlerExecutorManager m = manager(2, 0);
        HandlerExecutorGate gate = m.executorFor(TYPE);
        AtomicInteger wakes = new AtomicInteger();
        gate.onCapacityAvailable(wakes::incrementAndGet);
        m.start();

        // One task in a two-slot pool: never saturated → completion must NOT fire the callback.
        CountDownLatch firstDone = new CountDownLatch(1);
        gate.execute(firstDone::countDown);
        firstDone.await();
        await().atMost(Duration.ofSeconds(1)).until(() -> gate.freeCapacity() == 2);
        assertThat(wakes.get()).isZero();

        // Saturate both slots, then release: exactly one transition edge.
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch bothStarted = new CountDownLatch(2);
        for (int i = 0; i < 2; i++) {
            gate.execute(
                    () -> {
                        bothStarted.countDown();
                        awaitQuietly(release);
                    });
        }
        bothStarted.await();
        assertThat(gate.freeCapacity()).isZero();

        release.countDown();
        await().atMost(Duration.ofSeconds(2)).until(() -> gate.freeCapacity() == 2);
        assertThat(wakes.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("drain drops queued tasks without corrupting the next generation's counter")
    void generationsSurviveDrainWithDroppedQueue() throws Exception {
        HandlerExecutorManager m = manager(1, 5);
        HandlerExecutorGate gate = m.executorFor(TYPE);
        m.start();

        // Saturate: 1 running (ignores the drain interrupt for a while) + 5 queued.
        CountDownLatch started = new CountDownLatch(1);
        gate.execute(
                () -> {
                    started.countDown();
                    sleepIgnoringInterrupts(1000);
                });
        started.await();
        for (int i = 0; i < 5; i++) {
            gate.execute(() -> {});
        }
        assertThat(gate.freeCapacity()).isZero();

        // Drain times out immediately: the 5 queued tasks are dropped, their increments must be
        // compensated; the still-running task decrements its OWN (old) generation on completion.
        m.drain(Duration.ofMillis(50));

        m.start();
        assertThat(gate.freeCapacity()).isEqualTo(6);
        // Even after the abandoned task finally completes, the new generation stays pristine.
        Thread.sleep(1100);
        assertThat(gate.freeCapacity()).isEqualTo(6);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepIgnoringInterrupts(long ms) {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(deadline - System.currentTimeMillis());
            } catch (InterruptedException _) {
                // keep sleeping: simulates a handler that outlives the drain timeout
            }
        }
    }
}
