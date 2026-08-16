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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Cancellation semantics of a single dispatch: interrupt once, never after the dispatch ended. */
class DispatchHandleTest {

    @AfterEach
    void clearInterruptStatus() {
        // Never hand an interrupted thread back to JUnit, whatever the assertions did.
        Thread.interrupted();
    }

    @Test
    @DisplayName("interruptIfActive unblocks the dispatching thread and reports the interrupt")
    void interruptsActiveDispatch() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch blocked = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread worker =
                new Thread(
                        () -> {
                            started.countDown();
                            try {
                                blocked.await();
                            } catch (InterruptedException e) {
                                interrupted.set(true);
                                Thread.currentThread().interrupt();
                            }
                        },
                        "dispatch-under-test");
        worker.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        DispatchHandle handle = new DispatchHandle(worker);
        assertThat(handle.interruptIfActive()).isTrue();

        worker.join(Duration.ofSeconds(5).toMillis());
        assertThat(worker.isAlive()).isFalse();
        assertThat(interrupted).isTrue();
        assertThat(handle.threadName()).isEqualTo("dispatch-under-test");
    }

    @Test
    @DisplayName("a deactivated handle never interrupts — the pool thread has moved on")
    void doesNotInterruptFinishedDispatch() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean sawInterrupt = new AtomicBoolean();
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                // Stands in for the next event picked up by the same pool thread.
                                sawInterrupt.set(!release.await(2, TimeUnit.SECONDS));
                            } catch (InterruptedException e) {
                                sawInterrupt.set(true);
                            }
                        },
                        "recycled-pool-thread");
        worker.start();

        DispatchHandle handle = new DispatchHandle(worker);
        handle.deactivate();
        assertThat(handle.isActive()).isFalse();
        assertThat(handle.interruptIfActive()).isFalse();

        release.countDown();
        worker.join(Duration.ofSeconds(5).toMillis());
        assertThat(sawInterrupt).isFalse();
    }

    @Test
    @DisplayName("consumeInterrupt clears a watchdog interrupt and is repeatable")
    void consumesWatchdogInterrupt() {
        DispatchHandle handle = new DispatchHandle(Thread.currentThread());

        assertThat(handle.interruptIfActive()).isTrue();
        assertThat(Thread.currentThread().isInterrupted()).isTrue();

        // Called first when the handler unwinds, then again before the thread returns to the pool.
        handle.consumeInterrupt();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        handle.consumeInterrupt();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @Test
    @DisplayName("consumeInterrupt leaves an interrupt the watchdog did not cause alone")
    void leavesForeignInterruptAlone() {
        DispatchHandle handle = new DispatchHandle(Thread.currentThread());

        // A handler that restores its own interrupt status, or a shutdownNow() — not ours to eat.
        Thread.currentThread().interrupt();
        handle.consumeInterrupt();

        assertThat(Thread.interrupted()).isTrue();
    }

    @Test
    @DisplayName("each watchdog interrupt is consumed once — a later shutdownNow() survives")
    void consumesEachInterruptOnce() {
        DispatchHandle handle = new DispatchHandle(Thread.currentThread());

        assertThat(handle.interruptIfActive()).isTrue();
        handle.consumeInterrupt();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();

        // shutdownNow() during the drain, after this dispatch already consumed its own interrupt:
        // the next consumeInterrupt() of the same dispatch must not swallow it.
        Thread.currentThread().interrupt();
        handle.consumeInterrupt();
        assertThat(Thread.interrupted()).isTrue();

        // A fresh watchdog interrupt is still consumed as usual.
        assertThat(handle.interruptIfActive()).isTrue();
        handle.consumeInterrupt();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }
}
