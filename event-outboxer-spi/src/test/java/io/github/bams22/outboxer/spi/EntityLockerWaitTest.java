/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.exception.LockAcquisitionException;
import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The default {@link EntityLocker#tryLock(String, Duration, Duration)} against a scripted locker:
 * attempt counts, the zero-wait shortcut, the deadline, interrupt handling and error propagation
 * (ADR-0035). Timing-sensitive contract tests live in {@code AbstractEntityLockerContractTest}.
 */
class EntityLockerWaitTest {

    private static final LockHandle HANDLE = () -> {};
    private static final Duration TTL = Duration.ofSeconds(30);

    /** Busy for the first {@code busyAttempts} calls, then free. */
    private static EntityLocker busyFor(int busyAttempts, AtomicInteger calls) {
        return (key, ttl) ->
                calls.incrementAndGet() > busyAttempts ? Optional.of(HANDLE) : Optional.empty();
    }

    @Test
    @DisplayName("maxWait=0 is exactly one attempt — the pre-ADR-0035 behaviour")
    void zeroWaitIsSingleAttempt() {
        AtomicInteger calls = new AtomicInteger();
        EntityLocker locker = busyFor(1, calls);

        Optional<LockHandle> held = locker.tryLock("k", TTL, Duration.ZERO);

        assertThat(held).isEmpty();
        assertThat(calls).hasValue(1);
    }

    @Test
    @DisplayName("a free key is taken on the first attempt without sleeping")
    void freeKeyIsImmediate() {
        AtomicInteger calls = new AtomicInteger();
        EntityLocker locker = busyFor(0, calls);
        long start = System.nanoTime();

        Optional<LockHandle> held = locker.tryLock("k", TTL, Duration.ofSeconds(5));

        assertThat(held).isPresent();
        assertThat(calls).hasValue(1);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("a key that frees up inside the window is acquired on a later attempt")
    void acquiredAfterRetries() {
        AtomicInteger calls = new AtomicInteger();
        EntityLocker locker = busyFor(3, calls);

        Optional<LockHandle> held = locker.tryLock("k", TTL, Duration.ofSeconds(5));

        assertThat(held).isPresent();
        assertThat(calls).hasValue(4);
    }

    @Test
    @DisplayName("a key that never frees up yields empty once maxWait has elapsed")
    void givesUpAtDeadline() {
        AtomicInteger calls = new AtomicInteger();
        EntityLocker alwaysBusy =
                (key, ttl) -> {
                    calls.incrementAndGet();
                    return Optional.empty();
                };
        Duration maxWait = Duration.ofMillis(60);
        long start = System.nanoTime();

        Optional<LockHandle> held = alwaysBusy.tryLock("k", TTL, maxWait);

        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        assertThat(held).isEmpty();
        assertThat(elapsed).isGreaterThanOrEqualTo(maxWait);
        // Bounded by maxWait plus one attempt; generous slack for a loaded CI host.
        assertThat(elapsed).isLessThan(maxWait.plusSeconds(2));
        // 2+4+8+10+10+... ms: a 60 ms budget is at most ~8 attempts, never a hot loop.
        assertThat(calls.get()).isBetween(2, 12);
    }

    @Test
    @DisplayName("an interrupt ends the wait promptly, returns empty and keeps the flag set")
    void interruptEndsWaitAndPreservesFlag() throws Exception {
        CountDownLatch firstAttempt = new CountDownLatch(1);
        EntityLocker alwaysBusy =
                (key, ttl) -> {
                    firstAttempt.countDown();
                    return Optional.empty();
                };
        AtomicReference<Optional<LockHandle>> result = new AtomicReference<>();
        AtomicBoolean flagAfterReturn = new AtomicBoolean();
        Thread waiter =
                new Thread(
                        () -> {
                            result.set(alwaysBusy.tryLock("k", TTL, Duration.ofSeconds(30)));
                            flagAfterReturn.set(Thread.currentThread().isInterrupted());
                        },
                        "lock-waiter");
        waiter.start();
        assertThat(firstAttempt.await(5, TimeUnit.SECONDS)).isTrue();

        waiter.interrupt();
        waiter.join(Duration.ofSeconds(5).toMillis());

        assertThat(waiter.isAlive()).as("waiter must not sleep out its 30 s budget").isFalse();
        assertThat(result.get()).isEmpty();
        assertThat(flagAfterReturn).isTrue();
    }

    @Test
    @DisplayName("a thread that is already interrupted does not wait at all")
    void alreadyInterruptedDoesNotWait() {
        AtomicInteger calls = new AtomicInteger();
        EntityLocker alwaysBusy =
                (key, ttl) -> {
                    calls.incrementAndGet();
                    return Optional.empty();
                };
        Thread.currentThread().interrupt();
        try {
            Optional<LockHandle> held = alwaysBusy.tryLock("k", TTL, Duration.ofSeconds(30));

            assertThat(held).isEmpty();
            assertThat(calls).hasValue(1);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clear our own interrupt so it does not leak into the next test on this thread.
            Thread.interrupted();
        }
    }

    @Test
    @DisplayName("a backend error propagates immediately instead of being retried")
    void backendErrorPropagates() {
        AtomicInteger calls = new AtomicInteger();
        EntityLocker broken =
                (key, ttl) -> {
                    calls.incrementAndGet();
                    throw new LockAcquisitionException("backend down", null);
                };

        assertThatThrownBy(() -> broken.tryLock("k", TTL, Duration.ofSeconds(5)))
                .isInstanceOf(LockAcquisitionException.class);
        assertThat(calls).hasValue(1);
    }

    @Test
    @DisplayName("a negative maxWait is rejected")
    void negativeMaxWaitRejected() {
        EntityLocker locker = busyFor(0, new AtomicInteger());

        assertThatThrownBy(() -> locker.tryLock("k", TTL, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxWait");
    }

    @Test
    @DisplayName("NOOP grants the handle immediately with a wait budget too")
    void noopHonoursWaitOverload() {
        assertThat(EntityLocker.NOOP.tryLock("k", TTL, Duration.ofSeconds(1))).isPresent();
    }
}
