/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Reusable contract specification for every {@link EntityLocker} implementation. Focuses on the
 * semantics the dispatcher relies on: {@link EntityLocker#tryLock(String, Duration)} is
 * non-blocking, busy locks return {@link Optional#empty()}, a released handle makes the lock
 * available again, and the bounded wait of {@link EntityLocker#tryLock(String, Duration, Duration)}
 * (ADR-0035) honours its budget, its interrupt contract and its zero-wait shortcut — whether the
 * adapter inherits the polling default or overrides it natively.
 */
public abstract class AbstractEntityLockerContractTest {

    protected EntityLocker locker;

    protected abstract EntityLocker newLocker();

    /**
     * Opt-in hook for TTL-honouring lockers (ADR-0022). Return {@code true} and implement {@link
     * #forceExpire(String)} to activate the TTL-expiry contract tests; the default keeps them
     * skipped, which preserves the historic contract for lockers whose TTL is best-effort or
     * ignored (PG advisory) and for backends where expiry cannot be forced deterministically.
     */
    protected boolean supportsTtlExpiry() {
        return false;
    }

    /**
     * Force the lease/lock of {@code key} into the expired state, as if its TTL had elapsed —
     * without waiting wall-clock time. Only called when {@link #supportsTtlExpiry()} is {@code
     * true}. Implementations that track both an acquisition and an expiry timestamp must backdate
     * both (the lease table's CHECK requires {@code expires_at > acquired_at}).
     */
    protected void forceExpire(String key) {
        throw new UnsupportedOperationException(
                "forceExpire() must be implemented when supportsTtlExpiry() returns true");
    }

    /**
     * Whether an interrupt ends the bounded wait of {@link EntityLocker#tryLock(String, Duration,
     * Duration)} before {@code maxWait} elapses. {@code true} for the polling default and for any
     * adapter that waits in interruptible Java code; adapters that block natively in a backend call
     * a platform thread cannot interrupt (a PostgreSQL {@code pg_advisory_lock} under a statement
     * timeout) return {@code false}, and the contract then only requires the wait to end by {@code
     * maxWait} with the interrupt status preserved.
     */
    protected boolean interruptEndsWaitEarly() {
        return true;
    }

    @BeforeEach
    void setUpLocker() {
        locker = newLocker();
    }

    @Test
    @DisplayName("tryLock() returns a handle when the key is free")
    void tryLock_free_returnsHandle() {
        try (LockHandle handle = locker.tryLock("key-1", Duration.ofSeconds(30)).orElseThrow()) {
            assertThat(handle).isNotNull();
        }
    }

    @Test
    @DisplayName("tryLock() returns empty when another holder still owns the same key")
    void tryLock_busy_returnsEmpty() {
        try (LockHandle _ = locker.tryLock("busy", Duration.ofSeconds(30)).orElseThrow()) {
            Optional<LockHandle> second = locker.tryLock("busy", Duration.ofSeconds(30));
            assertThat(second).isEmpty();
        }
    }

    @Test
    @DisplayName("tryLock() on a different key succeeds while another key is held")
    void tryLock_independentKeys() {
        try (LockHandle _ = locker.tryLock("a", Duration.ofSeconds(30)).orElseThrow()) {
            Optional<LockHandle> b = locker.tryLock("b", Duration.ofSeconds(30));
            assertThat(b).isPresent();
            b.orElseThrow().close();
        }
    }

    @Test
    @DisplayName("release via close() makes the key available to a subsequent acquirer")
    void close_releasesLock() {
        LockHandle first = locker.tryLock("key-cycle", Duration.ofSeconds(30)).orElseThrow();
        first.close();

        Optional<LockHandle> second = locker.tryLock("key-cycle", Duration.ofSeconds(30));
        assertThat(second).isPresent();
        second.orElseThrow().close();
    }

    @Test
    @DisplayName("close() is idempotent — releasing twice must not throw")
    void close_idempotent() {
        LockHandle handle = locker.tryLock("key-twice", Duration.ofSeconds(30)).orElseThrow();
        handle.close();
        handle.close();
    }

    @Test
    @DisplayName("an expired lock is available to the next acquirer (TTL-honouring lockers only)")
    void ttlExpiry_allowsTakeover() {
        Assumptions.assumeTrue(supportsTtlExpiry(), "locker does not support deterministic expiry");
        LockHandle dead = locker.tryLock("expiring", Duration.ofSeconds(30)).orElseThrow();
        forceExpire("expiring");

        Optional<LockHandle> successor = locker.tryLock("expiring", Duration.ofSeconds(30));
        assertThat(successor).isPresent();
        successor.orElseThrow().close();
        dead.close();
    }

    @Test
    @DisplayName(
            "a stale close() must not release the successor's lock (TTL-honouring lockers only)")
    void ttlExpiry_staleCloseDoesNotReleaseSuccessor() {
        Assumptions.assumeTrue(supportsTtlExpiry(), "locker does not support deterministic expiry");
        LockHandle zombie = locker.tryLock("contested", Duration.ofSeconds(30)).orElseThrow();
        forceExpire("contested");
        LockHandle successor = locker.tryLock("contested", Duration.ofSeconds(30)).orElseThrow();

        // The zombie's token no longer matches the row/key — its close() must be a silent no-op.
        zombie.close();

        Optional<LockHandle> third = locker.tryLock("contested", Duration.ofSeconds(30));
        assertThat(third).as("successor's lock must survive the zombie's stale close()").isEmpty();
        successor.close();
    }

    @Test
    @DisplayName("tryLock() from concurrent threads grants the lock to exactly one holder per key")
    void tryLock_concurrent_exclusivity() throws Exception {
        int threads = 32;
        String key = "hot-key";
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger acquired = new AtomicInteger();
            ConcurrentHashMap.KeySetView<LockHandle, Boolean> handles =
                    ConcurrentHashMap.newKeySet();
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < threads; i++) {
                futures.add(
                        exec.submit(
                                () -> {
                                    try {
                                        go.await();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        return;
                                    }
                                    Optional<LockHandle> h =
                                            locker.tryLock(key, Duration.ofSeconds(30));
                                    h.ifPresent(
                                            handle -> {
                                                acquired.incrementAndGet();
                                                handles.add(handle);
                                            });
                                }));
            }
            go.countDown();
            for (var f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            assertThat(acquired).hasValue(1);
            handles.forEach(LockHandle::close);
        } finally {
            exec.shutdownNow();
        }
    }

    // ==================== Bounded wait (ADR-0035) ====================

    @Test
    @DisplayName("tryLock(maxWait=0) on a busy key returns empty without waiting")
    void wait_zeroBudget_isNonBlocking() {
        try (LockHandle _ = locker.tryLock("w-zero", Duration.ofSeconds(30)).orElseThrow()) {
            long start = System.nanoTime();
            Optional<LockHandle> second =
                    locker.tryLock("w-zero", Duration.ofSeconds(30), Duration.ZERO);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            assertThat(second).isEmpty();
            assertThat(elapsed).isLessThan(Duration.ofSeconds(1));
        }
    }

    @Test
    @DisplayName("tryLock(maxWait) on a free key returns the handle at once")
    void wait_freeKey_isImmediate() {
        Optional<LockHandle> held =
                locker.tryLock("w-free", Duration.ofSeconds(30), Duration.ofSeconds(5));
        assertThat(held).isPresent();
        held.orElseThrow().close();
    }

    @Test
    @DisplayName("tryLock(maxWait) obtains the lock once the holder releases inside the window")
    void wait_holderReleasesInsideWindow_acquires() throws Exception {
        LockHandle holder = locker.tryLock("w-handover", Duration.ofSeconds(30)).orElseThrow();
        Duration holdFor = Duration.ofMillis(80);
        Thread releaser =
                new Thread(
                        () -> {
                            try {
                                Thread.sleep(holdFor);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            holder.close();
                        },
                        "lock-releaser");
        long start = System.nanoTime();
        releaser.start();

        Optional<LockHandle> waiter =
                locker.tryLock("w-handover", Duration.ofSeconds(30), Duration.ofSeconds(10));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        releaser.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(waiter).as("the waiter must get the lock the holder released").isPresent();
        assertThat(elapsed).isGreaterThanOrEqualTo(holdFor);
        assertThat(elapsed)
                .as("must not wait out the whole budget")
                .isLessThan(Duration.ofSeconds(5));
        waiter.orElseThrow().close();
    }

    @Test
    @DisplayName(
            "tryLock(maxWait) gives up once the budget is spent while the holder keeps the key")
    void wait_holderKeepsKey_givesUpAtBudget() {
        Duration budget = Duration.ofMillis(150);
        try (LockHandle _ = locker.tryLock("w-busy", Duration.ofSeconds(30)).orElseThrow()) {
            long start = System.nanoTime();
            Optional<LockHandle> waiter = locker.tryLock("w-busy", Duration.ofSeconds(30), budget);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            assertThat(waiter).isEmpty();
            assertThat(elapsed).isGreaterThanOrEqualTo(budget);
            // Bounded by maxWait plus one attempt; slack for a slow backend round trip.
            assertThat(elapsed).isLessThan(budget.plusSeconds(5));
        }
    }

    @Test
    @DisplayName("an interrupted waiter returns empty with its interrupt flag preserved")
    void wait_interrupted_returnsAndKeepsFlag() throws Exception {
        // A natively blocking adapter returns at maxWait, not at the interrupt: keep it short
        // there.
        Duration budget = interruptEndsWaitEarly() ? Duration.ofSeconds(30) : Duration.ofSeconds(2);
        try (LockHandle _ = locker.tryLock("w-int", Duration.ofSeconds(30)).orElseThrow()) {
            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Optional<LockHandle>> result = new AtomicReference<>();
            AtomicBoolean flagAfterReturn = new AtomicBoolean();
            Thread waiter =
                    new Thread(
                            () -> {
                                started.countDown();
                                result.set(locker.tryLock("w-int", Duration.ofSeconds(30), budget));
                                flagAfterReturn.set(Thread.currentThread().isInterrupted());
                            },
                            "lock-waiter");
            long start = System.nanoTime();
            waiter.start();
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(50);

            waiter.interrupt();
            waiter.join(TimeUnit.SECONDS.toMillis(15));
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            assertThat(waiter.isAlive()).as("waiter must not outlive its budget").isFalse();
            if (interruptEndsWaitEarly()) {
                assertThat(elapsed)
                        .as("interrupt must cut the wait short")
                        .isLessThan(Duration.ofSeconds(5));
            }
            assertThat(result.get()).isEmpty();
            assertThat(flagAfterReturn).as("interrupt status must be preserved").isTrue();
        }
    }
}
