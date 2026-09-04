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

import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The waiter registry behind the notification-backed lockers (Redis pub/sub, PostgreSQL
 * LISTEN/NOTIFY), against a scripted transport: subscription sharing and hand-off, the generation
 * counter that rules out lost wake-ups, timeouts, interrupts, and the shared wait loop.
 */
class LockWaitersTest {

    /** Records subscribe/unsubscribe calls; can be told to fail the next subscribe. */
    private static final class RecordingTransport implements LockWaiters.Transport {
        final List<String> calls = new CopyOnWriteArrayList<>();
        final AtomicBoolean failSubscribe = new AtomicBoolean();

        @Override
        public void subscribe(String channel) {
            calls.add("sub:" + channel);
            if (failSubscribe.get()) {
                throw new IllegalStateException("pub/sub down");
            }
        }

        @Override
        public void unsubscribe(String channel) {
            calls.add("unsub:" + channel);
        }
    }

    @Test
    @DisplayName("the first waiter subscribes, later waiters share, the last one unsubscribes")
    void subscriptionIsSharedAndHandedOff() {
        RecordingTransport transport = new RecordingTransport();
        LockWaiters wakeups = new LockWaiters(transport);

        LockWaiters.Ticket a = wakeups.register("ch");
        LockWaiters.Ticket b = wakeups.register("ch");
        assertThat(transport.calls).containsExactly("sub:ch");
        assertThat(wakeups.waitingTopics()).isEqualTo(1);

        wakeups.unregister(a);
        assertThat(transport.calls).containsExactly("sub:ch");
        wakeups.unregister(b);
        assertThat(transport.calls).containsExactly("sub:ch", "unsub:ch");
        assertThat(wakeups.waitingTopics()).isZero();

        // A fresh registration after the hand-off subscribes again.
        LockWaiters.Ticket c = wakeups.register("ch");
        assertThat(transport.calls).containsExactly("sub:ch", "unsub:ch", "sub:ch");
        wakeups.unregister(c);
    }

    @Test
    @DisplayName("a wake-up published between the probe and the park is not lost")
    void wakeBetweenProbeAndParkIsSeen() throws Exception {
        LockWaiters wakeups = new LockWaiters(new RecordingTransport());
        LockWaiters.Ticket t = wakeups.register("ch");

        long seen = t.generation(); // "before the probe"
        wakeups.wake("ch"); // release lands while we are probing
        assertThat(t.awaitNewer(seen, Duration.ofSeconds(5))).isTrue();
        // Nothing new since: the next wait times out.
        assertThat(t.awaitNewer(t.generation(), Duration.ofMillis(20))).isFalse();
        wakeups.unregister(t);
    }

    @Test
    @DisplayName("a parked waiter wakes when the channel is signalled")
    void parkedWaiterWakes() throws Exception {
        LockWaiters wakeups = new LockWaiters(new RecordingTransport());
        LockWaiters.Ticket t = wakeups.register("ch");
        CountDownLatch parked = new CountDownLatch(1);
        AtomicReference<Boolean> woke = new AtomicReference<>();
        Thread waiter =
                new Thread(
                        () -> {
                            long seen = t.generation();
                            parked.countDown();
                            try {
                                woke.set(t.awaitNewer(seen, Duration.ofSeconds(10)));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
        long start = System.nanoTime();
        waiter.start();
        assertThat(parked.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(30);

        wakeups.wake("ch");
        waiter.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(waiter.isAlive()).isFalse();
        assertThat(woke.get()).isTrue();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
        wakeups.unregister(t);
    }

    @Test
    @DisplayName("a signal on another channel or with no waiters is ignored")
    void unrelatedSignalsAreIgnored() throws Exception {
        LockWaiters wakeups = new LockWaiters(new RecordingTransport());
        wakeups.wake("nobody"); // no entry: must not throw
        LockWaiters.Ticket t = wakeups.register("ch");
        long seen = t.generation();
        wakeups.wake("other");
        assertThat(t.awaitNewer(seen, Duration.ofMillis(20))).isFalse();
        wakeups.unregister(t);
    }

    @Test
    @DisplayName("an interrupt while parked propagates as InterruptedException")
    void interruptWhileParked() throws Exception {
        LockWaiters wakeups = new LockWaiters(new RecordingTransport());
        LockWaiters.Ticket t = wakeups.register("ch");
        CountDownLatch parked = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread waiter =
                new Thread(
                        () -> {
                            long seen = t.generation();
                            parked.countDown();
                            try {
                                t.awaitNewer(seen, Duration.ofSeconds(30));
                            } catch (InterruptedException e) {
                                interrupted.set(true);
                            }
                        });
        waiter.start();
        assertThat(parked.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(30);

        waiter.interrupt();
        waiter.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(waiter.isAlive()).isFalse();
        assertThat(interrupted).isTrue();
        wakeups.unregister(t);
    }

    @Test
    @DisplayName("a failed subscribe undoes the registration and propagates")
    void failedSubscribeLeavesNoEntry() {
        RecordingTransport transport = new RecordingTransport();
        LockWaiters wakeups = new LockWaiters(transport);
        transport.failSubscribe.set(true);

        assertThatThrownBy(() -> wakeups.register("ch"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("pub/sub down");
        assertThat(wakeups.waitingTopics()).isZero();

        // The next attempt subscribes afresh.
        transport.failSubscribe.set(false);
        LockWaiters.Ticket t = wakeups.register("ch");
        assertThat(wakeups.waitingTopics()).isEqualTo(1);
        wakeups.unregister(t);
    }

    // ==================== the shared wait loop ====================

    private static final LockHandle HANDLE = () -> {};

    @Test
    @DisplayName("loop: a wake-up ends the park and the next probe takes the freed key")
    void loop_wakeEndsPark() throws Exception {
        LockWaiters waiters = new LockWaiters(new RecordingTransport());
        AtomicBoolean free = new AtomicBoolean(false);
        AtomicInteger probes = new AtomicInteger();
        EntityLocker locker =
                (key, ttl) -> {
                    probes.incrementAndGet();
                    return free.get() ? Optional.of(HANDLE) : Optional.empty();
                };
        AtomicReference<Optional<LockHandle>> result = new AtomicReference<>();
        Thread waiter =
                new Thread(
                        () ->
                                result.set(
                                        waiters.tryLockWithWakeup(
                                                locker,
                                                "k",
                                                Duration.ofSeconds(30),
                                                Duration.ofSeconds(30),
                                                "topic:k",
                                                Duration.ofSeconds(10))));
        long start = System.nanoTime();
        waiter.start();
        Thread.sleep(50);
        assertThat(waiters.waitingTopics()).isEqualTo(1);

        free.set(true);
        waiters.wake("topic:k");
        waiter.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(waiter.isAlive()).isFalse();
        assertThat(result.get()).isPresent();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
        assertThat(probes.get()).isBetween(2, 4); // initial, after subscribe, after wake
        assertThat(waiters.waitingTopics()).isZero();
    }

    @Test
    @DisplayName("loop: without a wake-up the fallback probe finds the freed key")
    void loop_fallbackProbeFindsFreedKey() {
        LockWaiters waiters = new LockWaiters(new RecordingTransport());
        AtomicInteger probes = new AtomicInteger();
        EntityLocker freeOnFourthProbe =
                (key, ttl) ->
                        probes.incrementAndGet() >= 4 ? Optional.of(HANDLE) : Optional.empty();
        long start = System.nanoTime();

        Optional<LockHandle> held =
                waiters.tryLockWithWakeup(
                        freeOnFourthProbe,
                        "k",
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(5),
                        "topic:k",
                        Duration.ofMillis(20));

        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        assertThat(held).isPresent();
        assertThat(probes).hasValue(4);
        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(40));
        assertThat(elapsed).isLessThan(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("loop: zero maxWait is a single probe, a spent budget yields empty")
    void loop_zeroWaitAndDeadline() {
        LockWaiters waiters = new LockWaiters(new RecordingTransport());
        AtomicInteger probes = new AtomicInteger();
        EntityLocker busy =
                (key, ttl) -> {
                    probes.incrementAndGet();
                    return Optional.empty();
                };

        assertThat(
                        waiters.tryLockWithWakeup(
                                busy,
                                "k",
                                Duration.ofSeconds(30),
                                Duration.ZERO,
                                "t",
                                Duration.ofMillis(20)))
                .isEmpty();
        assertThat(probes).hasValue(1);

        long start = System.nanoTime();
        assertThat(
                        waiters.tryLockWithWakeup(
                                busy,
                                "k",
                                Duration.ofSeconds(30),
                                Duration.ofMillis(60),
                                "t",
                                Duration.ofMillis(20)))
                .isEmpty();
        assertThat(Duration.ofNanos(System.nanoTime() - start))
                .isGreaterThanOrEqualTo(Duration.ofMillis(60));
        assertThat(waiters.waitingTopics()).isZero();
    }

    @Test
    @DisplayName("loop: an interrupt while parked returns empty with the flag preserved")
    void loop_interruptPreservesFlag() throws Exception {
        LockWaiters waiters = new LockWaiters(new RecordingTransport());
        EntityLocker busy = (key, ttl) -> Optional.empty();
        AtomicReference<Optional<LockHandle>> result = new AtomicReference<>();
        AtomicBoolean flag = new AtomicBoolean();
        Thread waiter =
                new Thread(
                        () -> {
                            result.set(
                                    waiters.tryLockWithWakeup(
                                            busy,
                                            "k",
                                            Duration.ofSeconds(30),
                                            Duration.ofSeconds(30),
                                            "t",
                                            Duration.ofSeconds(10)));
                            flag.set(Thread.currentThread().isInterrupted());
                        });
        waiter.start();
        Thread.sleep(50);

        waiter.interrupt();
        waiter.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(waiter.isAlive()).isFalse();
        assertThat(result.get()).isEmpty();
        assertThat(flag).isTrue();
        assertThat(waiters.waitingTopics()).isZero();
    }

    @Test
    @DisplayName("wakeAll() releases every parked waiter, whatever its topic")
    void wakeAllReleasesEveryTopic() throws Exception {
        LockWaiters waiters = new LockWaiters(new RecordingTransport());
        LockWaiters.Ticket a = waiters.register("a");
        LockWaiters.Ticket b = waiters.register("b");
        long seenA = a.generation();
        long seenB = b.generation();

        waiters.wakeAll();

        assertThat(a.awaitNewer(seenA, Duration.ofSeconds(1))).isTrue();
        assertThat(b.awaitNewer(seenB, Duration.ofSeconds(1))).isTrue();
        waiters.unregister(a);
        waiters.unregister(b);
    }
}
