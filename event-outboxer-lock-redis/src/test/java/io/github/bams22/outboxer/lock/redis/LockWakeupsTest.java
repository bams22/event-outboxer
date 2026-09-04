/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The waiter registry behind the Redis locker's pub/sub wake-up, against a scripted transport:
 * subscription sharing and hand-off, the generation counter that rules out lost wake-ups, timeouts
 * and interrupts.
 */
class LockWakeupsTest {

    /** Records subscribe/unsubscribe calls; can be told to fail the next subscribe. */
    private static final class RecordingTransport implements LockWakeups.Transport {
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
        LockWakeups wakeups = new LockWakeups(transport);

        LockWakeups.Ticket a = wakeups.register("ch");
        LockWakeups.Ticket b = wakeups.register("ch");
        assertThat(transport.calls).containsExactly("sub:ch");
        assertThat(wakeups.waitingChannels()).isEqualTo(1);

        wakeups.unregister(a);
        assertThat(transport.calls).containsExactly("sub:ch");
        wakeups.unregister(b);
        assertThat(transport.calls).containsExactly("sub:ch", "unsub:ch");
        assertThat(wakeups.waitingChannels()).isZero();

        // A fresh registration after the hand-off subscribes again.
        LockWakeups.Ticket c = wakeups.register("ch");
        assertThat(transport.calls).containsExactly("sub:ch", "unsub:ch", "sub:ch");
        wakeups.unregister(c);
    }

    @Test
    @DisplayName("a wake-up published between the probe and the park is not lost")
    void wakeBetweenProbeAndParkIsSeen() throws Exception {
        LockWakeups wakeups = new LockWakeups(new RecordingTransport());
        LockWakeups.Ticket t = wakeups.register("ch");

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
        LockWakeups wakeups = new LockWakeups(new RecordingTransport());
        LockWakeups.Ticket t = wakeups.register("ch");
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
        LockWakeups wakeups = new LockWakeups(new RecordingTransport());
        wakeups.wake("nobody"); // no entry: must not throw
        LockWakeups.Ticket t = wakeups.register("ch");
        long seen = t.generation();
        wakeups.wake("other");
        assertThat(t.awaitNewer(seen, Duration.ofMillis(20))).isFalse();
        wakeups.unregister(t);
    }

    @Test
    @DisplayName("an interrupt while parked propagates as InterruptedException")
    void interruptWhileParked() throws Exception {
        LockWakeups wakeups = new LockWakeups(new RecordingTransport());
        LockWakeups.Ticket t = wakeups.register("ch");
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
        LockWakeups wakeups = new LockWakeups(transport);
        transport.failSubscribe.set(true);

        assertThatThrownBy(() -> wakeups.register("ch"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("pub/sub down");
        assertThat(wakeups.waitingChannels()).isZero();

        // The next attempt subscribes afresh.
        transport.failSubscribe.set(false);
        LockWakeups.Ticket t = wakeups.register("ch");
        assertThat(wakeups.waitingChannels()).isEqualTo(1);
        wakeups.unregister(t);
    }
}
