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

import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jspecify.annotations.Nullable;

/**
 * Support for {@link EntityLocker} adapters that end the bounded wait of {@link
 * EntityLocker#tryLock(String, Duration, Duration)} on a release notification from the backend
 * instead of polling (ADR-0035): a registry of threads waiting per topic, and the wait loop that
 * uses it. The Redis locker keys topics by pub/sub channel (one per lock key), the PostgreSQL lease
 * locker by the lock key carried in a {@code NOTIFY} payload on one shared channel.
 *
 * <p>Subscriptions are shared: the first waiter of a topic subscribes through the {@link Transport}
 * (synchronously, so that a release published after the subscription is acknowledged can no longer
 * be missed), later waiters join, and the last one to leave unsubscribes. The unsubscribe is issued
 * while the registry entry is being removed, so a waiter registering right after it observes a
 * fresh entry and re-subscribes behind the unsubscribe on the same connection — the wire order
 * keeps the final state correct. Backends with a single subscription for every key pass {@link
 * Transport#NONE}.
 *
 * <p>Lost wake-ups are ruled out by a generation counter: a waiter reads the generation, probes the
 * lock, and then waits for the generation to move past what it read. A notification that lands
 * between the probe and the wait has already moved the counter, and the wait returns at once.
 *
 * <p>Notifications are at-most-once on every backend, so {@link #tryLockWithWakeup} still re-probes
 * every {@code fallbackProbeInterval}; a backend that lost its connection should call {@link
 * #wakeAll()} once reconnected so that parked waiters re-probe immediately.
 */
public final class LockWaiters {

    /** The subscription side of the backend, reduced to what the registry needs. */
    public interface Transport {

        /** Subscribe and return once the backend has acknowledged the subscription. */
        void subscribe(String topic);

        /** Unsubscribe; may be asynchronous. */
        void unsubscribe(String topic);

        /** For backends with one subscription covering every topic. */
        Transport NONE =
                new Transport() {
                    @Override
                    public void subscribe(String topic) {}

                    @Override
                    public void unsubscribe(String topic) {}
                };
    }

    private final Transport transport;
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    public LockWaiters(Transport transport) {
        this.transport = Objects.requireNonNull(transport, "transport must not be null");
    }

    /**
     * The wait loop shared by notification-backed adapters: probe, park until woken or until the
     * fallback interval elapses, probe again, bounded by {@code maxWait} from the call.
     *
     * <p>Contract as in {@link EntityLocker#tryLock(String, Duration, Duration)}: the first attempt
     * is immediate, a zero {@code maxWait} is that single attempt, an interrupt ends the wait with
     * the flag preserved, backend errors propagate. A {@link Transport#subscribe} failure
     * propagates too — the adapter decides whether to fall back to polling.
     *
     * @param locker the adapter's non-blocking {@code tryLock}
     * @param topic registry topic of {@code key} (channel name, or the key itself)
     * @param fallbackProbeInterval longest park before a waiter re-probes on its own
     */
    public Optional<LockHandle> tryLockWithWakeup(
            EntityLocker locker,
            String key,
            Duration ttl,
            Duration maxWait,
            String topic,
            Duration fallbackProbeInterval) {
        Objects.requireNonNull(locker, "locker must not be null");
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        Objects.requireNonNull(maxWait, "maxWait must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(fallbackProbeInterval, "fallbackProbeInterval must not be null");
        if (maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait must not be negative, got " + maxWait);
        }
        if (fallbackProbeInterval.isNegative() || fallbackProbeInterval.isZero()) {
            throw new IllegalArgumentException(
                    "fallbackProbeInterval must be positive, got " + fallbackProbeInterval);
        }
        Optional<LockHandle> held = locker.tryLock(key, ttl);
        if (held.isPresent() || maxWait.isZero()) {
            return held;
        }
        long deadline = System.nanoTime() + maxWait.toNanos();
        Ticket ticket = register(topic);
        try {
            while (true) {
                // Read the generation before the probe: a release that lands between the probe
                // and the park has moved it, and awaitNewer returns at once.
                long seen = ticket.generation();
                held = locker.tryLock(key, ttl);
                if (held.isPresent()) {
                    return held;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return Optional.empty();
                }
                try {
                    ticket.awaitNewer(
                            seen,
                            Duration.ofNanos(Math.min(remaining, fallbackProbeInterval.toNanos())));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            }
        } finally {
            unregister(ticket);
        }
    }

    /**
     * Register the calling thread as a waiter on {@code topic}, subscribing on the first waiter.
     * Returns once the subscription is acknowledged, so a probe made afterwards cannot miss a
     * release published later.
     *
     * @throws RuntimeException whatever the transport threw on subscribe; the registration is
     *     undone first
     */
    public Ticket register(String topic) {
        Objects.requireNonNull(topic, "topic must not be null");
        Entry entry =
                entries.compute(
                        topic,
                        (t, existing) -> {
                            Entry e = existing != null ? existing : new Entry();
                            e.waiters++;
                            return e;
                        });
        try {
            entry.ensureSubscribed(transport, topic);
        } catch (RuntimeException ex) {
            leave(topic);
            throw ex;
        }
        return new Ticket(entry, topic);
    }

    /** Drop a registration; the last waiter of a topic unsubscribes. */
    public void unregister(Ticket ticket) {
        leave(ticket.topic);
    }

    /** Called by the backend listener when a release was notified on {@code topic}. */
    public void wake(String topic) {
        Entry entry = entries.get(topic);
        if (entry != null) {
            entry.bump();
        }
    }

    /** Wake every parked waiter — after a reconnect, when notifications may have been lost. */
    public void wakeAll() {
        entries.values().forEach(Entry::bump);
    }

    /** Number of topics with at least one waiter — diagnostics and tests. */
    public int waitingTopics() {
        return entries.size();
    }

    private void leave(String topic) {
        entries.computeIfPresent(
                topic,
                (t, e) -> {
                    e.waiters--;
                    if (e.waiters > 0) {
                        return e;
                    }
                    // Issued under the map's bucket lock: a registration racing with this removal
                    // creates a fresh entry and subscribes only after we return, i.e. after this
                    // unsubscribe was queued on the connection.
                    try {
                        transport.unsubscribe(topic);
                    } catch (RuntimeException ex) {
                        // A broken transport: the next registration will re-subscribe or fail on
                        // its own; there is nothing left to clean up here.
                    }
                    return null;
                });
    }

    /** Per-topic state: subscription hand-off, waiter count and the wake-up generation. */
    private static final class Entry {

        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private volatile long generation;
        private int waiters;
        private @Nullable CompletableFuture<Void> subscribed;

        void ensureSubscribed(Transport transport, String topic) {
            CompletableFuture<Void> future;
            boolean owner = false;
            synchronized (this) {
                if (subscribed == null) {
                    subscribed = new CompletableFuture<>();
                    owner = true;
                }
                future = subscribed;
            }
            if (owner) {
                try {
                    transport.subscribe(topic);
                    future.complete(null);
                } catch (RuntimeException ex) {
                    future.completeExceptionally(ex);
                }
            }
            try {
                future.join();
            } catch (CompletionException ex) {
                if (ex.getCause() instanceof RuntimeException cause) {
                    throw cause;
                }
                throw ex;
            }
        }

        void bump() {
            lock.lock();
            try {
                generation++;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /** One waiter's handle: read the generation before a probe, then wait for it to move. */
    public static final class Ticket {

        private final Entry entry;
        private final String topic;

        private Ticket(Entry entry, String topic) {
            this.entry = entry;
            this.topic = topic;
        }

        /** Current wake-up generation; read it before probing the lock. */
        public long generation() {
            return entry.generation;
        }

        /**
         * Park until the generation moves past {@code seen} or {@code timeout} elapses.
         *
         * @return {@code true} on a wake-up, {@code false} on timeout
         * @throws InterruptedException if the thread is interrupted while parked; the caller
         *     restores the flag
         */
        public boolean awaitNewer(long seen, Duration timeout) throws InterruptedException {
            long nanos = timeout.toNanos();
            entry.lock.lockInterruptibly();
            try {
                while (entry.generation == seen) {
                    if (nanos <= 0) {
                        return false;
                    }
                    nanos = entry.changed.awaitNanos(nanos);
                }
                return true;
            } finally {
                entry.lock.unlock();
            }
        }
    }
}
