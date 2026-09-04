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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jspecify.annotations.Nullable;

/**
 * Registry of threads waiting for a lock release notification, keyed by pub/sub channel (one
 * channel per lock key). Backs the wake-up variant of {@link RedisEntityLocker#tryLock(String,
 * Duration, Duration)}: instead of probing {@code SET NX PX} every few milliseconds, a waiter parks
 * until the holder's release script {@code PUBLISH}es on the key's channel, or until a fallback
 * interval elapses.
 *
 * <p>Subscriptions are shared: the first waiter of a channel subscribes (synchronously, so that a
 * release published after the subscription is acknowledged can no longer be missed), later waiters
 * join, and the last one to leave unsubscribes. The unsubscribe is issued while the registry entry
 * is being removed, so a waiter registering right after it observes a fresh entry and re-subscribes
 * behind the unsubscribe on the same connection — the wire order keeps the final state correct.
 *
 * <p>Lost wake-ups are ruled out by a generation counter: a waiter reads the generation, probes the
 * lock, and then waits for the generation to move past what it read. A notification that lands
 * between the probe and the wait has already moved the counter, and the wait returns at once.
 *
 * <p>The transport is abstracted so the registry is unit-testable without Redis.
 */
final class LockWakeups {

    /** The pub/sub side of the connection, reduced to what the registry needs. */
    interface Transport {

        /** Subscribe and return once the server has acknowledged the subscription. */
        void subscribe(String channel);

        /** Unsubscribe; may be asynchronous. */
        void unsubscribe(String channel);
    }

    private final Transport transport;
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    LockWakeups(Transport transport) {
        this.transport = Objects.requireNonNull(transport, "transport must not be null");
    }

    /**
     * Register the calling thread as a waiter on {@code channel}, subscribing on the first waiter.
     * Returns once the subscription is acknowledged, so a probe made afterwards cannot miss a
     * release published later.
     *
     * @throws RuntimeException whatever the transport threw on subscribe; the registration is
     *     undone first
     */
    Ticket register(String channel) {
        Entry entry =
                entries.compute(
                        channel,
                        (c, existing) -> {
                            Entry e = existing != null ? existing : new Entry();
                            e.waiters++;
                            return e;
                        });
        try {
            entry.ensureSubscribed(transport, channel);
        } catch (RuntimeException ex) {
            leave(channel);
            throw ex;
        }
        return new Ticket(entry, channel);
    }

    /** Drop a registration; the last waiter of a channel unsubscribes. */
    void unregister(Ticket ticket) {
        leave(ticket.channel);
    }

    /** Called from the pub/sub listener when a release was published on {@code channel}. */
    void wake(String channel) {
        Entry entry = entries.get(channel);
        if (entry != null) {
            entry.bump();
        }
    }

    /** Number of channels with at least one waiter — diagnostics and tests. */
    int waitingChannels() {
        return entries.size();
    }

    private void leave(String channel) {
        entries.computeIfPresent(
                channel,
                (c, e) -> {
                    e.waiters--;
                    if (e.waiters > 0) {
                        return e;
                    }
                    // Issued under the map's bucket lock: a registration racing with this removal
                    // creates a fresh entry and subscribes only after we return, i.e. after this
                    // unsubscribe was queued on the connection.
                    try {
                        transport.unsubscribe(channel);
                    } catch (RuntimeException ex) {
                        // A broken transport: the next registration will re-subscribe or fail on
                        // its own; there is nothing left to clean up here.
                    }
                    return null;
                });
    }

    /** Per-channel state: subscription hand-off, waiter count and the wake-up generation. */
    private static final class Entry {

        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private volatile long generation;
        private int waiters;
        private @Nullable CompletableFuture<Void> subscribed;

        void ensureSubscribed(Transport transport, String channel) {
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
                    transport.subscribe(channel);
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
    static final class Ticket {

        private final Entry entry;
        private final String channel;

        private Ticket(Entry entry, String channel) {
            this.entry = entry;
            this.channel = channel;
        }

        /** Current wake-up generation; read it before probing the lock. */
        long generation() {
            return entry.generation;
        }

        /**
         * Park until the generation moves past {@code seen} or {@code timeout} elapses.
         *
         * @return {@code true} on a wake-up, {@code false} on timeout
         * @throws InterruptedException if the thread is interrupted while parked; the caller
         *     restores the flag
         */
        boolean awaitNewer(long seen, Duration timeout) throws InterruptedException {
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
