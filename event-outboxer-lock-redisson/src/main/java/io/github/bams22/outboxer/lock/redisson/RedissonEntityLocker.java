/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.redisson;

import io.github.bams22.outboxer.domain.exception.LockAcquisitionException;
import io.github.bams22.outboxer.domain.exception.LockReleaseException;
import io.github.bams22.outboxer.spi.EntityLocker;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import org.jspecify.annotations.Nullable;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link EntityLocker} over a Redisson {@link RLock} (ADR-0036). For applications that already run
 * Redisson — typically through {@code redisson-spring-boot-starter} — and want the outbox's entity
 * locks on that client: its topologies (single, master-replica, sentinel, cluster), its connection
 * management, and its native pub/sub wait for the bounded lock wait of ADR-0035.
 *
 * <h2>Semantics, and the three Redisson traps this adapter closes</h2>
 *
 * <ul>
 *   <li><b>The watchdog is off.</b> Every acquisition passes an explicit lease time ({@code ttl}),
 *       which disables Redisson's lock watchdog; without it a stuck handler would have its lock
 *       renewed for as long as the JVM lives, the advisory-locker failure mode ADR-0022 moved away
 *       from. The guarantee is therefore the Redis locker's: exclusion holds until {@code
 *       min(close(), ttl)}, a dead JVM's lock frees itself at TTL, no fencing at the resource, and
 *       the engine's {@code lockTtl >= handlerMaxRuntime} rule applies.
 *   <li><b>No re-entrance.</b> An {@code RLock} is reentrant per (client, thread): the thread that
 *       holds a key would get a second handle for it where the SPI promises "busy". The engine
 *       never re-enters — a handler thread holds one lock at a time — but the contract is kept
 *       anyway: the locker remembers, per thread, the keys it holds and answers busy for them
 *       (confirming with {@code isHeldByCurrentThread()} first, so a lease that expired unnoticed
 *       does not shadow the key).
 *   <li><b>Release from any thread, stale release a no-op.</b> Redisson ownership is per (client,
 *       thread), so a plain {@code unlock()} from another thread would fail; the handle carries the
 *       acquiring thread's id and unlocks with it ({@code unlockAsync(threadId)}), which works from
 *       wherever the engine or a caller closes it. After the TTL expired and someone else took the
 *       key, the unlock reports the mismatch and the handle logs it at debug — the Redis locker's
 *       token-checked release, in Redisson terms.
 * </ul>
 *
 * <h2>Bounded wait</h2>
 *
 * {@link #tryLock(String, Duration, Duration)} calls {@code RLock.tryLock(maxWait, ttl)}: Redisson
 * subscribes to the lock's channel and parks until the holder's unlock publishes — no polling, no
 * extra connection to manage. {@code nativeWait = false} falls back to the SPI's polling default.
 * {@code fair = true} uses {@code RFairLock}, which grants waiters in arrival order at the price of
 * extra bookkeeping per acquisition; the outbox contract promises no ordering, so it is off by
 * default.
 *
 * <h2>Keys</h2>
 *
 * {@code <keyPrefix><key>}, default prefix {@link #DEFAULT_KEY_PREFIX} — deliberately different
 * from the Lettuce locker's {@code outbox:lock:}: Redisson stores a hash where the Lettuce locker
 * stores a string, so the two must never share keys, and a fleet must not mix the two lockers
 * (their holders would not exclude each other). Redis Cluster users can add a hash tag to the
 * prefix.
 *
 * <h2>Construction</h2>
 *
 * {@code RedissonEntityLocker.builder()}. Required: {@code client}. Defaulted: {@code keyPrefix}
 * ({@link #DEFAULT_KEY_PREFIX}), {@code fair} ({@code false}), {@code nativeWait} ({@code true}).
 * The locker does not own the client.
 */
public final class RedissonEntityLocker implements EntityLocker {

    private static final Logger log = LoggerFactory.getLogger(RedissonEntityLocker.class);

    /** Default key prefix; distinct from the Lettuce locker's on purpose (different value type). */
    public static final String DEFAULT_KEY_PREFIX = "outbox:rlock:";

    private final RedissonClient client;
    private final String keyPrefix;
    private final boolean fair;
    private final boolean nativeWait;

    /** Keys the current thread holds through this locker — the re-entrance guard. */
    private final ThreadLocal<Set<String>> heldByThread = ThreadLocal.withInitial(HashSet::new);

    public RedissonEntityLocker(RedissonClient client) {
        this(client, null, false, null);
    }

    /**
     * Builder-backed constructor; parameter names are the builder's method names.
     *
     * @param client the application's Redisson client; required, not owned
     * @param keyPrefix namespace of the lock keys; {@code null} = {@link #DEFAULT_KEY_PREFIX}
     * @param fair use {@code RFairLock} (arrival-order grants); default {@code false}
     * @param nativeWait wait for a busy key inside {@code RLock.tryLock} (pub/sub); {@code null} or
     *     {@code true} = yes, {@code false} = the SPI's polling wait
     */
    @Builder
    private RedissonEntityLocker(
            RedissonClient client,
            @Nullable String keyPrefix,
            boolean fair,
            @Nullable Boolean nativeWait) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.keyPrefix = keyPrefix != null ? keyPrefix : DEFAULT_KEY_PREFIX;
        this.fair = fair;
        this.nativeWait = nativeWait == null || nativeWait;
    }

    /** Redis key of {@code key}'s lock. */
    public String keyFor(String key) {
        return keyPrefix + key;
    }

    public boolean fair() {
        return fair;
    }

    public boolean nativeWait() {
        return nativeWait;
    }

    @Override
    public Optional<LockHandle> tryLock(String key, Duration ttl) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        validateTtl(ttl);
        return acquire(key, ttl, 0);
    }

    @Override
    public Optional<LockHandle> tryLock(String key, Duration ttl, Duration maxWait) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        Objects.requireNonNull(maxWait, "maxWait must not be null");
        validateTtl(ttl);
        if (maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait must not be negative, got " + maxWait);
        }
        if (!nativeWait || maxWait.isZero() || heldByThread.get().contains(key)) {
            // The last case is the re-entrance guard: Redisson's own wait would return at once
            // for a key this thread holds, so spend the budget in the SPI's polling loop instead,
            // where each probe answers busy until the holder (possibly another thread closing
            // this thread's handle) lets go.
            return EntityLocker.super.tryLock(key, ttl, maxWait);
        }
        return acquire(key, ttl, maxWait.toMillis());
    }

    private Optional<LockHandle> acquire(String key, Duration ttl, long waitMillis) {
        RLock lock = fair ? client.getFairLock(keyFor(key)) : client.getLock(keyFor(key));
        Set<String> mine = heldByThread.get();
        boolean acquired;
        try {
            if (mine.contains(key)) {
                // Redisson would re-enter; the SPI says busy. Confirm first: the lease may have
                // expired unnoticed, in which case the key is simply free again.
                if (lock.isHeldByCurrentThread()) {
                    return Optional.empty();
                }
                mine.remove(key);
            }
            // An explicit lease time switches Redisson's watchdog off: the lock expires at ttl
            // whatever the handler does, which is the guarantee the engine is built on.
            acquired = lock.tryLock(waitMillis, ttl.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (RuntimeException ex) {
            throw new LockAcquisitionException("redisson tryLock failed for key '" + key + "'", ex);
        }
        if (!acquired) {
            return Optional.empty();
        }
        mine.add(key);
        return Optional.of(new RedissonLockHandle(lock, key, Thread.currentThread(), mine));
    }

    private static void validateTtl(Duration ttl) {
        if (ttl.compareTo(Duration.ofMillis(1)) < 0) {
            throw new IllegalArgumentException("ttl must be >= 1ms, got " + ttl);
        }
    }

    private static final class RedissonLockHandle implements LockHandle {

        private final RLock lock;
        private final String key;
        private final Thread owner;
        private final long ownerThreadId;
        private final Set<String> ownerHeldKeys;
        private volatile boolean closed;

        RedissonLockHandle(RLock lock, String key, Thread owner, Set<String> ownerHeldKeys) {
            this.lock = lock;
            this.key = key;
            this.owner = owner;
            this.ownerThreadId = owner.threadId();
            this.ownerHeldKeys = ownerHeldKeys;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (Thread.currentThread() == owner) {
                // The guard set is thread-confined; only its owner may touch it. A close from
                // another thread leaves the entry, which the owner's next acquire clears after
                // confirming with Redis.
                ownerHeldKeys.remove(key);
            }
            try {
                // Unlock as the acquiring thread, from wherever this runs.
                lock.unlockAsync(ownerThreadId).toCompletableFuture().join();
            } catch (CompletionException ex) {
                if (ex.getCause() instanceof IllegalMonitorStateException) {
                    logStale();
                    return;
                }
                throw new LockReleaseException(
                        "redisson unlock failed for key '" + key + "'",
                        ex.getCause() != null ? ex.getCause() : ex);
            } catch (IllegalMonitorStateException ex) {
                logStale();
            } catch (RuntimeException ex) {
                throw new LockReleaseException("redisson unlock failed for key '" + key + "'", ex);
            }
        }

        private void logStale() {
            // Lease expired before close() arrived, and possibly someone else owns it now.
            log.debug(
                    "redisson lock '{}' was already released (TTL expired) by the time close() ran",
                    key);
        }
    }
}
