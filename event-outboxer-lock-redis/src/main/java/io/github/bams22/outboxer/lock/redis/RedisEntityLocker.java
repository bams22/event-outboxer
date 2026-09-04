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

import io.github.bams22.outboxer.domain.exception.LockAcquisitionException;
import io.github.bams22.outboxer.domain.exception.LockReleaseException;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.LockWaiters;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import lombok.Builder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link EntityLocker} backed by Redis / KeyDB. Recommended production default for clustered outbox
 * deployments (see ADR-0012 / risk #7).
 *
 * <h2>Acquisition</h2>
 *
 * Implements the "Redlock-style" single-instance recipe: {@code SET key token NX PX ttl}. The
 * {@code token} is a random UUID — different for every {@code tryLock} call — and is used at
 * release time to guarantee that the caller owns the lock it is trying to release.
 *
 * <h2>Release</h2>
 *
 * A Lua script atomically compares the stored value to the caller's token and deletes the key only
 * on match. Without this, holder A whose TTL elapsed would otherwise erase holder B's lock on
 * {@code close()}.
 *
 * <h2>Bounded wait with a pub/sub wake-up (ADR-0035)</h2>
 *
 * The release script also {@code PUBLISH}es the released token on the key's channel ({@code
 * <keyPrefix>released:<key>}). Given a second, pub/sub connection ({@code wakeupConnection}), a
 * waiter in {@link #tryLock(String, Duration, Duration)} subscribes to that channel and parks until
 * the holder releases instead of probing {@code SET NX PX} every few milliseconds — the first
 * waiter of a key subscribes, later ones share the subscription, the last one unsubscribes ({@link
 * LockWaiters}). A parked waiter still re-probes every {@code fallbackProbeInterval} (default
 * {@link #DEFAULT_FALLBACK_PROBE_INTERVAL}) because pub/sub is at-most-once: a message lost across
 * a reconnect, or a key that expired instead of being released, must not cost the whole budget.
 * Without a pub/sub connection the locker keeps the SPI's polling default. The {@code PUBLISH} is
 * issued regardless — it is one cheap command, and the waiters may sit in another JVM.
 *
 * <h2>Construction</h2>
 *
 * {@code RedisEntityLocker.builder()}. Required: {@code connection}. Defaulted when {@code null}:
 * {@code keyPrefix} ({@link #DEFAULT_KEY_PREFIX}), {@code wakeupConnection} (none — polling wait),
 * {@code fallbackProbeInterval} ({@link #DEFAULT_FALLBACK_PROBE_INTERVAL}). The two public
 * constructors cover the pre-wake-up shape. The locker owns neither connection.
 *
 * <h2>TTL — best effort, no fencing (ADR-0012 amendment)</h2>
 *
 * Always honoured; if the handler stalls past the TTL, the lock frees itself and the Lua
 * compare-and-delete becomes a no-op (distinct from the PG advisory adapter, whose TTL is merely
 * documentational). Consequences to understand:
 *
 * <ul>
 *   <li>The TTL is the crash-release mechanism: a dead JVM's lock frees itself after at most {@code
 *       ttl}.
 *   <li>There is no renewal and no fencing token at the protected resource: a zombie handler that
 *       outlives the TTL (and its force-reclaimed claim) can overlap with the next holder. The
 *       engine enforces {@code lockTtl >= handlerMaxRuntime} (default 2x) so this can only happen
 *       to handlers already past their runtime budget — full exclusion under arbitrary stalls
 *       requires fencing at the resource itself and is out of scope.
 * </ul>
 */
public final class RedisEntityLocker implements EntityLocker {

    private static final Logger log = LoggerFactory.getLogger(RedisEntityLocker.class);

    /** Default key prefix — avoids collisions with other Redis tenants. */
    public static final String DEFAULT_KEY_PREFIX = "outbox:lock:";

    /**
     * Default re-probe cadence of a waiter parked on the pub/sub wake-up. Rarely reached: it only
     * matters when a release notification was lost or the key expired instead of being released.
     */
    public static final Duration DEFAULT_FALLBACK_PROBE_INTERVAL = Duration.ofMillis(25);

    /** Channel-name segment between the key prefix and the lock key. */
    static final String CHANNEL_SEGMENT = "released:";

    /** Compare-and-delete, then tell the waiters: ARGV[1] = token, ARGV[2] = channel. */
    private static final String UNLOCK_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "  redis.call('del', KEYS[1]) "
                    + "  redis.call('publish', ARGV[2], ARGV[1]) "
                    + "  return 1 "
                    + "else "
                    + "  return 0 "
                    + "end";

    private final RedisCommands<String, String> commands;
    private final String keyPrefix;
    private final String channelPrefix;
    private final @Nullable LockWaiters wakeups;
    private final Duration fallbackProbeInterval;

    public RedisEntityLocker(StatefulRedisConnection<String, String> connection) {
        this(connection, DEFAULT_KEY_PREFIX, null, null);
    }

    public RedisEntityLocker(StatefulRedisConnection<String, String> connection, String keyPrefix) {
        this(
                connection,
                Objects.requireNonNull(keyPrefix, "keyPrefix must not be null"),
                null,
                null);
    }

    /**
     * Builder-backed constructor; parameter names are the builder's method names.
     *
     * @param connection command connection for acquire and release; required
     * @param keyPrefix namespace of the lock keys; {@code null} = {@link #DEFAULT_KEY_PREFIX}
     * @param wakeupConnection pub/sub connection for release notifications; {@code null} = no
     *     wake-up, the bounded wait polls
     * @param fallbackProbeInterval re-probe cadence of a parked waiter; {@code null} = {@link
     *     #DEFAULT_FALLBACK_PROBE_INTERVAL}
     */
    @Builder
    private RedisEntityLocker(
            StatefulRedisConnection<String, String> connection,
            @Nullable String keyPrefix,
            @Nullable StatefulRedisPubSubConnection<String, String> wakeupConnection,
            @Nullable Duration fallbackProbeInterval) {
        Objects.requireNonNull(connection, "connection must not be null");
        this.keyPrefix = keyPrefix != null ? keyPrefix : DEFAULT_KEY_PREFIX;
        this.channelPrefix = this.keyPrefix + CHANNEL_SEGMENT;
        this.commands = connection.sync();
        this.fallbackProbeInterval =
                fallbackProbeInterval != null
                        ? fallbackProbeInterval
                        : DEFAULT_FALLBACK_PROBE_INTERVAL;
        if (this.fallbackProbeInterval.isNegative() || this.fallbackProbeInterval.isZero()) {
            throw new IllegalArgumentException(
                    "fallbackProbeInterval must be positive, got " + this.fallbackProbeInterval);
        }
        if (wakeupConnection != null) {
            LockWaiters registry = new LockWaiters(new LettuceTransport(wakeupConnection));
            wakeupConnection.addListener(
                    new RedisPubSubAdapter<String, String>() {
                        @Override
                        public void message(String channel, String message) {
                            registry.wake(channel);
                        }
                    });
            this.wakeups = registry;
        } else {
            this.wakeups = null;
        }
    }

    /** Whether waiters park on release notifications ({@code true}) or poll ({@code false}). */
    public boolean wakeupEnabled() {
        return wakeups != null;
    }

    /** Number of lock keys with at least one waiter parked on a notification in this JVM. */
    public int waitingKeys() {
        return wakeups != null ? wakeups.waitingTopics() : 0;
    }

    /** Pub/sub channel the release of {@code key} is published on. */
    public String channelFor(String key) {
        return channelPrefix + key;
    }

    @Override
    public Optional<LockHandle> tryLock(String key, Duration ttl) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be positive, got " + ttl);
        }
        String namespacedKey = keyPrefix + key;
        String token = UUID.randomUUID().toString();
        String result;
        try {
            result = commands.set(namespacedKey, token, SetArgs.Builder.nx().px(ttl.toMillis()));
        } catch (RuntimeException ex) {
            throw new LockAcquisitionException("redis SET NX PX failed for key '" + key + "'", ex);
        }
        if (result == null) {
            return Optional.empty();
        }
        return Optional.of(new RedisLockHandle(namespacedKey, token, channelFor(key)));
    }

    @Override
    public Optional<LockHandle> tryLock(String key, Duration ttl, Duration maxWait) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        Objects.requireNonNull(maxWait, "maxWait must not be null");
        if (maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait must not be negative, got " + maxWait);
        }
        if (wakeups == null || maxWait.isZero()) {
            return EntityLocker.super.tryLock(key, ttl, maxWait);
        }
        long deadline = System.nanoTime() + maxWait.toNanos();
        try {
            return wakeups.tryLockWithWakeup(
                    this, key, ttl, maxWait, channelFor(key), fallbackProbeInterval);
        } catch (RuntimeException ex) {
            if (ex instanceof LockAcquisitionException) {
                throw ex;
            }
            // The subscribe failed (pub/sub connection down): poll for what is left of the budget.
            log.warn(
                    "could not subscribe to lock release notifications for key '{}' — polling for"
                            + " this wait instead: {}",
                    key,
                    ex.toString());
            long remaining = deadline - System.nanoTime();
            return remaining <= 0
                    ? Optional.empty()
                    : EntityLocker.super.tryLock(key, ttl, Duration.ofNanos(remaining));
        }
    }

    /** Lettuce pub/sub commands behind the {@link LockWaiters} registry. */
    private record LettuceTransport(StatefulRedisPubSubConnection<String, String> connection)
            implements LockWaiters.Transport {

        @Override
        public void subscribe(String channel) {
            // Synchronous: returns once the server acknowledged the subscription.
            connection.sync().subscribe(channel);
        }

        @Override
        public void unsubscribe(String channel) {
            connection.async().unsubscribe(channel);
        }
    }

    private final class RedisLockHandle implements LockHandle {

        private final String namespacedKey;
        private final String token;
        private final String channel;
        private volatile boolean closed;

        RedisLockHandle(String namespacedKey, String token, String channel) {
            this.namespacedKey = namespacedKey;
            this.token = token;
            this.channel = channel;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            Long result;
            try {
                result =
                        commands.eval(
                                UNLOCK_SCRIPT,
                                ScriptOutputType.INTEGER,
                                new String[] {namespacedKey},
                                token,
                                channel);
            } catch (RuntimeException ex) {
                throw new LockReleaseException(
                        "redis unlock script failed for key '" + namespacedKey + "'", ex);
            }
            if (result != null && result == 0L) {
                // Key expired before close() arrived, or someone else already owns it now.
                log.debug(
                        "redis lock '{}' was already released (TTL expired) by the time close()"
                                + " ran",
                        namespacedKey);
            }
        }
    }
}
