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
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
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

    private static final String UNLOCK_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "  return redis.call('del', KEYS[1]) "
                    + "else "
                    + "  return 0 "
                    + "end";

    private final RedisCommands<String, String> commands;
    private final String keyPrefix;

    public RedisEntityLocker(StatefulRedisConnection<String, String> connection) {
        this(connection, DEFAULT_KEY_PREFIX);
    }

    public RedisEntityLocker(StatefulRedisConnection<String, String> connection, String keyPrefix) {
        Objects.requireNonNull(connection, "connection must not be null");
        this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix must not be null");
        this.commands = connection.sync();
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
        return Optional.of(new RedisLockHandle(namespacedKey, token));
    }

    private final class RedisLockHandle implements LockHandle {

        private final String namespacedKey;
        private final String token;
        private volatile boolean closed;

        RedisLockHandle(String namespacedKey, String token) {
            this.namespacedKey = namespacedKey;
            this.token = token;
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
                                token);
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
