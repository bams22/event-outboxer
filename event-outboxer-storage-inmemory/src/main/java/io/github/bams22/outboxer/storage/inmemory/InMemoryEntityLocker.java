/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.inmemory;

import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-process {@link EntityLocker} backed by a {@link ConcurrentHashMap}. Holds each lock until
 * either {@link LockHandle#close()} is called or the TTL elapses — whichever comes first.
 *
 * <p>Each acquired lock carries a random fencing token so that {@link LockHandle#close()} only
 * releases the slot if the same token is still in place. This matches the semantics of the Redis
 * adapter and protects against the following sequence: holder A acquires, A's handler stalls past
 * TTL, locker reaps, holder B acquires, then A's late close runs. Without the token A would release
 * B's lock; with the token A's close is a no-op.
 */
public final class InMemoryEntityLocker implements EntityLocker {

    private final ConcurrentMap<String, Slot> slots = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemoryEntityLocker() {
        this(Clock.system());
    }

    public InMemoryEntityLocker(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public Optional<LockHandle> tryLock(String key, Duration ttl) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        Instant now = clock.now();
        Instant expiry = now.plus(ttl);
        String token = UUID.randomUUID().toString();
        Slot candidate = new Slot(token, expiry);

        Slot existing = slots.putIfAbsent(key, candidate);
        if (existing == null) {
            return Optional.of(new Handle(key, token));
        }
        if (existing.expiry.isBefore(now) && slots.replace(key, existing, candidate)) {
            return Optional.of(new Handle(key, token));
        }
        return Optional.empty();
    }

    private void releaseIfOwned(String key, String token) {
        slots.computeIfPresent(key, (k, cur) -> cur.token.equals(token) ? null : cur);
    }

    private record Slot(String token, Instant expiry) {}

    private final class Handle implements LockHandle {

        private final String key;
        private final String token;
        private volatile boolean closed;

        Handle(String key, String token) {
            this.key = key;
            this.token = token;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            releaseIfOwned(key, token);
        }
    }
}
