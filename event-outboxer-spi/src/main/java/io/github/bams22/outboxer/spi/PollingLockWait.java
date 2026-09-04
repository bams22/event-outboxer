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
import java.util.Optional;

/**
 * The polling wait loop behind the default {@link EntityLocker#tryLock(String, Duration, Duration)}
 * (ADR-0035). Package-private: adapters that can block natively override the interface method
 * instead of calling this.
 *
 * <p>Cadence: the first attempt is immediate; each busy attempt is followed by a sleep that starts
 * at {@link #INITIAL_BACKOFF} and doubles up to {@link #MAX_BACKOFF}, clipped to whatever is left
 * of {@code maxWait}. A 100 ms budget therefore costs at most eleven attempts, a 20 ms budget five.
 * The loop checks the deadline before every sleep, never after: once the budget is spent the answer
 * is the last attempt's, so the total time is bounded by {@code maxWait} plus one attempt.
 *
 * <p>Interrupts end the wait through {@link InterruptedException} from {@link
 * Thread#sleep(Duration)}; the flag is put back before returning so the caller sees it.
 */
final class PollingLockWait {

    static final Duration INITIAL_BACKOFF = Duration.ofMillis(2);
    static final Duration MAX_BACKOFF = Duration.ofMillis(10);

    private PollingLockWait() {}

    static Optional<LockHandle> tryLock(
            EntityLocker locker, String key, Duration ttl, Duration maxWait) {
        if (maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait must not be negative, got " + maxWait);
        }
        Optional<LockHandle> held = locker.tryLock(key, ttl);
        if (held.isPresent() || maxWait.isZero()) {
            return held;
        }
        long deadline = System.nanoTime() + maxWait.toNanos();
        long backoff = INITIAL_BACKOFF.toNanos();
        long ceiling = MAX_BACKOFF.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return Optional.empty();
            }
            try {
                Thread.sleep(Duration.ofNanos(Math.min(backoff, remaining)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
            held = locker.tryLock(key, ttl);
            if (held.isPresent()) {
                return held;
            }
            backoff = Math.min(backoff * 2, ceiling);
        }
    }
}
