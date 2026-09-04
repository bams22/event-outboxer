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

import io.github.bams22.outboxer.domain.exception.LockAcquisitionException;
import io.github.bams22.outboxer.domain.exception.LockReleaseException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Distributed business-key lock acquired by the engine before invoking {@code
 * EventHandler.handle(ctx, payload)} when the handler declares a non-null {@code
 * extractLockKey(payload)} (see ADR-0012).
 *
 * <p>Lock keys are arbitrary strings derived by the handler from the payload — for example {@code
 * "order:" + payload.orderId()}. They do NOT have to match anything persisted in the outbox; the
 * lock exists purely to serialize concurrent processing of events that touch the same business
 * aggregate (for example, two mutations on the same order).
 *
 * <p>The engine treats a busy lock as "reschedule and retry later": if {@link #tryLock(String,
 * Duration)} returns {@link Optional#empty()}, the dispatcher marks the event for a short-delay
 * retry and does <em>not</em> invoke the handler. If lock acquisition throws, the engine treats it
 * as a storage failure. With a non-zero per-type {@code lockWait} (ADR-0035) the dispatcher calls
 * {@link #tryLock(String, Duration, Duration)} instead and only takes the busy path once that
 * bounded wait has elapsed.
 *
 * <p>MVP ships four implementations:
 *
 * <ul>
 *   <li>{@link #NOOP} — no-op locker used when no {@code EntityLocker} bean is present and no
 *       handler declares a lock key;
 *   <li>{@code PgLeaseEntityLocker} in {@code event-outboxer-lock-postgres-lease} — lease row in
 *       the {@code entity_locks} table, the PostgreSQL default (ADR-0022);
 *   <li>{@code PgAdvisoryLocker} in {@code event-outboxer-lock-postgres-advisory} — session-scoped
 *       PG advisory locks (opt-out);
 *   <li>{@code RedisEntityLocker} in {@code event-outboxer-lock-redis} — {@code SET NX PX} on
 *       KeyDB/Redis with a fencing token.
 * </ul>
 *
 * <p>Implementations must be thread-safe.
 */
public interface EntityLocker {

    /**
     * Attempt to acquire the lock named {@code key} with the given {@code ttl}. Returns a {@link
     * LockHandle} on success or {@link Optional#empty()} if the lock is currently held by someone
     * else.
     *
     * <p>{@code ttl} is a safety timeout used by implementations that support TTLs (Redis {@code
     * SET PX}, for example) so that a dead process cannot hold the lock forever. Implementations
     * without intrinsic TTL support (PG session-scoped advisory locks, for example) still honour
     * the {@code ttl} contract by relying on handler-level timeouts — see the adapter's
     * documentation.
     *
     * <p>Exclusion guarantees differ per backend (ADR-0012 amendment): TTL-honouring lockers
     * release the lock at {@code min(close, ttl)} — the engine therefore requires {@code lockTtl >=
     * handlerMaxRuntime} so a legitimate handler can never outlive its own lock; session-scoped
     * lockers hold until close or connection loss, at the price of one pooled connection per held
     * lock.
     *
     * @throws LockAcquisitionException if the locker backend is unreachable or returns an error
     *     distinct from "lock is busy"
     */
    Optional<LockHandle> tryLock(String key, Duration ttl);

    /**
     * Like {@link #tryLock(String, Duration)}, but keeps trying for up to {@code maxWait} before
     * giving up (ADR-0035, bounded wait for a busy entity lock).
     *
     * <p>Contract, identical for the default implementation and for adapter overrides:
     *
     * <ul>
     *   <li>{@code maxWait} of zero is exactly one attempt — equivalent to {@link #tryLock(String,
     *       Duration)}; the dispatcher relies on this to keep {@code lockWait: 0} byte-for-byte the
     *       pre-ADR-0035 behaviour.
     *   <li>The first attempt is made immediately; a busy key is retried until the lock is obtained
     *       or {@code maxWait} has elapsed, measured from the call. The total time spent is bounded
     *       by {@code maxWait} plus the duration of one attempt.
     *   <li>An interrupt of the calling thread ends the wait: the method returns {@link
     *       Optional#empty()} with the thread's interrupt status <em>preserved</em>, so the caller
     *       can tell a watchdog cancellation or an executor shutdown from a plain timeout. The
     *       polling default returns at the next probe boundary; an adapter that blocks natively in
     *       a backend call a platform thread cannot interrupt returns when {@code maxWait} elapses
     *       at the latest.
     *   <li>A {@link LockAcquisitionException} from the backend propagates immediately — a broken
     *       backend is not retried inside the wait.
     *   <li>No fairness among waiters is promised; none of the shipped backends offers it, and
     *       per-key ordering is not part of the outbox contract.
     * </ul>
     *
     * <p>The default implementation polls {@link #tryLock(String, Duration)} with a short
     * exponential back-off (2 ms doubling to a 10 ms ceiling, the last sleep clipped to the
     * remaining budget) via {@link Thread#sleep(Duration)}, which parks a virtual thread without
     * pinning. Every existing adapter — and every third-party one — gets the wait for free;
     * adapters whose backend can block natively (a PostgreSQL advisory lock under a statement
     * timeout, for example) override it.
     *
     * @throws LockAcquisitionException if the locker backend is unreachable or returns an error
     *     distinct from "lock is busy"
     * @throws IllegalArgumentException if {@code maxWait} is negative
     */
    default Optional<LockHandle> tryLock(String key, Duration ttl, Duration maxWait) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        Objects.requireNonNull(maxWait, "maxWait must not be null");
        return PollingLockWait.tryLock(this, key, ttl, maxWait);
    }

    /** No-op {@code EntityLocker} used when no handler declares a lock key. */
    EntityLocker NOOP = new NoopEntityLocker();

    /**
     * Handle to an acquired lock. Callers must release the lock via {@link #close()} in a {@code
     * try-with-resources} block; the engine does this automatically after the handler returns or
     * throws.
     *
     * <p>{@link #close()} is idempotent: double-release must not throw.
     */
    interface LockHandle extends AutoCloseable {

        /**
         * Release the lock. Must be idempotent — releasing a handle more than once is a no-op.
         *
         * @throws LockReleaseException if the locker backend refuses the release (for example, the
         *     fencing token mismatched)
         */
        @Override
        void close();
    }
}
