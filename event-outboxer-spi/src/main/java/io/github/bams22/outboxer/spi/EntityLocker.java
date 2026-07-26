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
import java.util.Optional;

/**
 * Distributed business-key lock acquired by the engine before invoking {@code
 * EventHandler.handle(ctx, payload)} when the handler declares a non-null {@code
 * extractLockKey(payload)} (see ADR-0012).
 *
 * <p>Lock keys are arbitrary strings derived by the handler from the payload — for example
 * {@code "order:" + payload.orderId()}. They do NOT have to match anything persisted in the
 * outbox; the lock exists purely to serialize concurrent processing of events that touch the same
 * business aggregate (for example, two mutations on the same order).
 *
 * <p>The engine treats a busy lock as "reschedule and retry later": if {@link #tryLock(String,
 * Duration)} returns {@link Optional#empty()}, the dispatcher marks the event for a short-delay
 * retry and does <em>not</em> invoke the handler. If lock acquisition throws, the engine treats it
 * as a storage failure.
 *
 * <p>MVP ships three implementations:
 *
 * <ul>
 *   <li>{@link #NOOP} — no-op locker used when no {@code EntityLocker} bean is present and no
 *       handler declares a lock key;
 *   <li>{@code PgAdvisoryLocker} in {@code event-outboxer-lock-postgres} — PG advisory locks;
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
   * <p>{@code ttl} is a safety timeout used by implementations that support TTLs (Redis {@code SET
   * PX}, for example) so that a dead process cannot hold the lock forever. Implementations without
   * intrinsic TTL support (PG session-scoped advisory locks, for example) still honour the {@code
   * ttl} contract by relying on handler-level timeouts — see the adapter's documentation.
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
