/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.observer;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Payload of {@link OutboxListener#onLockAcquisitionFailed(LockAcquisitionInfo)} — fired when
 * {@code EntityLocker.tryLock} did not yield a lock. Two distinct situations share this callback,
 * discriminated by {@link #outcome()}: the normal busy path ({@code Optional.empty()} — the key is
 * held by another worker) and a technical failure (the locker backend threw, for example Redis
 * unreachable). In both cases the engine releases the event back to {@code PENDING} with a short
 * delay without consuming its retry budget.
 *
 * <p>With a non-zero per-type {@code lockWait} (ADR-0035) the dispatcher waits for the key before
 * giving up; {@link #waited()} is how long that took, so {@link Outcome#BUSY} means "still busy
 * after the bounded wait". The successful counterpart is {@link
 * OutboxListener#onLockAcquired(LockAcquiredInfo)}.
 *
 * @param eventId identifier of the event whose lock could not be acquired
 * @param eventType event type string
 * @param lockKey the contested lock key
 * @param outcome whether the lock was busy or the locker backend failed
 * @param waited wall time spent in {@code EntityLocker.tryLock(...)} before the outcome — the whole
 *     bounded wait for {@link Outcome#BUSY}; a single attempt when {@code lockWait} is zero
 * @param cause the backend exception for {@link Outcome#ERROR}, always null for {@link
 *     Outcome#BUSY}
 */
public record LockAcquisitionInfo(
        UUID eventId,
        String eventType,
        String lockKey,
        Outcome outcome,
        Duration waited,
        @Nullable Throwable cause) {

    public LockAcquisitionInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(lockKey, "lockKey must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        Objects.requireNonNull(waited, "waited must not be null");
    }

    /** Why the lock acquisition did not yield a lock. Bounded set, safe as a metric tag. */
    public enum Outcome {
        /**
         * The lock key is held by another worker — the normal contention path. With a non-zero
         * {@code lockWait} it means the key stayed busy for the whole bounded wait.
         */
        BUSY,
        /**
         * The locker backend threw while trying to acquire — a technical failure, not contention.
         */
        ERROR
    }
}
