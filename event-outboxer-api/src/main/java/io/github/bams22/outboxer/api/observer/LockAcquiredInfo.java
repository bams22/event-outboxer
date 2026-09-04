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

/**
 * Payload of {@link OutboxListener#onLockAcquired(LockAcquiredInfo)} — fired when {@code
 * EntityLocker.tryLock(...)} yielded the lock for an event whose handler declares a lock key, right
 * before the handler runs. Fires for every acquisition, immediate ones included, so that together
 * with {@link LockAcquisitionInfo} it accounts for every attempt: the share of acquisitions with a
 * non-trivial {@link #waited()} is the share that succeeded only thanks to the bounded wait of
 * ADR-0035.
 *
 * @param eventId identifier of the event whose lock was acquired
 * @param eventType event type string
 * @param lockKey the acquired lock key
 * @param waited wall time spent in {@code EntityLocker.tryLock(...)} — close to zero when the key
 *     was free, up to the type's {@code lockWait} when the key was busy first
 */
public record LockAcquiredInfo(UUID eventId, String eventType, String lockKey, Duration waited) {

    public LockAcquiredInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(lockKey, "lockKey must not be null");
        Objects.requireNonNull(waited, "waited must not be null");
    }
}
