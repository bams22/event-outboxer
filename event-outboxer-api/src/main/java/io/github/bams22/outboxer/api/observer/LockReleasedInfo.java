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
 * Payload of {@link OutboxListener#onLockReleased(LockReleasedInfo)} — fired when the engine
 * released an entity lock after the handler and its finalize, i.e. when {@code LockHandle.close()}
 * returned normally. {@link #held()} is the lock's hold time from acquisition to release — handler
 * plus finalize — the number that sizes the per-type {@code lock-wait} (ADR-0035) and {@code
 * lock-ttl}: a wait shorter than the typical hold rarely pays off, a TTL not comfortably above the
 * longest hold risks losing exclusion.
 *
 * @param eventId identifier of the event whose lock was released
 * @param eventType event type string
 * @param lockKey the released lock key
 * @param held wall time between acquisition and release
 */
public record LockReleasedInfo(UUID eventId, String eventType, String lockKey, Duration held) {

    public LockReleasedInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(lockKey, "lockKey must not be null");
        Objects.requireNonNull(held, "held must not be null");
    }
}
