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

import java.util.Objects;
import java.util.UUID;

/**
 * Payload of {@link OutboxListener#onLockAcquisitionFailed(LockAcquisitionInfo)} — fired when
 * {@code EntityLocker.tryLock} returned {@code Optional.empty()} (lock currently held by another
 * worker). The engine re-schedules the event with a short delay and retries. Note: a technical
 * failure (for example Redis unreachable) surfaces as {@code StorageErrorInfo} instead.
 *
 * @param eventId identifier of the event whose lock could not be acquired
 * @param eventType event type string
 * @param lockKey the contested lock key
 */
public record LockAcquisitionInfo(UUID eventId, String eventType, String lockKey) {

  public LockAcquisitionInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(lockKey, "lockKey must not be null");
  }
}
