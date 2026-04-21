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
 * Payload of {@link OutboxListener#onLockReleaseFailed(LockReleaseInfo)} — fired when
 * {@code LockHandle.close()} swallowed a release error. The lock is effectively orphaned and
 * will release when its TTL expires (Redis locks) or when the owning session ends (PostgreSQL
 * advisory locks).
 *
 * @param eventId identifier of the event whose lock release failed
 * @param eventType event type string
 * @param lockKey the lock key that could not be released
 * @param cause underlying exception
 */
public record LockReleaseInfo(UUID eventId, String eventType, String lockKey, Throwable cause) {

  public LockReleaseInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(lockKey, "lockKey must not be null");
    Objects.requireNonNull(cause, "cause must not be null");
  }
}
