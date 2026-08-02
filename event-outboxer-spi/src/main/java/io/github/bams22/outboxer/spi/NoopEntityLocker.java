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

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * No-op {@link EntityLocker} implementation — exposed as the {@link EntityLocker#NOOP} singleton.
 *
 * <p>Always grants a zero-cost {@link EntityLocker.LockHandle} that does nothing on close. Used
 * when no handler declares {@code extractLockKey(payload)} (see ADR-0012) so that the dispatcher
 * does not need a null check on the per-call locker path.
 */
final class NoopEntityLocker implements EntityLocker {

    private static final LockHandle HANDLE = () -> {};

    @Override
    public Optional<LockHandle> tryLock(String key, Duration ttl) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        return Optional.of(HANDLE);
    }
}
