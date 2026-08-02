/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.polling;

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventStore;
import java.util.List;
import java.util.Objects;

/**
 * Default {@link PollStrategy} — one SQL claim per tick, backed by {@code SELECT FOR UPDATE SKIP
 * LOCKED} in the PostgreSQL adapter. Matches the "lock-and-fetch" branch of ADR-0004.
 */
public final class LockAndFetchStrategy implements PollStrategy {

    private final EventStore store;

    public LockAndFetchStrategy(EventStore store) {
        this.store = Objects.requireNonNull(store, "store must not be null");
    }

    @Override
    public List<ClaimedEvent> pollOnce(String eventType, WorkerId workerId, int batchSize) {
        return store.claim(new ClaimRequest(eventType, workerId, batchSize));
    }
}
