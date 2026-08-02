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

import io.github.bams22.outboxer.domain.WorkerId;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Payload of {@link OutboxListener#onOrphansReclaimed(OrphansReclaimedInfo)} — fired by the
 * orphan-recovery task when it found dead workers and returned their in-flight events to PENDING.
 *
 * @param deadWorkers worker ids identified as dead in this run (defensively copied)
 * @param eventCount total number of events returned to PENDING across those workers
 */
public record OrphansReclaimedInfo(Collection<WorkerId> deadWorkers, int eventCount) {

    public OrphansReclaimedInfo {
        Objects.requireNonNull(deadWorkers, "deadWorkers must not be null");
        deadWorkers = List.copyOf(deadWorkers);
        if (eventCount < 0) {
            throw new IllegalArgumentException("eventCount must be >= 0, got " + eventCount);
        }
    }
}
