/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.db;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * What the outbox schema looks like after the fleet has stopped. A clean drain leaves no event rows
 * (processed events are deleted, or moved to the archive when it is enabled) and no lease rows.
 *
 * @param eventRows rows left in {@code events}
 * @param lockRows live leases left in {@code entity_locks} that somebody alive should have released
 *     (see {@code PgProbe.storageState} for what is excluded under chaos); {@code -1} when the
 *     table does not exist (advisory or noop locker)
 */
public record StorageState(long eventRows, long lockRows) {

    /** The storage-cleanliness invariant (ADR-0034 §5). */
    @JsonProperty("clean")
    public boolean clean() {
        return eventRows == 0 && lockRows <= 0;
    }
}
