/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.inmemory;

import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.contracts.AbstractEventStoreContractTest;
import java.time.Instant;
import java.util.UUID;

class InMemoryEventStoreTest extends AbstractEventStoreContractTest {

    private InMemoryEventStore inMemoryStore;

    @Override
    protected EventStore newStore() {
        inMemoryStore = new InMemoryEventStore();
        return inMemoryStore;
    }

    @Override
    protected void backdateClaim(UUID id, Instant at) {
        InMemoryEventStore.EventRow row = inMemoryStore.rows().get(id);
        if (row == null) {
            throw new AssertionError("no row for " + id);
        }
        synchronized (row) {
            row.claimedAt = at;
        }
    }
}
