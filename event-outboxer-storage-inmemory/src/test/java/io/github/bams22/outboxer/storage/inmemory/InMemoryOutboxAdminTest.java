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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.spi.contracts.AbstractOutboxAdminContractTest;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InMemoryOutboxAdminTest extends AbstractOutboxAdminContractTest {

    private InMemoryEventStore inMemoryStore;

    @Override
    protected EventStore newStore() {
        inMemoryStore = new InMemoryEventStore();
        return inMemoryStore;
    }

    @Override
    protected OutboxAdmin newAdmin() {
        return new InMemoryOutboxAdmin(inMemoryStore);
    }

    @Test
    @DisplayName("no archive in the in-memory adapter: findInArchive empty, purgeArchive zero")
    void archiveOperationsAreNoOps() {
        assertThat(admin.findInArchive(UUID.randomUUID())).isEmpty();
        assertThat(admin.purgeArchive(Instant.now().plusSeconds(3600), 100)).isZero();
        assertThat(admin.replayFromArchive(UUID.randomUUID()))
                .isEqualTo(OutboxAdmin.ReplayOutcome.NOT_FOUND);
        assertThat(admin.replayAllFromArchive("ADMIN_A", null, null, 100, null))
                .isEqualTo(new OutboxAdmin.ReplayAllResult(0, 0, 0, null));
    }
}
