/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi.contracts.support;

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.spi.AdminCursor;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Inert {@link OutboxAdmin} for tests that need the port but exercise only a slice of it: every
 * operation answers "nothing there" — empty pages, empty archive lookups, zero counts, {@code
 * false} — so a subclass overrides just the methods its assertions are about.
 *
 * <p>Shared through the SPI test-jar on purpose. Hand-written copies of the whole interface
 * accumulated in unrelated test classes and every new {@code OutboxAdmin} method had to be added to
 * each of them; the replay operations of ADR-0033 paid that cost twice before this class existed.
 * The archive replay methods are not listed below because {@link OutboxAdmin} defaults them to the
 * same inert answer already.
 *
 * <p>This is a stub, not a fake: it has no storage behind it and never remembers a call. Tests that
 * need state should use {@code InMemoryOutboxAdmin} over an {@code InMemoryEventStore}.
 */
public class StubOutboxAdmin implements OutboxAdmin {

    @Override
    public List<Event> findByStatus(
            EventStatus status,
            @Nullable String eventType,
            int limit,
            @Nullable AdminCursor after) {
        return List.of();
    }

    @Override
    public Optional<ArchivedEvent> findInArchive(UUID id) {
        return Optional.empty();
    }

    @Override
    public boolean reenable(UUID id) {
        return false;
    }

    @Override
    public int reenableAll(String eventType, @Nullable Instant createdBefore, int limit) {
        return 0;
    }

    @Override
    public int purgeDisabled(@Nullable String eventType, Instant olderThan, int limit) {
        return 0;
    }

    @Override
    public int purgeArchive(Instant archivedBefore, int limit) {
        return 0;
    }
}
