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

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.spi.AdminCursor;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore.EventRow;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * In-memory {@link OutboxAdmin} over the rows of an {@link InMemoryEventStore}. The in-memory
 * adapter has no archive (ADR-0008), so {@link #findInArchive} is always empty and {@link
 * #purgeArchive} is a no-op.
 */
public final class InMemoryOutboxAdmin implements OutboxAdmin {

    /** Newest first; ties broken by id so keyset pagination is total and stable. */
    private static final Comparator<Event> PAGE_ORDER =
            Comparator.comparing(Event::createdAt).thenComparing(Event::id).reversed();

    private final InMemoryEventStore store;

    public InMemoryOutboxAdmin(InMemoryEventStore store) {
        this.store = Objects.requireNonNull(store, "store must not be null");
    }

    @Override
    public List<Event> findByStatus(
            EventStatus status,
            @Nullable String eventType,
            int limit,
            @Nullable AdminCursor after) {
        Objects.requireNonNull(status, "status must not be null");
        requirePositive(limit);
        List<Event> matching = new ArrayList<>();
        for (EventRow row : store.rows().values()) {
            synchronized (row) {
                if (row.status == status
                        && (eventType == null || row.eventType.equals(eventType))) {
                    matching.add(row.toEvent());
                }
            }
        }
        matching.sort(PAGE_ORDER);
        List<Event> page = new ArrayList<>(Math.min(limit, matching.size()));
        for (Event e : matching) {
            if (after != null && !isAfterCursor(e, after)) {
                continue;
            }
            page.add(e);
            if (page.size() >= limit) {
                break;
            }
        }
        return page;
    }

    @Override
    public Optional<ArchivedEvent> findInArchive(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        return Optional.empty();
    }

    @Override
    public boolean reenable(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        EventRow row = store.rows().get(id);
        if (row == null) {
            return false;
        }
        synchronized (row) {
            if (row.status != EventStatus.DISABLED) {
                return false;
            }
            reenableLocked(row);
            return true;
        }
    }

    @Override
    public int reenableAll(String eventType, @Nullable Instant createdBefore, int limit) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        requirePositive(limit);
        int reenabled = 0;
        for (EventRow row : store.rows().values()) {
            if (reenabled >= limit) {
                break;
            }
            synchronized (row) {
                if (row.status == EventStatus.DISABLED
                        && row.eventType.equals(eventType)
                        && (createdBefore == null || row.createdAt.isBefore(createdBefore))) {
                    reenableLocked(row);
                    reenabled++;
                }
            }
        }
        return reenabled;
    }

    @Override
    public int purgeDisabled(@Nullable String eventType, Instant olderThan, int limit) {
        Objects.requireNonNull(olderThan, "olderThan must not be null");
        requirePositive(limit);
        int purged = 0;
        for (EventRow row : store.rows().values()) {
            if (purged >= limit) {
                break;
            }
            synchronized (row) {
                if (row.status == EventStatus.DISABLED
                        && (eventType == null || row.eventType.equals(eventType))
                        && row.createdAt.isBefore(olderThan)) {
                    store.rows().remove(row.id);
                    purged++;
                }
            }
        }
        return purged;
    }

    @Override
    public int purgeArchive(Instant archivedBefore, int limit) {
        Objects.requireNonNull(archivedBefore, "archivedBefore must not be null");
        requirePositive(limit);
        return 0;
    }

    /** Must be called under {@code synchronized (row)} with status == DISABLED. */
    private void reenableLocked(EventRow row) {
        row.status = EventStatus.PENDING;
        row.attempts = 0;
        row.claimedBy = null;
        row.claimedAt = null;
        row.version += 1;
        row.lastFailReason = "reenabled by operator";
        row.runAt = store.clock().now();
    }

    /** {@code true} when {@code e} comes strictly after the cursor in {@link #PAGE_ORDER}. */
    private static boolean isAfterCursor(Event e, AdminCursor after) {
        int byCreatedAt = e.createdAt().compareTo(after.createdAt());
        if (byCreatedAt != 0) {
            return byCreatedAt < 0; // strictly older than the cursor row
        }
        return e.id().compareTo(after.id()) < 0;
    }

    private static void requirePositive(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
    }
}
