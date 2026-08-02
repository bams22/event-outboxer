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

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.exception.EventStoreException;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot.EventTypeStats;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jspecify.annotations.Nullable;

/**
 * In-memory {@link EventStore} — TEST INFRASTRUCTURE, deliberately unreachable through starter
 * configuration (ADR-0020). Stores every row in a {@link ConcurrentHashMap} keyed by event id and
 * synchronizes state transitions on the row object so that concurrent claim / finalize / force
 * reclaim calls observe strict linearisability per-row.
 *
 * <p>Per ADR-0008 this adapter does <em>not</em> archive successful events — {@link
 * #markProcessed(UUID, WorkerId, long)} removes the row from the active store. Applications that
 * need an archive table should use the PostgreSQL adapter with {@code
 * event-outboxer.storage.archive-enabled}.
 */
public final class InMemoryEventStore implements EventStore {

    private final ConcurrentMap<UUID, EventRow> rows = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemoryEventStore() {
        this(Clock.system());
    }

    public InMemoryEventStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    // ---------------------------------------------------------------------------------------------
    // save / saveAll / findById
    // ---------------------------------------------------------------------------------------------

    /** Serializes dedup-key inserts — the in-memory analogue of the partial unique index. */
    private final Object dedupInsertMonitor = new Object();

    @Override
    public boolean save(PendingEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        String dedupKey = event.dedupKey();
        if (dedupKey == null) {
            return insert(event);
        }
        // Coarse but correct for test infrastructure: one monitor makes the exists-check and the
        // insert atomic, mirroring the PG partial unique index over PENDING rows (ADR-0021).
        synchronized (dedupInsertMonitor) {
            if (findPendingByDedupKey(event.eventType(), dedupKey).isPresent()) {
                return false;
            }
            return insert(event);
        }
    }

    private boolean insert(PendingEvent event) {
        EventRow row = EventRow.fromPending(event, clock.now());
        EventRow previous = rows.putIfAbsent(row.id, row);
        if (previous != null) {
            throw new EventStoreException("duplicate event id: " + row.id);
        }
        return true;
    }

    @Override
    public Optional<UUID> lockPendingByDedupKey(String eventType, String dedupKey) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(dedupKey, "dedupKey must not be null");
        // No transactions in the in-memory adapter, hence nothing to pin: the coalescing
        // visibility race of ADR-0021 cannot occur here (every save is immediately visible).
        return findPendingByDedupKey(eventType, dedupKey);
    }

    private Optional<UUID> findPendingByDedupKey(String eventType, String dedupKey) {
        for (EventRow row : rows.values()) {
            synchronized (row) {
                if (row.status == EventStatus.PENDING
                        && eventType.equals(row.eventType)
                        && dedupKey.equals(row.dedupKey)) {
                    return Optional.of(row.id);
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public void saveAll(List<PendingEvent> events) {
        Objects.requireNonNull(events, "events must not be null");
        for (PendingEvent e : events) {
            if (e.dedupKey() != null) {
                throw new IllegalArgumentException(
                        "saveAll does not accept events with a dedup key (event " + e.id() + ")");
            }
        }
        for (PendingEvent e : events) {
            save(e);
        }
    }

    @Override
    public Optional<Event> findById(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        EventRow row = rows.get(id);
        if (row == null) {
            return Optional.empty();
        }
        synchronized (row) {
            return Optional.of(row.toEvent());
        }
    }

    // ---------------------------------------------------------------------------------------------
    // claim
    // ---------------------------------------------------------------------------------------------

    @Override
    public List<ClaimedEvent> claim(ClaimRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        Instant now = clock.now();

        List<EventRow> candidates = new ArrayList<>();
        for (EventRow row : rows.values()) {
            if (!row.eventType.equals(request.eventType())) {
                continue;
            }
            synchronized (row) {
                if (row.status == EventStatus.PENDING && !row.runAt.isAfter(now)) {
                    candidates.add(row);
                }
            }
        }
        candidates.sort(CLAIM_ORDER);

        List<ClaimedEvent> claimed = new ArrayList<>(Math.min(candidates.size(), request.limit()));
        for (EventRow row : candidates) {
            if (claimed.size() >= request.limit()) {
                break;
            }
            synchronized (row) {
                if (row.status == EventStatus.PENDING && !row.runAt.isAfter(now)) {
                    row.status = EventStatus.PROCESSING;
                    row.claimedBy = request.workerId();
                    row.claimedAt = now;
                    row.version += 1;
                    claimed.add(row.toClaimed());
                }
            }
        }
        return claimed;
    }

    // Priority DESC, then runAt ASC — matches STORAGE.md §Key queries / claim.
    private static final Comparator<EventRow> CLAIM_ORDER =
            Comparator.comparingInt((EventRow r) -> (int) r.priority)
                    .reversed()
                    .thenComparing(r -> r.runAt);

    // ---------------------------------------------------------------------------------------------
    // finalize operations
    // ---------------------------------------------------------------------------------------------

    @Override
    public boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion) {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
        EventRow row = rows.get(id);
        if (row == null) {
            return false;
        }
        synchronized (row) {
            if (!matchesClaim(row, workerId, claimedVersion)) {
                return false;
            }
            rows.remove(id);
            return true;
        }
    }

    @Override
    public boolean markForRetry(
            UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt) {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(runAt, "runAt must not be null");
        EventRow row = rows.get(id);
        if (row == null) {
            return false;
        }
        synchronized (row) {
            if (!matchesClaim(row, workerId, claimedVersion)) {
                return false;
            }
            row.status = EventStatus.PENDING;
            row.claimedBy = null;
            row.claimedAt = null;
            row.attempts += 1;
            row.version += 1;
            row.lastFailReason = reason;
            row.runAt = runAt;
            return true;
        }
    }

    @Override
    public boolean markDisabled(UUID id, WorkerId workerId, long claimedVersion, String reason) {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
        EventRow row = rows.get(id);
        if (row == null) {
            return false;
        }
        synchronized (row) {
            if (!matchesClaim(row, workerId, claimedVersion)) {
                return false;
            }
            row.status = EventStatus.DISABLED;
            row.claimedBy = null;
            row.claimedAt = null;
            row.version += 1;
            row.lastFailReason = reason;
            return true;
        }
    }

    @Override
    public boolean release(
            UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt) {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(runAt, "runAt must not be null");
        EventRow row = rows.get(id);
        if (row == null) {
            return false;
        }
        synchronized (row) {
            if (!matchesClaim(row, workerId, claimedVersion)) {
                return false;
            }
            row.status = EventStatus.PENDING;
            row.claimedBy = null;
            row.claimedAt = null;
            row.version += 1;
            row.lastFailReason = reason;
            row.runAt = runAt;
            return true;
        }
    }

    @Override
    public int releaseClaimed(WorkerId workerId, Instant now) {
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(now, "now must not be null");
        int released = 0;
        for (EventRow row : rows.values()) {
            synchronized (row) {
                if (row.status == EventStatus.PROCESSING && workerId.equals(row.claimedBy)) {
                    row.status = EventStatus.PENDING;
                    row.claimedBy = null;
                    row.claimedAt = null;
                    row.version += 1;
                    row.lastFailReason = "released: worker shutdown";
                    row.runAt = now;
                    released++;
                }
            }
        }
        return released;
    }

    @Override
    public boolean forceReclaim(UUID id, WorkerId workerId, long claimedVersion, Instant runAt) {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(runAt, "runAt must not be null");
        EventRow row = rows.get(id);
        if (row == null) {
            return false;
        }
        synchronized (row) {
            if (!matchesClaim(row, workerId, claimedVersion)) {
                return false;
            }
            row.status = EventStatus.PENDING;
            row.claimedBy = null;
            row.claimedAt = null;
            row.attempts += 1;
            row.version += 1;
            row.runAt = runAt;
            return true;
        }
    }

    @Override
    public int sweepStale(java.time.Duration olderThan, int limit) {
        Objects.requireNonNull(olderThan, "olderThan must not be null");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
        Instant cutoff = clock.now().minus(olderThan);
        int swept = 0;
        for (EventRow row : rows.values()) {
            if (swept >= limit) {
                break;
            }
            synchronized (row) {
                Instant claimedAt = row.claimedAt;
                if (row.status == EventStatus.PROCESSING
                        && claimedAt != null
                        && claimedAt.isBefore(cutoff)) {
                    row.status = EventStatus.PENDING;
                    row.claimedBy = null;
                    row.claimedAt = null;
                    row.attempts += 1;
                    row.version += 1;
                    row.lastFailReason = "swept: stale PROCESSING claim";
                    row.runAt = clock.now();
                    swept++;
                }
            }
        }
        return swept;
    }

    @Override
    public int reclaimOrphans(List<WorkerId> deadWorkers, Instant now) {
        Objects.requireNonNull(deadWorkers, "deadWorkers must not be null");
        Objects.requireNonNull(now, "now must not be null");
        if (deadWorkers.isEmpty()) {
            return 0;
        }
        int reclaimed = 0;
        for (EventRow row : rows.values()) {
            synchronized (row) {
                if (row.status == EventStatus.PROCESSING
                        && row.claimedBy != null
                        && deadWorkers.contains(row.claimedBy)) {
                    row.status = EventStatus.PENDING;
                    row.claimedBy = null;
                    row.claimedAt = null;
                    row.attempts += 1;
                    row.version += 1;
                    row.runAt = now;
                    reclaimed++;
                }
            }
        }
        return reclaimed;
    }

    // ---------------------------------------------------------------------------------------------
    // metrics
    // ---------------------------------------------------------------------------------------------

    @Override
    public OutboxMetricsSnapshot metricsSnapshot() {
        long pending = 0;
        long processing = 0;
        long disabled = 0;
        Instant oldestPending = null;
        Instant oldestClaimed = null;
        Map<String, int[]> perTypeCounts = new HashMap<>();
        Map<String, Instant> perTypeOldestPending = new HashMap<>();

        for (EventRow row : rows.values()) {
            synchronized (row) {
                int[] counts = perTypeCounts.computeIfAbsent(row.eventType, _ -> new int[3]);
                switch (row.status) {
                    case PENDING -> {
                        pending++;
                        counts[0]++;
                        if (oldestPending == null || row.runAt.isBefore(oldestPending)) {
                            oldestPending = row.runAt;
                        }
                        Instant typeOldest = perTypeOldestPending.get(row.eventType);
                        if (typeOldest == null || row.runAt.isBefore(typeOldest)) {
                            perTypeOldestPending.put(row.eventType, row.runAt);
                        }
                    }
                    case PROCESSING -> {
                        processing++;
                        counts[1]++;
                        Instant claimedAt = row.claimedAt;
                        if (claimedAt != null
                                && (oldestClaimed == null || claimedAt.isBefore(oldestClaimed))) {
                            oldestClaimed = claimedAt;
                        }
                    }
                    case DISABLED -> {
                        disabled++;
                        counts[2]++;
                    }
                }
            }
        }

        List<EventTypeStats> perType = new ArrayList<>(perTypeCounts.size());
        for (var entry : perTypeCounts.entrySet()) {
            int[] counts = entry.getValue();
            perType.add(
                    EventTypeStats.builder()
                            .eventType(entry.getKey())
                            .pending(counts[0])
                            .processing(counts[1])
                            .disabled(counts[2])
                            .oldestPendingRunAt(perTypeOldestPending.get(entry.getKey()))
                            .build());
        }

        return OutboxMetricsSnapshot.builder()
                .totalPending(pending)
                .totalProcessing(processing)
                .totalDisabled(disabled)
                .oldestPendingRunAt(oldestPending)
                .oldestClaimedAt(oldestClaimed)
                .takenAt(clock.now())
                .perType(perType)
                .build();
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    /** Backing rows, exposed for {@link InMemoryOutboxAdmin} in this package. */
    ConcurrentMap<UUID, EventRow> rows() {
        return rows;
    }

    /** Clock this store stamps rows with, exposed for {@link InMemoryOutboxAdmin}. */
    Clock clock() {
        return clock;
    }

    /** Must be called under {@code synchronized (row)}. */
    private static boolean matchesClaim(EventRow row, WorkerId workerId, long claimedVersion) {
        return row.status == EventStatus.PROCESSING
                && workerId.equals(row.claimedBy)
                && row.version == claimedVersion;
    }

    /**
     * Mutable row; every state transition happens under {@code synchronized (this)}. Fields are
     * package-private for direct access from the enclosing class under the monitor.
     */
    static final class EventRow {

        final UUID id;
        final String eventType;
        final SerializedPayload payload;
        final String payloadFormat;
        final String payloadClass;
        final short priority;
        final Instant createdAt;
        final Map<String, String> traceContext;

        int attempts;
        EventStatus status;
        Instant runAt;
        @Nullable WorkerId claimedBy;
        @Nullable Instant claimedAt;
        @Nullable String lastFailReason;
        long version;
        final @Nullable String dedupKey;

        private EventRow(PendingEvent source, Instant createdAt) {
            this.id = source.id();
            this.eventType = source.eventType();
            this.payload = source.payload();
            this.payloadFormat = source.payloadFormat();
            this.payloadClass = source.payloadClass();
            this.priority = source.priority();
            this.createdAt = createdAt;
            this.traceContext = source.traceContext();
            this.dedupKey = source.dedupKey();
            this.attempts = 0;
            this.status = EventStatus.PENDING;
            this.runAt = source.runAt();
            this.claimedBy = null;
            this.claimedAt = null;
            this.lastFailReason = null;
            this.version = 0L;
        }

        static EventRow fromPending(PendingEvent source, Instant createdAt) {
            return new EventRow(source, createdAt);
        }

        Event toEvent() {
            return new Event(
                    id,
                    eventType,
                    payload,
                    payloadFormat,
                    payloadClass,
                    priority,
                    attempts,
                    status,
                    createdAt,
                    runAt,
                    claimedBy,
                    claimedAt,
                    lastFailReason,
                    traceContext,
                    version,
                    dedupKey);
        }

        ClaimedEvent toClaimed() {
            Instant ca = Objects.requireNonNull(claimedAt, "claimedAt must be set when claimed");
            return new ClaimedEvent(
                    id,
                    eventType,
                    payload,
                    payloadFormat,
                    payloadClass,
                    priority,
                    attempts,
                    createdAt,
                    ca,
                    traceContext,
                    version);
        }
    }
}
