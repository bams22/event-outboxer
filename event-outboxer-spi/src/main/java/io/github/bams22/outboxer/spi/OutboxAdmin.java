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

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.exception.EventStoreException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Operational surface over the outbox store, deliberately separate from {@link EventStore}: the
 * engine's hot path never calls these methods, and adapters that only serve the engine are not
 * forced to implement them. Consumed by the {@code event-outboxer-admin-actuator} and {@code
 * event-outboxer-admin-rest} modules and by the optional retention task (ADR-0019).
 *
 * <h2>Semantics</h2>
 *
 * <ul>
 *   <li>{@link #reenable(UUID)} applies only to {@code DISABLED} rows: back to {@code PENDING},
 *       {@code attempts} reset to zero (an operator re-enabling after a fix expects a fresh retry
 *       budget), {@code version} bumped, {@code run_at = now}.
 *   <li>{@link #purgeDisabled} filters by {@code created_at} — the schema does not record the
 *       moment of disabling, so retention age is approximated by event age.
 *   <li>{@link #replayFromArchive} moves an archived row back into the hot table as a fresh {@code
 *       PENDING} event (attempts 0, version 0, {@code created_at = now}, {@code run_at = now},
 *       {@code last_fail_reason = "replayed from archive"}) so it executes again (ADR-0033). The
 *       replay is a new lifecycle, so it starts a new retention clock: with the original publish
 *       time an event archived long ago would be purged by the next {@link #purgeDisabled} sweep
 *       and would page last in {@link #findByStatus}. A replay that cannot proceed leaves the
 *       archive row untouched and says why: {@link ReplayOutcome#COALESCED} when a {@code PENDING}
 *       event with the same {@code (event_type, dedup_key)} already exists (ADR-0021 arbiter — the
 *       work is already scheduled), {@link ReplayOutcome#ID_IN_USE} when the hot table already
 *       holds the event's id.
 *   <li>Every bulk operation takes a {@code limit} and is expected to run as a single bounded
 *       statement; callers loop for full sweeps.
 * </ul>
 *
 * <p>Implementations must be thread-safe.
 */
public interface OutboxAdmin {

    /**
     * Page of events in the given status, newest-first by {@code (created_at, id)} descending,
     * optionally filtered by event type. Pass the last row of the previous page as {@code after} to
     * fetch the next page.
     *
     * @param limit maximum rows to return; must be positive
     * @throws EventStoreException if the query fails
     */
    List<Event> findByStatus(
            EventStatus status, @Nullable String eventType, int limit, @Nullable AdminCursor after);

    /**
     * Look up a successfully processed event in the archive table (ADR-0008). Empty when the
     * archive feature is disabled, the adapter has no archive (in-memory), or no such row exists.
     *
     * @throws EventStoreException if the query fails
     */
    Optional<ArchivedEvent> findInArchive(UUID id);

    /**
     * Return a {@code DISABLED} event to {@code PENDING} with a fresh attempts budget.
     *
     * @return {@code true} if the row existed in {@code DISABLED} and was re-enabled
     * @throws EventStoreException if the update fails
     */
    boolean reenable(UUID id);

    /**
     * Bulk {@link #reenable(UUID)} for every {@code DISABLED} event of the given type, optionally
     * only those created before {@code createdBefore}, capped by {@code limit}.
     *
     * @return the number of rows re-enabled
     * @throws EventStoreException if the update fails
     */
    int reenableAll(String eventType, @Nullable Instant createdBefore, int limit);

    /**
     * Delete {@code DISABLED} events created before {@code olderThan}, optionally filtered by type,
     * capped by {@code limit}.
     *
     * @return the number of rows deleted
     * @throws EventStoreException if the delete fails
     */
    int purgeDisabled(@Nullable String eventType, Instant olderThan, int limit);

    /**
     * Delete archive rows archived before {@code archivedBefore}, capped by {@code limit}. A no-op
     * (returns zero) for adapters without an archive.
     *
     * @return the number of rows deleted
     * @throws EventStoreException if the delete fails
     */
    int purgeArchive(Instant archivedBefore, int limit);

    /**
     * Move one archived event back into the hot table as a fresh {@code PENDING} row so it executes
     * again (ADR-0033) — see the class-level semantics for the field reset and the outcomes. {@link
     * ReplayOutcome#NOT_FOUND} when no such archive row exists, including adapters without an
     * archive (in-memory).
     *
     * <p>If the application re-published the replayed event's explicit UUID after it was archived,
     * the hot table already holds that id and the replay reports {@link ReplayOutcome#ID_IN_USE}
     * instead of failing — the archive row is left untouched, so the operator can inspect the live
     * event and decide.
     *
     * <p>Defaulted to the archive-less answer, so an adapter over a store without an archive
     * inherits the documented behaviour instead of restating it.
     *
     * @throws EventStoreException if the statement fails
     */
    default ReplayOutcome replayFromArchive(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        return ReplayOutcome.NOT_FOUND;
    }

    /**
     * Bulk {@link #replayFromArchive(UUID)} for archived events of the given type, optionally
     * bounded to an {@code archived_at} window (both bounds exclusive), capped by {@code limit}.
     * Rows are taken oldest-archived first, so when the batch holds several rows with the same
     * dedup key, the oldest one replays and the newer ones coalesce and stay archived.
     *
     * <p>A row that cannot be replayed — coalesced, or its id already live — is <em>skipped, not
     * fatal</em>: the batch still moves every other row. Those rows stay in the archive, so the
     * same window keeps finding them and {@code replayed() > 0} is not a usable loop condition.
     * Sweep with the cursor instead, which advances past every row the batch considered:
     *
     * <pre>{@code
     * ArchiveCursor cursor = null;
     * ReplayAllResult batch;
     * do {
     *     batch = admin.replayAllFromArchive(type, after, before, 100, cursor);
     *     cursor = batch.next();
     * } while (cursor != null);
     * }</pre>
     *
     * @param after keyset cursor from the previous batch's {@link ReplayAllResult#next()}; {@code
     *     null} starts at the oldest row of the window
     * @param limit maximum archive rows to consider; must be positive
     * @return per-row verdict counts and the cursor to continue from; zeros and a {@code null}
     *     cursor for adapters without an archive, which is also what the default implementation
     *     answers once it has validated the arguments
     * @throws IllegalArgumentException if {@code limit} is not positive, or the window cannot match
     *     anything because {@code archivedAfter} is not strictly before {@code archivedBefore} —
     *     swapped date pickers are reported, not answered with an empty result
     * @throws EventStoreException if the statement fails
     */
    default ReplayAllResult replayAllFromArchive(
            String eventType,
            @Nullable Instant archivedAfter,
            @Nullable Instant archivedBefore,
            int limit,
            @Nullable ArchiveCursor after) {
        requireReplayAllArguments(eventType, archivedAfter, archivedBefore, limit);
        return new ReplayAllResult(0, 0, 0, null);
    }

    /**
     * Argument checks of {@link #replayAllFromArchive} — part of the contract, not of any one
     * adapter, so the archive-less default above rejects a bad call exactly as a real adapter does
     * instead of hiding it behind its zero result.
     *
     * <p>The window bounds are both exclusive, so a lower bound that is not strictly below the
     * upper one cannot match a row. That is reported rather than answered: a UI with its two date
     * pickers swapped would otherwise return "nothing to replay" for an incident window, which an
     * operator reads as "already done".
     */
    private static void requireReplayAllArguments(
            String eventType,
            @Nullable Instant archivedAfter,
            @Nullable Instant archivedBefore,
            int limit) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
        if (archivedAfter != null
                && archivedBefore != null
                && !archivedAfter.isBefore(archivedBefore)) {
            throw new IllegalArgumentException(
                    "archivedAfter must be strictly before archivedBefore, got "
                            + archivedAfter
                            + " and "
                            + archivedBefore);
        }
    }

    /** Result of {@link #replayFromArchive(UUID)}. */
    enum ReplayOutcome {
        /** The archive row moved back to the hot table as a fresh {@code PENDING} event. */
        REPLAYED,
        /**
         * A {@code PENDING} event with the same {@code (event_type, dedup_key)} already exists —
         * nothing was inserted and the archive row was kept (ADR-0021 coalescing).
         */
        COALESCED,
        /**
         * The hot table already holds an event with this id — the application re-published the
         * archived event's explicit UUID. Nothing was inserted and the archive row was kept; the
         * live event is the one to look at.
         */
        ID_IN_USE,
        /** No archive row with that id (or the adapter has no archive). */
        NOT_FOUND
    }

    /**
     * Result of {@link #replayAllFromArchive}: one counter per {@link ReplayOutcome} the batch
     * produced, plus the cursor to continue the sweep from.
     *
     * <p>{@code replayed + coalesced + idInUse} is the number of archive rows the batch considered.
     * The two non-replayed counters are reporting only — neither blocks the sweep, because {@code
     * next} advances past those rows as well.
     *
     * @param replayed rows moved back to the hot table
     * @param coalesced rows left archived because a {@code PENDING} event with the same {@code
     *     (event_type, dedup_key)} already exists
     * @param idInUse rows left archived because the hot table already holds their id
     * @param next cursor of the last row considered, to pass as {@code after} on the following
     *     call; {@code null} when the batch found nothing, which is the end of the sweep
     */
    record ReplayAllResult(
            int replayed, int coalesced, int idInUse, @Nullable ArchiveCursor next) {}
}
