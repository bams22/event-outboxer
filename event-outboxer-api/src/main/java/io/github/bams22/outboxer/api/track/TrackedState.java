/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.track;

/**
 * What an observer can say about an event id (ADR-0038 §3). Deliberately a different type from the
 * persisted {@link io.github.bams22.outboxer.domain.EventStatus}: the three hot-table statuses plus
 * the two answers for an id that is <em>not</em> in the hot table.
 *
 * <table>
 *   <caption>Mapping from storage to {@code TrackedState}</caption>
 *   <tr><th>Lookup result</th><th>{@code TrackedState}</th></tr>
 *   <tr><td>hot-table row, {@code status = PENDING}</td><td>{@link #PENDING}</td></tr>
 *   <tr><td>hot-table row, {@code status = PROCESSING}</td><td>{@link #PROCESSING}</td></tr>
 *   <tr><td>hot-table row, {@code status = DISABLED}</td><td>{@link #DISABLED}</td></tr>
 *   <tr><td>no hot-table row, archive row present</td><td>{@link #ARCHIVED}</td></tr>
 *   <tr><td>no hot-table row, no archive row</td><td>{@link #ABSENT}</td></tr>
 * </table>
 */
public enum TrackedState {

    /**
     * Hot-table row with {@code status = PENDING}: waiting for its first attempt, or for the next
     * one after a retry, an orphan recovery or a reclaim.
     */
    PENDING,

    /**
     * Hot-table row with {@code status = PROCESSING}: claimed by a worker, a handler is running.
     */
    PROCESSING,

    /**
     * Hot-table row with {@code status = DISABLED}: failed for good — retries exhausted or the
     * handler returned {@code EventOutcome.fail(...)}. Stays until an operator re-enables or purges
     * it.
     */
    DISABLED,

    /**
     * No hot-table row, but the opt-in archive ({@code event-outboxer.storage.archive-enabled=true},
     * ADR-0008) holds one: the event was finalised after a {@code Success} or a {@code Skip}, or was
     * coalesced into another event of the same dedup key at claim time (ADR-0037).
     */
    ARCHIVED,

    /**
     * Neither a hot-table row nor an archive row. Exactly one of these, and the library cannot tell
     * which:
     *
     * <ul>
     *   <li>the event was processed (or skipped, or coalesced) while the archive is disabled —
     *       success means {@code DELETE} (ADR-0008);
     *   <li>the publishing transaction never committed — it rolled back, or is still open;
     *   <li>the row was purged by retention;
     *   <li>the id is simply unknown.
     * </ul>
     *
     * <p>Enable the archive to get an authoritative "done" for arbitrary ids.
     */
    ABSENT
}
