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

import java.time.Duration;
import java.util.UUID;

/**
 * Read-only view of an event by its id (ADR-0038). The {@code UUID} returned by {@code
 * OutboxEventPublisher.publish(...)} is the handle: serialisable, valid across requests, threads
 * and process restarts. The tracker is stateless, reads the database only, and works on
 * publish-only instances.
 *
 * <p><b>After commit.</b> The published row becomes visible to the engine only once the publishing
 * transaction commits (ADR-0002); waiting inside that transaction can only time out. {@link
 * #await(UUID, Duration)} therefore throws {@link
 * io.github.bams22.outboxer.domain.exception.AwaitInTransactionException} when it detects an active
 * transaction on the calling thread. Return the id from the transactional method and call {@code
 * await} from its non-transactional caller, a later request, or any other code path that runs
 * outside a transaction. Transaction synchronisation callbacks ({@code afterCommit}, {@code
 * afterCompletion}) still count as inside the transaction: they run before the transaction is
 * cleaned up and while its connection is still bound to the thread, so {@code await} throws there
 * too.
 *
 * <p><b>{@code ABSENT} vs {@code ARCHIVED}.</b> Success means {@code DELETE} from the hot table
 * (ADR-0008). With the archive off (the default) a processed event, an unknown id and a
 * rolled-back publish all look the same — {@link TrackedState#ABSENT}. Enable the archive ({@code
 * event-outboxer.storage.archive-enabled=true}) when you need an authoritative "done" for arbitrary
 * ids.
 *
 * <p><b>Latency.</b> Waiting does not make the engine faster: an event is picked up on its
 * poller's cadence — right after commit on the publishing instance, up to {@code
 * poll-max-interval} elsewhere. Each waiter costs a parked thread and one primary-key lookup per
 * poll interval; think seconds, not minutes, for {@code timeout}.
 *
 * <p><b>Results.</b> No handler return value is stored or returned (ADR-0015: a handler may run
 * twice). Have the handler write its outcome into your own table, inside its own transaction,
 * keyed by {@code EventContext.eventId()}; {@link AwaitResult.Completed} is the signal to read it.
 */
public interface OutboxEventTracker {

    /**
     * Look the event up once and report what is known about it, see the mapping table on {@link
     * TrackedState}. Never blocks beyond the lookup itself.
     *
     * @param eventId id returned by {@code publish(...)}
     * @return current state; {@link TrackedState#ABSENT} when neither the hot table nor the archive
     *     knows the id
     * @throws io.github.bams22.outboxer.domain.exception.EventStoreException if a lookup fails
     */
    TrackedState state(UUID eventId);

    /**
     * Wait until the event is finalised or {@code timeout} elapses, polling at the configured
     * interval (default 200 ms). Same as {@link #await(UUID, Duration, Duration)} with that
     * interval.
     *
     * @param eventId id returned by {@code publish(...)}, after its transaction committed
     * @param timeout upper bound on the wait; must be positive
     * @return {@link AwaitResult.Completed}, {@link AwaitResult.Disabled} or {@link
     *     AwaitResult.TimedOut}
     * @throws io.github.bams22.outboxer.domain.exception.AwaitInTransactionException if a
     *     transaction is active on the calling thread
     * @throws io.github.bams22.outboxer.domain.exception.AwaitInterruptedException if the thread is
     *     interrupted while waiting; the interrupt flag is restored
     */
    AwaitResult await(UUID eventId, Duration timeout);

    /**
     * Wait until the event is finalised or {@code timeout} elapses, reading the hot-table row by
     * primary key every {@code pollInterval}:
     *
     * <ul>
     *   <li>{@code PENDING} / {@code PROCESSING} — sleep {@code min(pollInterval, remaining)} and
     *       look again;
     *   <li>{@code DISABLED} — return {@link AwaitResult.Disabled};
     *   <li>no row — consult the archive once and return {@link AwaitResult.Completed}, carrying
     *       the archive row when there is one. A rolled-back publish is reported here too: {@code
     *       await} is for ids whose transaction the caller knows committed;
     *   <li>deadline passed — return {@link AwaitResult.TimedOut} with the last state seen.
     * </ul>
     *
     * @param eventId id returned by {@code publish(...)}, after its transaction committed
     * @param timeout upper bound on the wait; must be positive
     * @param pollInterval pause between lookups; must be positive
     * @return {@link AwaitResult.Completed}, {@link AwaitResult.Disabled} or {@link
     *     AwaitResult.TimedOut}
     * @throws io.github.bams22.outboxer.domain.exception.AwaitInTransactionException if a
     *     transaction is active on the calling thread
     * @throws io.github.bams22.outboxer.domain.exception.AwaitInterruptedException if the thread is
     *     interrupted while waiting; the interrupt flag is restored
     * @throws io.github.bams22.outboxer.domain.exception.EventStoreException if a lookup fails
     */
    AwaitResult await(UUID eventId, Duration timeout, Duration pollInterval);
}
