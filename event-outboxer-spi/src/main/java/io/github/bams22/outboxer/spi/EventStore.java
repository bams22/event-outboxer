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

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.exception.EventStoreException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Persistent store of outbox events. The only port that touches user data in the database — every
 * SQL statement lives in the adapter implementing this interface (see STORAGE.md for the PostgreSQL
 * query set).
 *
 * <h2>Semantics of finalize operations</h2>
 *
 * {@link #markProcessed(UUID, WorkerId, long)}, {@link #markForRetry(UUID, WorkerId, long, String,
 * Instant)}, {@link #markDisabled(UUID, WorkerId, long, String)} and {@link #forceReclaim(UUID,
 * WorkerId, long, Instant)} each return {@code boolean}:
 *
 * <ul>
 *   <li>{@code true} — the state transition was applied.
 *   <li>{@code false} — the row's {@code version} moved on under us (orphan recovery took the event
 *       back, or another worker finalized it first). This is an expected at-least-once race
 *       (ADR-0015) and the engine treats it as "someone else handled this; move on". Boolean
 *       returns rather than exceptions for this condition follow ADR-0014.
 * </ul>
 *
 * <h2>Threading</h2>
 *
 * Implementations must be thread-safe. The engine calls every method concurrently from multiple
 * per-type pollers and maintenance tasks.
 */
public interface EventStore {

  /**
   * Insert a single pending event. Participates in the caller's transaction via the {@link
   * ConnectionSupplier} port when the PostgreSQL adapter is used (ADR-0002).
   *
   * <p>When {@code event.dedupKey()} is set, the insert is conditional (ADR-0021): if a {@code
   * PENDING} event with the same {@code (eventType, dedupKey)} already exists, nothing is inserted
   * and {@code false} is returned. Events without a dedup key always insert.
   *
   * @return {@code true} if the row was inserted; {@code false} if it was coalesced away by an
   *     existing PENDING event with the same dedup key
   * @throws EventStoreException if the write fails
   */
  boolean save(PendingEvent event);

  /**
   * Batch-insert pending events. Adapters should prefer a single multi-value INSERT or a {@code
   * PreparedStatement.addBatch()} so that N publishes do not imply N round-trips. Per ADR-0011 the
   * operation is fail-fast: any violation rolls the whole batch back through the enclosing
   * transaction.
   *
   * <p>Events with a dedup key are NOT allowed here — coalescing needs the per-row feedback of
   * {@link #save(PendingEvent)}; the publisher routes such requests individually.
   *
   * @throws EventStoreException if the batch fails
   * @throws IllegalArgumentException if any event carries a dedup key
   */
  void saveAll(List<PendingEvent> events);

  /**
   * Find the {@code PENDING} event with the given {@code (eventType, dedupKey)} and — for adapters
   * that participate in the caller's transaction — lock its row so that claim queries (which use
   * {@code FOR UPDATE SKIP LOCKED}) skip it until the caller's transaction commits. This is the
   * coalescing visibility guarantee of ADR-0021: the transaction whose publish was coalesced into
   * an existing event is always committed before that event is handled.
   *
   * @return the id of the locked PENDING event, or empty if none exists (claimed, finalized or
   *     never published) — callers should retry the insert then
   * @throws EventStoreException if the query fails
   */
  Optional<UUID> lockPendingByDedupKey(String eventType, String dedupKey);

  /**
   * Claim up to {@code request.limit()} events of {@code request.eventType()} for processing by
   * {@code request.workerId()}. The PostgreSQL adapter implements this with a CTE that runs {@code
   * SELECT ... FOR UPDATE SKIP LOCKED} against the per-type index and then {@code UPDATE ...
   * RETURNING *}. Claim semantics:
   *
   * <ul>
   *   <li>Only rows with {@code status = PENDING AND run_at <= now()} are eligible.
   *   <li>Rows are returned in priority-desc then {@code run_at}-asc order.
   *   <li>Every returned row has its {@code claimed_by}, {@code claimed_at}, {@code status =
   *       PROCESSING} and {@code version} fields updated atomically; the {@code version} on the
   *       returned {@link ClaimedEvent} is the <em>new</em> value, which the engine must echo back
   *       to finalize methods.
   * </ul>
   *
   * @throws EventStoreException if the claim query fails
   */
  List<ClaimedEvent> claim(ClaimRequest request);

  /**
   * Finalize a successful handler run: delete the row from the active store (or move it to the
   * archive table if the archive feature is enabled — see ADR-0008).
   *
   * <p>Guarded by {@code WHERE id = :id AND version = :claimedVersion AND claimed_by = :workerId}.
   * Returns {@code false} if the row was already taken over by orphan recovery or another
   * force-reclaim.
   *
   * @throws EventStoreException if the SQL fails
   */
  boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion);

  /**
   * Finalize a failed handler run scheduled for retry: update the row back to {@code PENDING},
   * clear {@code claimed_by}/{@code claimed_at}, bump {@code attempts} and {@code version}, record
   * {@code reason} and the new {@code runAt}.
   *
   * <p>Guarded by {@code WHERE id = :id AND version = :claimedVersion AND claimed_by = :workerId}.
   *
   * @throws EventStoreException if the SQL fails
   */
  boolean markForRetry(
      UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt);

  /**
   * Batch form of {@link #markProcessed(UUID, WorkerId, long)}: finalize several successful runs of
   * the same worker in one operation. Adapters should implement this as a single multi-row
   * statement so that N finalizes do not imply N round-trips (the engine's group-commit path relies
   * on it — see ADR-0014).
   *
   * <p>Each mark keeps its own optimistic-locking guard ({@code version = claimedVersion AND
   * claimed_by = workerId AND status = 'PROCESSING'}); the returned set contains the ids whose
   * guard matched. Ids absent from the set lost the at-least-once race exactly like a single-row
   * {@code markProcessed} returning {@code false}. Result order is unspecified; an empty input
   * returns an empty set without touching the store.
   *
   * <p>The default implementation loops {@link #markProcessed(UUID, WorkerId, long)} — correct for
   * any adapter, just without the round-trip savings.
   *
   * @throws EventStoreException if the SQL fails (no per-row verdicts are reported then; the engine
   *     releases the whole batch back to {@code PENDING})
   */
  default Set<UUID> markProcessedAll(List<ProcessedMark> marks, WorkerId workerId) {
    Objects.requireNonNull(marks, "marks must not be null");
    Objects.requireNonNull(workerId, "workerId must not be null");
    Set<UUID> applied = new HashSet<>();
    for (ProcessedMark mark : marks) {
      if (markProcessed(mark.id(), workerId, mark.claimedVersion())) {
        applied.add(mark.id());
      }
    }
    return applied;
  }

  /**
   * Batch form of {@link #markForRetry(UUID, WorkerId, long, String, Instant)}: schedule several
   * failed runs of the same worker for retry in one operation, each with its own {@code reason} and
   * {@code runAt}. Same per-row guard and return semantics as {@link #markProcessedAll(List,
   * WorkerId)}.
   *
   * <p>The default implementation loops {@link #markForRetry(UUID, WorkerId, long, String,
   * Instant)}.
   *
   * @throws EventStoreException if the SQL fails
   */
  default Set<UUID> markForRetryAll(List<RetryMark> marks, WorkerId workerId) {
    Objects.requireNonNull(marks, "marks must not be null");
    Objects.requireNonNull(workerId, "workerId must not be null");
    Set<UUID> applied = new HashSet<>();
    for (RetryMark mark : marks) {
      if (markForRetry(mark.id(), workerId, mark.claimedVersion(), mark.reason(), mark.runAt())) {
        applied.add(mark.id());
      }
    }
    return applied;
  }

  /**
   * One row of a {@link #markProcessedAll(List, WorkerId)} batch: the event id plus the version
   * observed at claim time, which forms the per-row optimistic-locking guard (ADR-0014).
   */
  record ProcessedMark(UUID id, long claimedVersion) {

    public ProcessedMark {
      Objects.requireNonNull(id, "id must not be null");
    }
  }

  /**
   * One row of a {@link #markForRetryAll(List, WorkerId)} batch: the event id, the version observed
   * at claim time, and the per-event failure {@code reason} and retry {@code runAt}.
   */
  record RetryMark(UUID id, long claimedVersion, String reason, Instant runAt) {

    public RetryMark {
      Objects.requireNonNull(id, "id must not be null");
      Objects.requireNonNull(reason, "reason must not be null");
      Objects.requireNonNull(runAt, "runAt must not be null");
    }
  }

  /**
   * Finalize a run that exhausted its retry budget or returned explicit {@code Fail}: update the
   * row to {@code DISABLED}, clear {@code claimed_by}/{@code claimed_at}, bump {@code version},
   * record {@code reason}.
   *
   * <p>Guarded by {@code WHERE id = :id AND version = :claimedVersion AND claimed_by = :workerId}.
   *
   * @throws EventStoreException if the SQL fails
   */
  boolean markDisabled(UUID id, WorkerId workerId, long claimedVersion, String reason);

  /**
   * Return a claimed event to {@code PENDING} <em>without</em> incrementing {@code attempts}. Used
   * when the engine could not run the handler at all — the business-key lock was busy, the handler
   * executor rejected the dispatch, no handler is registered for the type, or a finalize call
   * failed transiently. In all of these cases no handler execution took place (or its outcome could
   * not be recorded), so burning an attempt would let pure contention or backpressure push an event
   * toward {@code DISABLED} (see {@code MaxRetriesFailureHandler}).
   *
   * <p>Bumps {@code version}, clears {@code claimed_by}/{@code claimed_at}, records {@code reason}
   * and the new {@code runAt}. Guarded by {@code WHERE id = :id AND version = :claimedVersion AND
   * claimed_by = :workerId}.
   *
   * @throws EventStoreException if the SQL fails
   */
  boolean release(UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt);

  /**
   * Return <em>every</em> event currently claimed by the given worker to {@code PENDING} without
   * incrementing {@code attempts}. Called by the engine at the end of a graceful shutdown, after
   * the handler executors have drained (or timed out): any row still {@code PROCESSING} under this
   * worker at that point was either queued-but-never-started or interrupted mid-run, and once the
   * worker deregisters no orphan recovery would ever find it. Bumps {@code version} so a late
   * finalize from an interrupted handler loses the race cleanly.
   *
   * @return the number of rows released
   * @throws EventStoreException if the SQL fails
   */
  int releaseClaimed(WorkerId workerId, Instant now);

  /**
   * Forcibly reclaim an event whose handler has been running longer than {@code handlerMaxRuntime}
   * (watchdog path). The watchdog, not orphan recovery, is the caller: the worker is still alive
   * and heartbeating, but a specific handler is stuck.
   *
   * <p>Transitions the row back to {@code PENDING}, clears {@code claimed_by}/{@code claimed_at},
   * bumps {@code attempts} and {@code version}, reschedules for {@code runAt}. Guarded by {@code
   * WHERE id = :id AND version = :claimedVersion AND claimed_by = :workerId} so that a finalize
   * from the stuck handler — if it eventually returns — loses the race cleanly.
   *
   * @throws EventStoreException if the SQL fails
   */
  boolean forceReclaim(UUID id, WorkerId workerId, long claimedVersion, Instant runAt);

  /**
   * Return every {@code PROCESSING} row whose {@code claimed_at} is older than {@code olderThan} to
   * {@code PENDING}, regardless of which worker owns the claim. Last-line safety net for rows
   * invisible to both the watchdog (never registered in-flight) and orphan recovery (the owning
   * worker is alive and heartbeating): unknown-handler {@code FAIL} rows, claims stranded by a hang
   * between claim and in-flight registration, double release failures. Increments {@code attempts}
   * and {@code version} — crash-path semantics, mirroring {@link #forceReclaim} and {@link
   * #reclaimOrphans}.
   *
   * <p>Callers must pick {@code olderThan} comfortably above the largest per-type {@code
   * handlerMaxRuntime}: this method cannot see any in-flight registry and would otherwise reclaim a
   * legitimately long-running handler's row on another JVM.
   *
   * @return the number of rows swept
   * @throws EventStoreException if the SQL fails
   */
  int sweepStale(java.time.Duration olderThan, int limit);

  /**
   * Reclaim all events owned by the given set of dead workers back to {@code PENDING} in a single
   * transaction. Increments {@code attempts} and {@code version}, clears {@code claimed_by}/{@code
   * claimed_at}, sets {@code run_at} to {@code now} so the events are immediately eligible again.
   * Returns the number of rows reclaimed.
   *
   * <p>The PostgreSQL adapter pairs this with {@link WorkerRegistry#removeDead(List)} inside the
   * same transaction so that a crash between the two cannot leak orphan references to dead workers
   * (see STORAGE.md §Key queries / orphan recovery).
   *
   * @throws EventStoreException if the SQL fails
   */
  int reclaimOrphans(List<WorkerId> deadWorkers, Instant now);

  /**
   * Administrative lookup. Returns {@link Optional#empty()} if no row with the given id exists.
   *
   * @throws EventStoreException if the query fails
   */
  Optional<Event> findById(UUID id);

  /**
   * Capture a snapshot of per-status / per-type counters for metrics and health checks. Adapters
   * are expected to cache the result for a short TTL (see CONFIGURATION.md §metrics) so that
   * dashboards scraping the registry every few seconds do not hammer the database.
   *
   * @throws EventStoreException if the query fails
   */
  OutboxMetricsSnapshot metricsSnapshot();
}
