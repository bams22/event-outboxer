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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistent store of outbox events. The only port that touches user data in the database — every
 * SQL statement lives in the adapter implementing this interface (see STORAGE.md for the
 * PostgreSQL query set).
 *
 * <h2>Semantics of finalize operations</h2>
 *
 * {@link #markProcessed(UUID, WorkerId, long)}, {@link #markForRetry(UUID, WorkerId, long, String,
 * Instant)}, {@link #markDisabled(UUID, WorkerId, long, String)} and {@link #forceReclaim(UUID,
 * WorkerId, long, Instant)} each return {@code boolean}:
 *
 * <ul>
 *   <li>{@code true} — the state transition was applied.
 *   <li>{@code false} — the row's {@code version} moved on under us (orphan recovery took the
 *       event back, or another worker finalized it first). This is an expected at-least-once race
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
   * @throws EventStoreException if the write fails
   */
  void save(PendingEvent event);

  /**
   * Batch-insert pending events. Adapters should prefer a single multi-value INSERT or a
   * {@code PreparedStatement.addBatch()} so that N publishes do not imply N round-trips. Per
   * ADR-0011 the operation is fail-fast: any violation rolls the whole batch back through the
   * enclosing transaction.
   *
   * @throws EventStoreException if the batch fails
   */
  void saveAll(List<PendingEvent> events);

  /**
   * Claim up to {@code request.limit()} events of {@code request.eventType()} for processing by
   * {@code request.workerId()}. The PostgreSQL adapter implements this with a CTE that runs
   * {@code SELECT ... FOR UPDATE SKIP LOCKED} against the per-type index and then {@code UPDATE
   * ... RETURNING *}. Claim semantics:
   *
   * <ul>
   *   <li>Only rows with {@code status = PENDING AND run_at <= now()} are eligible.
   *   <li>Rows are returned in priority-desc then {@code run_at}-asc order.
   *   <li>Every returned row has its {@code claimed_by}, {@code claimed_at}, {@code status =
   *       PROCESSING} and {@code version} fields updated atomically; the {@code version} on the
   *       returned {@link ClaimedEvent} is the <em>new</em> value, which the engine must echo
   *       back to finalize methods.
   * </ul>
   *
   * @throws EventStoreException if the claim query fails
   */
  List<ClaimedEvent> claim(ClaimRequest request);

  /**
   * Finalize a successful handler run: delete the row from the active store (or move it to the
   * archive table if the archive feature is enabled — see ADR-0008).
   *
   * <p>Guarded by {@code WHERE id = :id AND version = :claimedVersion AND claimed_by =
   * :workerId}. Returns {@code false} if the row was already taken over by orphan recovery or
   * another force-reclaim.
   *
   * @throws EventStoreException if the SQL fails
   */
  boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion);

  /**
   * Finalize a failed handler run scheduled for retry: update the row back to {@code PENDING},
   * clear {@code claimed_by}/{@code claimed_at}, bump {@code attempts} and {@code version},
   * record {@code reason} and the new {@code runAt}.
   *
   * <p>Guarded by {@code WHERE id = :id AND version = :claimedVersion AND claimed_by =
   * :workerId}.
   *
   * @throws EventStoreException if the SQL fails
   */
  boolean markForRetry(
      UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt);

  /**
   * Finalize a run that exhausted its retry budget or returned explicit {@code Fail}: update the
   * row to {@code DISABLED}, clear {@code claimed_by}/{@code claimed_at}, bump {@code version},
   * record {@code reason}.
   *
   * <p>Guarded by {@code WHERE id = :id AND version = :claimedVersion AND claimed_by =
   * :workerId}.
   *
   * @throws EventStoreException if the SQL fails
   */
  boolean markDisabled(UUID id, WorkerId workerId, long claimedVersion, String reason);

  /**
   * Forcibly reclaim an event whose handler has been running longer than {@code
   * handlerMaxRuntime} (watchdog path). The watchdog, not orphan recovery, is the caller: the
   * worker is still alive and heartbeating, but a specific handler is stuck.
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
   * Reclaim all events owned by the given set of dead workers back to {@code PENDING} in a single
   * transaction. Increments {@code attempts} and {@code version}, clears {@code
   * claimed_by}/{@code claimed_at}, sets {@code run_at} to {@code now} so the events are
   * immediately eligible again. Returns the number of rows reclaimed.
   *
   * <p>The PostgreSQL adapter pairs this with {@link WorkerRegistry#removeDead(List)} inside the
   * same transaction so that a crash between the two cannot leak orphan references to dead
   * workers (see STORAGE.md §Key queries / orphan recovery).
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
