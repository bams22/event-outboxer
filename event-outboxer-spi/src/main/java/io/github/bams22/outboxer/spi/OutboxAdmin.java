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
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Operational surface over the outbox store, deliberately separate from {@link EventStore}: the
 * engine's hot path never calls these methods, and adapters that only serve the engine are not
 * forced to implement them. Consumed by the {@code event-outboxer-admin-actuator} and
 * {@code event-outboxer-admin-rest} modules and by the optional retention task (ADR-0019).
 *
 * <h2>Semantics</h2>
 *
 * <ul>
 *   <li>{@link #reenable(UUID)} applies only to {@code DISABLED} rows: back to {@code PENDING},
 *       {@code attempts} reset to zero (an operator re-enabling after a fix expects a fresh
 *       retry budget), {@code version} bumped, {@code run_at = now}.
 *   <li>{@link #purgeDisabled} filters by {@code created_at} — the schema does not record the
 *       moment of disabling, so retention age is approximated by event age.
 *   <li>Every bulk operation takes a {@code limit} and is expected to run as a single bounded
 *       statement; callers loop for full sweeps.
 * </ul>
 *
 * <p>Implementations must be thread-safe.
 */
public interface OutboxAdmin {

  /**
   * Page of events in the given status, newest-first by {@code (created_at, id)} descending,
   * optionally filtered by event type. Pass the last row of the previous page as {@code after}
   * to fetch the next page.
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
   * Delete {@code DISABLED} events created before {@code olderThan}, optionally filtered by
   * type, capped by {@code limit}.
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
}
