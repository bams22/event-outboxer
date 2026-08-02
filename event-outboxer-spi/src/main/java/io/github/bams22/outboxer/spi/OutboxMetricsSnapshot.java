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

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Aggregate view of outbox state used by Actuator {@code /actuator/health/outbox} and by the
 * starter-registered backlog gauges (per-event-type pending / processing / disabled and the
 * oldest-age gauges in {@code MicrometerAutoConfiguration}). Returned by {@link
 * EventStore#metricsSnapshot()}; adapters delegate caching to the {@link MetricsSnapshotCache} SPI
 * — the default in-memory factory covers single-JVM cases and {@code event-outboxer-cache-redis}
 * provides a shared-across-pods variant so that dashboards scraping the registry every few seconds
 * do not hammer the database.
 *
 * <p>All counts are non-negative. {@code oldestPendingRunAt} and {@code oldestClaimedAt} are {@code
 * null} when there are no rows in the corresponding state.
 *
 * @param totalPending total number of events in {@code PENDING} state across all types
 * @param totalProcessing total number of events currently being processed
 * @param totalDisabled total number of events that were disabled (terminal failure or explicit
 *     {@code Fail})
 * @param oldestPendingRunAt earliest {@code run_at} among pending events, or {@code null} if none —
 *     exposed as a gauge for "how far behind is the outbox"
 * @param oldestClaimedAt earliest {@code claimed_at} among processing events, or {@code null} if
 *     none — exposed as a gauge for "oldest in-flight handler"
 * @param takenAt wall-clock time the snapshot was captured (used to detect stale cache entries)
 * @param perType per-event-type breakdown; never null (empty list allowed)
 */
@Builder
public record OutboxMetricsSnapshot(
    long totalPending,
    long totalProcessing,
    long totalDisabled,
    @Nullable Instant oldestPendingRunAt,
    @Nullable Instant oldestClaimedAt,
    Instant takenAt,
    List<EventTypeStats> perType) {

  public OutboxMetricsSnapshot {
    if (totalPending < 0) {
      throw new IllegalArgumentException("totalPending must not be negative, got " + totalPending);
    }
    if (totalProcessing < 0) {
      throw new IllegalArgumentException(
          "totalProcessing must not be negative, got " + totalProcessing);
    }
    if (totalDisabled < 0) {
      throw new IllegalArgumentException(
          "totalDisabled must not be negative, got " + totalDisabled);
    }
    Objects.requireNonNull(takenAt, "takenAt must not be null");
    Objects.requireNonNull(perType, "perType must not be null");
    perType = List.copyOf(perType);
  }

  /**
   * Per-event-type breakdown of the outbox state. One instance per registered event type that has
   * at least one row in the store at snapshot time.
   *
   * @param eventType non-blank event-type string
   * @param pending count of {@code PENDING} rows for this type
   * @param processing count of {@code PROCESSING} rows for this type
   * @param disabled count of {@code DISABLED} rows for this type
   * @param oldestPendingRunAt earliest {@code run_at} among pending rows of this type, or {@code
   *     null} if none
   */
  @Builder
  public record EventTypeStats(
      String eventType,
      long pending,
      long processing,
      long disabled,
      @Nullable Instant oldestPendingRunAt) {

    public EventTypeStats {
      Objects.requireNonNull(eventType, "eventType must not be null");
      if (eventType.isBlank()) {
        throw new IllegalArgumentException("eventType must not be blank");
      }
      if (pending < 0) {
        throw new IllegalArgumentException("pending must not be negative, got " + pending);
      }
      if (processing < 0) {
        throw new IllegalArgumentException("processing must not be negative, got " + processing);
      }
      if (disabled < 0) {
        throw new IllegalArgumentException("disabled must not be negative, got " + disabled);
      }
    }
  }
}
