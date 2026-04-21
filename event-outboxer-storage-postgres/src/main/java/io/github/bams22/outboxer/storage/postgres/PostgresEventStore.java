/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres;

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.exception.EventStoreException;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot.EventTypeStats;
import io.github.bams22.outboxer.storage.postgres.internal.FlatMapJson;
import io.github.bams22.outboxer.storage.postgres.internal.JsonbHandler;
import io.github.bams22.outboxer.storage.postgres.internal.OutboxJdbcRunner;
import io.github.bams22.outboxer.storage.postgres.internal.OutboxJdbcRunner.ParameterBinder;
import io.github.bams22.outboxer.storage.postgres.internal.OutboxJdbcRunner.ResultSetMapper;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import io.github.bams22.outboxer.spi.Clock;

/**
 * PostgreSQL implementation of {@link EventStore}. Every SQL statement is the one documented in
 * STORAGE.md §Key queries. The adapter is Spring-free: it only consumes a {@link
 * ConnectionSupplier} to participate in the caller's transaction (ADR-0002).
 */
public final class PostgresEventStore implements EventStore {

  private final OutboxJdbcRunner jdbc;
  private final SchemaResolver tables;
  private final PostgresStorageProperties properties;
  private final Clock clock;

  private final String sqlInsert;
  private final String sqlClaim;
  private final String sqlMarkProcessedNoArchive;
  private final String sqlArchiveInsert;
  private final String sqlMarkProcessedDeleteWithArchive;
  private final String sqlMarkForRetry;
  private final String sqlMarkDisabled;
  private final String sqlForceReclaim;
  private final String sqlReclaimOrphans;
  private final String sqlFindById;
  private final String sqlMetrics;

  private final AtomicReference<CachedSnapshot> metricsCache = new AtomicReference<>();

  public PostgresEventStore(
      ConnectionSupplier connections,
      PostgresStorageProperties properties,
      Clock clock) {
    this.jdbc = new OutboxJdbcRunner(Objects.requireNonNull(connections, "connections must not be null"));
    this.properties = Objects.requireNonNull(properties, "properties must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.tables = new SchemaResolver(properties);

    this.sqlInsert =
        "INSERT INTO "
            + tables.events()
            + " (id, event_type, payload, payload_class, priority, status, created_at, run_at, trace_context) "
            + "VALUES (?, ?, ?::jsonb, ?, ?, 'PENDING', now(), ?, ?::jsonb)";

    this.sqlClaim =
        "WITH picked AS ("
            + "  SELECT id FROM "
            + tables.events()
            + "  WHERE event_type = ? AND status = 'PENDING' AND run_at <= now()"
            + "  ORDER BY priority DESC, run_at"
            + "  LIMIT ?"
            + "  FOR UPDATE SKIP LOCKED"
            + ") "
            + "UPDATE "
            + tables.events()
            + " e "
            + "SET status = 'PROCESSING', claimed_by = ?, claimed_at = now(), version = version + 1 "
            + "FROM picked "
            + "WHERE e.id = picked.id "
            + "RETURNING e.id, e.event_type, e.payload, e.payload_class, e.priority, e.attempts, "
            + "e.created_at, e.claimed_at, e.trace_context, e.version";

    this.sqlMarkProcessedNoArchive =
        "DELETE FROM "
            + tables.events()
            + " WHERE id = ? AND version = ? AND claimed_by = ? AND status = 'PROCESSING'";

    this.sqlArchiveInsert =
        "INSERT INTO "
            + tables.archive()
            + " (id, event_type, payload, payload_class, priority, attempts, created_at, run_at, "
            + "last_fail_reason, trace_context, archived_at, archived_by) "
            + "SELECT id, event_type, payload, payload_class, priority, attempts, created_at, run_at, "
            + "last_fail_reason, trace_context, now(), ? FROM "
            + tables.events()
            + " WHERE id = ? AND version = ? AND claimed_by = ? AND status = 'PROCESSING'";

    this.sqlMarkProcessedDeleteWithArchive =
        "DELETE FROM "
            + tables.events()
            + " WHERE id = ? AND version = ? AND claimed_by = ?";

    this.sqlMarkForRetry =
        "UPDATE "
            + tables.events()
            + " SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, "
            + "attempts = attempts + 1, version = version + 1, last_fail_reason = ?, run_at = ? "
            + "WHERE id = ? AND version = ? AND claimed_by = ? AND status = 'PROCESSING'";

    this.sqlMarkDisabled =
        "UPDATE "
            + tables.events()
            + " SET status = 'DISABLED', claimed_by = NULL, claimed_at = NULL, "
            + "version = version + 1, last_fail_reason = ? "
            + "WHERE id = ? AND version = ? AND claimed_by = ? AND status = 'PROCESSING'";

    this.sqlForceReclaim =
        "UPDATE "
            + tables.events()
            + " SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, "
            + "attempts = attempts + 1, version = version + 1, "
            + "last_fail_reason = 'stuck: handler exceeded handlerMaxRuntime', run_at = ? "
            + "WHERE id = ? AND version = ? AND claimed_by = ? AND status = 'PROCESSING'";

    this.sqlReclaimOrphans =
        "UPDATE "
            + tables.events()
            + " e SET status = 'PENDING', attempts = attempts + 1, "
            + "last_fail_reason = 'orphan-recovered: worker ' || e.claimed_by || ' was stale', "
            + "claimed_by = NULL, claimed_at = NULL, version = version + 1, run_at = ? "
            + "WHERE e.claimed_by = ANY(?) AND e.status = 'PROCESSING'";

    this.sqlFindById =
        "SELECT id, event_type, payload, payload_class, priority, attempts, status, "
            + "created_at, run_at, claimed_by, claimed_at, last_fail_reason, trace_context, version "
            + "FROM "
            + tables.events()
            + " WHERE id = ?";

    this.sqlMetrics =
        "SELECT event_type, status, count(*) AS cnt, "
            + "min(run_at) FILTER (WHERE status = 'PENDING') AS oldest_pending, "
            + "min(claimed_at) FILTER (WHERE status = 'PROCESSING') AS oldest_claimed "
            + "FROM "
            + tables.events()
            + " GROUP BY event_type, status";
  }

  // ---------------------------------------------------------------------------------------------
  // save / saveAll / findById
  // ---------------------------------------------------------------------------------------------

  @Override
  public void save(PendingEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    try {
      jdbc.update(sqlInsert, ps -> bindPending(ps, event));
    } catch (SQLException ex) {
      throw new EventStoreException("failed to save event " + event.id(), ex);
    }
  }

  @Override
  public void saveAll(List<PendingEvent> events) {
    Objects.requireNonNull(events, "events must not be null");
    if (events.isEmpty()) {
      return;
    }
    try {
      jdbc.doWork(
          conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sqlInsert)) {
              for (PendingEvent pe : events) {
                bindPending(ps, pe);
                ps.addBatch();
              }
              ps.executeBatch();
            }
            return null;
          });
    } catch (SQLException ex) {
      throw new EventStoreException("failed to saveAll (" + events.size() + " events)", ex);
    }
  }

  @Override
  public Optional<Event> findById(UUID id) {
    Objects.requireNonNull(id, "id must not be null");
    try {
      return jdbc.queryOne(sqlFindById, ps -> ps.setObject(1, id), PostgresEventStore::readEvent);
    } catch (SQLException ex) {
      throw new EventStoreException("findById(" + id + ") failed", ex);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // claim
  // ---------------------------------------------------------------------------------------------

  @Override
  public List<ClaimedEvent> claim(ClaimRequest request) {
    Objects.requireNonNull(request, "request must not be null");
    try {
      return jdbc.updateReturning(
          sqlClaim,
          ps -> {
            ps.setString(1, request.eventType());
            ps.setInt(2, request.limit());
            ps.setString(3, request.workerId().value());
          },
          PostgresEventStore::readClaimed);
    } catch (SQLException ex) {
      throw new EventStoreException("claim(" + request.eventType() + ") failed", ex);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // finalize operations
  // ---------------------------------------------------------------------------------------------

  @Override
  public boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(workerId, "workerId must not be null");
    if (!properties.archiveEnabled()) {
      return runFinalize(
          sqlMarkProcessedNoArchive,
          ps -> {
            ps.setObject(1, id);
            ps.setLong(2, claimedVersion);
            ps.setString(3, workerId.value());
          });
    }
    try {
      return jdbc.doWorkInTransaction(
          conn -> {
            int inserted;
            try (PreparedStatement ins = conn.prepareStatement(sqlArchiveInsert)) {
              ins.setString(1, workerId.value());
              ins.setObject(2, id);
              ins.setLong(3, claimedVersion);
              ins.setString(4, workerId.value());
              inserted = ins.executeUpdate();
            }
            if (inserted == 0) {
              return Boolean.FALSE;
            }
            try (PreparedStatement del = conn.prepareStatement(sqlMarkProcessedDeleteWithArchive)) {
              del.setObject(1, id);
              del.setLong(2, claimedVersion);
              del.setString(3, workerId.value());
              int deleted = del.executeUpdate();
              return deleted > 0;
            }
          });
    } catch (SQLException ex) {
      throw new EventStoreException("markProcessed(" + id + ") failed", ex);
    }
  }

  @Override
  public boolean markForRetry(
      UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(workerId, "workerId must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
    Objects.requireNonNull(runAt, "runAt must not be null");
    return runFinalize(
        sqlMarkForRetry,
        ps -> {
          ps.setString(1, trim(reason));
          ps.setTimestamp(2, Timestamp.from(runAt));
          ps.setObject(3, id);
          ps.setLong(4, claimedVersion);
          ps.setString(5, workerId.value());
        });
  }

  @Override
  public boolean markDisabled(UUID id, WorkerId workerId, long claimedVersion, String reason) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(workerId, "workerId must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
    return runFinalize(
        sqlMarkDisabled,
        ps -> {
          ps.setString(1, trim(reason));
          ps.setObject(2, id);
          ps.setLong(3, claimedVersion);
          ps.setString(4, workerId.value());
        });
  }

  @Override
  public boolean forceReclaim(UUID id, WorkerId workerId, long claimedVersion, Instant runAt) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(workerId, "workerId must not be null");
    Objects.requireNonNull(runAt, "runAt must not be null");
    return runFinalize(
        sqlForceReclaim,
        ps -> {
          ps.setTimestamp(1, Timestamp.from(runAt));
          ps.setObject(2, id);
          ps.setLong(3, claimedVersion);
          ps.setString(4, workerId.value());
        });
  }

  @Override
  public int reclaimOrphans(List<WorkerId> deadWorkers, Instant now) {
    Objects.requireNonNull(deadWorkers, "deadWorkers must not be null");
    Objects.requireNonNull(now, "now must not be null");
    if (deadWorkers.isEmpty()) {
      return 0;
    }
    String[] ids = deadWorkers.stream().map(WorkerId::value).toArray(String[]::new);
    try {
      return jdbc.doWork(
          conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sqlReclaimOrphans)) {
              Array arr = conn.createArrayOf("varchar", ids);
              try {
                ps.setTimestamp(1, Timestamp.from(now));
                ps.setArray(2, arr);
                return ps.executeUpdate();
              } finally {
                arr.free();
              }
            }
          });
    } catch (SQLException ex) {
      throw new EventStoreException("reclaimOrphans failed", ex);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // metrics
  // ---------------------------------------------------------------------------------------------

  @Override
  public OutboxMetricsSnapshot metricsSnapshot() {
    CachedSnapshot cached = metricsCache.get();
    Instant now = clock.now();
    if (cached != null && !cached.expired(now, properties.metricsCacheTtl())) {
      return cached.snapshot;
    }
    OutboxMetricsSnapshot fresh = computeMetrics(now);
    metricsCache.set(new CachedSnapshot(fresh, now));
    return fresh;
  }

  private OutboxMetricsSnapshot computeMetrics(Instant now) {
    try {
      List<MetricsRow> rows =
          jdbc.queryList(
              sqlMetrics,
              ParameterBinder.EMPTY,
              rs ->
                  new MetricsRow(
                      rs.getString("event_type"),
                      rs.getString("status"),
                      rs.getLong("cnt"),
                      toInstantNullable(rs.getTimestamp("oldest_pending")),
                      toInstantNullable(rs.getTimestamp("oldest_claimed"))));
      long totalPending = 0;
      long totalProcessing = 0;
      long totalDisabled = 0;
      Instant oldestPending = null;
      Instant oldestClaimed = null;
      Map<String, long[]> perTypeCounts = new HashMap<>();
      Map<String, Instant> perTypeOldestPending = new HashMap<>();
      for (MetricsRow row : rows) {
        long[] counts = perTypeCounts.computeIfAbsent(row.eventType, k -> new long[3]);
        switch (row.status) {
          case "PENDING" -> {
            totalPending += row.cnt;
            counts[0] += row.cnt;
            if (row.oldestPending != null) {
              if (oldestPending == null || row.oldestPending.isBefore(oldestPending)) {
                oldestPending = row.oldestPending;
              }
              Instant cur = perTypeOldestPending.get(row.eventType);
              if (cur == null || row.oldestPending.isBefore(cur)) {
                perTypeOldestPending.put(row.eventType, row.oldestPending);
              }
            }
          }
          case "PROCESSING" -> {
            totalProcessing += row.cnt;
            counts[1] += row.cnt;
            if (row.oldestClaimed != null) {
              if (oldestClaimed == null || row.oldestClaimed.isBefore(oldestClaimed)) {
                oldestClaimed = row.oldestClaimed;
              }
            }
          }
          case "DISABLED" -> {
            totalDisabled += row.cnt;
            counts[2] += row.cnt;
          }
          default -> {
            // forward-compatible; ignore unknown statuses
          }
        }
      }
      List<EventTypeStats> perType = new ArrayList<>(perTypeCounts.size());
      for (Map.Entry<String, long[]> e : perTypeCounts.entrySet()) {
        long[] counts = e.getValue();
        perType.add(
            EventTypeStats.builder()
                .eventType(e.getKey())
                .pending(counts[0])
                .processing(counts[1])
                .disabled(counts[2])
                .oldestPendingRunAt(perTypeOldestPending.get(e.getKey()))
                .build());
      }
      return OutboxMetricsSnapshot.builder()
          .totalPending(totalPending)
          .totalProcessing(totalProcessing)
          .totalDisabled(totalDisabled)
          .oldestPendingRunAt(oldestPending)
          .oldestClaimedAt(oldestClaimed)
          .takenAt(now)
          .perType(perType)
          .build();
    } catch (SQLException ex) {
      throw new EventStoreException("metricsSnapshot failed", ex);
    }
  }

  /** Clear the metrics cache — useful for tests that need to observe changes immediately. */
  public void invalidateMetricsCache() {
    metricsCache.set(null);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private boolean runFinalize(String sql, ParameterBinder binder) {
    try {
      return jdbc.update(sql, binder) > 0;
    } catch (SQLException ex) {
      throw new EventStoreException("finalize failed", ex);
    }
  }

  private static void bindPending(PreparedStatement ps, PendingEvent event) throws SQLException {
    ps.setObject(1, event.id());
    ps.setString(2, event.eventType());
    ps.setObject(3, JsonbHandler.jsonb(event.payload()));
    ps.setString(4, event.payloadClass());
    ps.setShort(5, event.priority());
    ps.setTimestamp(6, Timestamp.from(event.runAt()));
    ps.setObject(7, JsonbHandler.jsonb(FlatMapJson.serialize(event.traceContext())));
  }

  private static ClaimedEvent readClaimed(ResultSet rs) throws SQLException {
    return new ClaimedEvent(
        (UUID) rs.getObject("id"),
        rs.getString("event_type"),
        rs.getString("payload"),
        rs.getString("payload_class"),
        rs.getShort("priority"),
        rs.getInt("attempts"),
        toInstant(rs.getTimestamp("created_at")),
        toInstant(rs.getTimestamp("claimed_at")),
        toTraceContext(rs.getString("trace_context")),
        rs.getLong("version"));
  }

  private static Event readEvent(ResultSet rs) throws SQLException {
    String statusStr = rs.getString("status");
    String claimedByStr = rs.getString("claimed_by");
    Timestamp claimedAt = rs.getTimestamp("claimed_at");
    return new Event(
        (UUID) rs.getObject("id"),
        rs.getString("event_type"),
        rs.getString("payload"),
        rs.getString("payload_class"),
        rs.getShort("priority"),
        rs.getInt("attempts"),
        EventStatus.valueOf(statusStr),
        toInstant(rs.getTimestamp("created_at")),
        toInstant(rs.getTimestamp("run_at")),
        claimedByStr == null ? null : new WorkerId(claimedByStr),
        toInstantNullable(claimedAt),
        rs.getString("last_fail_reason"),
        toTraceContext(rs.getString("trace_context")),
        rs.getLong("version"));
  }

  private static Map<String, String> toTraceContext(String json) {
    if (json == null || json.isEmpty()) {
      return Map.of();
    }
    return FlatMapJson.parse(json);
  }

  private static Instant toInstant(Timestamp ts) {
    return ts.toInstant();
  }

  private static Instant toInstantNullable(Timestamp ts) {
    return ts == null ? null : ts.toInstant();
  }

  private static String trim(String reason) {
    final int max = 10_000;
    return reason.length() <= max ? reason : reason.substring(0, max);
  }

  /** Internal row type for the metrics GROUP BY query. */
  private record MetricsRow(
      String eventType, String status, long cnt, Instant oldestPending, Instant oldestClaimed) {}

  private record CachedSnapshot(OutboxMetricsSnapshot snapshot, Instant takenAt) {
    boolean expired(Instant now, java.time.Duration ttl) {
      return now.isAfter(takenAt.plus(ttl));
    }
  }

  // suppress unused import warning (Types is referenced by the adapter's future work on JSONB nulls)
  @SuppressWarnings("unused")
  private static int sqlTypeOther() {
    return Types.OTHER;
  }

  @SuppressWarnings("unused")
  private static Collection<WorkerId> unused() {
    return List.of();
  }
}
