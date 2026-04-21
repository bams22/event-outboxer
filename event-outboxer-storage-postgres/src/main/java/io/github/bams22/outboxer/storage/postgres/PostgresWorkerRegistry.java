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

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.domain.exception.WorkerRegistryException;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.storage.postgres.internal.FlatMapJson;
import io.github.bams22.outboxer.storage.postgres.internal.JsonbHandler;
import io.github.bams22.outboxer.storage.postgres.internal.OutboxJdbcRunner;
import io.github.bams22.outboxer.storage.postgres.internal.OutboxJdbcRunner.ParameterBinder;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * PostgreSQL implementation of {@link WorkerRegistry}. Heartbeat lives in {@code outbox.workers}
 * (ADR-0005); a {@code PRIMARY KEY} on {@code worker_id} makes both heartbeat UPDATEs and
 * {@code ON CONFLICT} upserts O(1).
 */
public final class PostgresWorkerRegistry implements WorkerRegistry {

  private final OutboxJdbcRunner jdbc;
  private final String sqlUpsert;
  private final String sqlHeartbeat;
  private final String sqlGracefulStop;
  private final String sqlDelete;
  private final String sqlFindDead;
  private final String sqlRemoveDead;
  private final String sqlFindById;
  private final String sqlFindAll;

  public PostgresWorkerRegistry(
      ConnectionSupplier connections, PostgresStorageProperties properties) {
    this.jdbc = new OutboxJdbcRunner(Objects.requireNonNull(connections, "connections"));
    SchemaResolver tables = new SchemaResolver(Objects.requireNonNull(properties, "properties"));

    this.sqlUpsert =
        "INSERT INTO "
            + tables.workers()
            + " (worker_id, host, pid, started_at, last_heartbeat, graceful_stop, metadata) "
            + "VALUES (?, ?, ?, ?, ?, FALSE, ?::jsonb) "
            + "ON CONFLICT (worker_id) DO UPDATE SET "
            + "host = EXCLUDED.host, "
            + "pid = EXCLUDED.pid, "
            + "started_at = EXCLUDED.started_at, "
            + "last_heartbeat = EXCLUDED.last_heartbeat, "
            + "graceful_stop = FALSE, "
            + "metadata = EXCLUDED.metadata";

    this.sqlHeartbeat =
        "UPDATE " + tables.workers() + " SET last_heartbeat = ? WHERE worker_id = ?";
    this.sqlGracefulStop =
        "UPDATE " + tables.workers() + " SET graceful_stop = TRUE WHERE worker_id = ?";
    this.sqlDelete = "DELETE FROM " + tables.workers() + " WHERE worker_id = ?";

    this.sqlFindDead =
        "SELECT worker_id, host, pid, started_at, last_heartbeat, metadata "
            + "FROM "
            + tables.workers()
            + " WHERE last_heartbeat < ? AND graceful_stop = FALSE "
            + "ORDER BY last_heartbeat LIMIT ?";

    this.sqlRemoveDead = "DELETE FROM " + tables.workers() + " WHERE worker_id = ANY(?)";

    this.sqlFindById =
        "SELECT worker_id, host, pid, started_at, last_heartbeat, metadata "
            + "FROM "
            + tables.workers()
            + " WHERE worker_id = ?";
    this.sqlFindAll =
        "SELECT worker_id, host, pid, started_at, last_heartbeat, metadata "
            + "FROM "
            + tables.workers();
  }

  @Override
  public void register(WorkerInfo info) {
    Objects.requireNonNull(info, "info must not be null");
    Instant started = info.startedAt() != null ? info.startedAt() : Instant.now();
    try {
      jdbc.update(
          sqlUpsert,
          ps -> {
            ps.setString(1, info.id().value());
            ps.setString(2, info.host());
            if (info.pid() != null) {
              ps.setInt(3, info.pid());
            } else {
              ps.setNull(3, Types.INTEGER);
            }
            ps.setTimestamp(4, Timestamp.from(started));
            ps.setTimestamp(5, Timestamp.from(started));
            ps.setObject(6, JsonbHandler.jsonb(FlatMapJson.serialize(info.metadata())));
          });
    } catch (SQLException ex) {
      throw new WorkerRegistryException("failed to register worker " + info.id(), ex);
    }
  }

  @Override
  public boolean heartbeat(WorkerId id, Instant at) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(at, "at must not be null");
    try {
      return jdbc.update(
              sqlHeartbeat,
              ps -> {
                ps.setTimestamp(1, Timestamp.from(at));
                ps.setString(2, id.value());
              })
          > 0;
    } catch (SQLException ex) {
      throw new WorkerRegistryException("heartbeat failed for worker " + id, ex);
    }
  }

  @Override
  public void markGracefulStop(WorkerId id) {
    Objects.requireNonNull(id, "id must not be null");
    try {
      jdbc.update(sqlGracefulStop, ps -> ps.setString(1, id.value()));
    } catch (SQLException ex) {
      throw new WorkerRegistryException("markGracefulStop failed for worker " + id, ex);
    }
  }

  @Override
  public void deregister(WorkerId id) {
    Objects.requireNonNull(id, "id must not be null");
    try {
      jdbc.update(sqlDelete, ps -> ps.setString(1, id.value()));
    } catch (SQLException ex) {
      throw new WorkerRegistryException("deregister failed for worker " + id, ex);
    }
  }

  @Override
  public List<WorkerInfo> findDead(Duration deadThreshold, int limit) {
    Objects.requireNonNull(deadThreshold, "deadThreshold must not be null");
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be positive, got " + limit);
    }
    Instant cutoff = Instant.now().minus(deadThreshold);
    try {
      return jdbc.queryList(
          sqlFindDead,
          ps -> {
            ps.setTimestamp(1, Timestamp.from(cutoff));
            ps.setInt(2, limit);
          },
          PostgresWorkerRegistry::readWorkerInfo);
    } catch (SQLException ex) {
      throw new WorkerRegistryException("findDead failed", ex);
    }
  }

  @Override
  public void removeDead(List<WorkerId> ids) {
    Objects.requireNonNull(ids, "ids must not be null");
    if (ids.isEmpty()) {
      return;
    }
    String[] values = ids.stream().map(WorkerId::value).toArray(String[]::new);
    try {
      jdbc.doWork(
          conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sqlRemoveDead)) {
              Array arr = conn.createArrayOf("varchar", values);
              try {
                ps.setArray(1, arr);
                return ps.executeUpdate();
              } finally {
                arr.free();
              }
            }
          });
    } catch (SQLException ex) {
      throw new WorkerRegistryException("removeDead failed", ex);
    }
  }

  @Override
  public Optional<WorkerInfo> findById(WorkerId id) {
    Objects.requireNonNull(id, "id must not be null");
    try {
      return jdbc.queryOne(
          sqlFindById,
          ps -> ps.setString(1, id.value()),
          PostgresWorkerRegistry::readWorkerInfo);
    } catch (SQLException ex) {
      throw new WorkerRegistryException("findById(" + id + ") failed", ex);
    }
  }

  @Override
  public List<WorkerInfo> findAll() {
    try {
      return jdbc.queryList(sqlFindAll, ParameterBinder.EMPTY, PostgresWorkerRegistry::readWorkerInfo);
    } catch (SQLException ex) {
      throw new WorkerRegistryException("findAll failed", ex);
    }
  }

  private static WorkerInfo readWorkerInfo(ResultSet rs) throws SQLException {
    WorkerId id = new WorkerId(rs.getString("worker_id"));
    @Nullable Integer pid = (Integer) rs.getObject("pid");
    @Nullable String metadataJson = rs.getString("metadata");
    Map<String, String> metadata = metadataJson == null ? Map.of() : FlatMapJson.parse(metadataJson);
    return WorkerInfo.builder()
        .id(id)
        .host(rs.getString("host"))
        .pid(pid)
        .startedAt(rs.getTimestamp("started_at").toInstant())
        .metadata(metadata)
        .build();
  }
}
