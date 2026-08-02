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

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.exception.EventStoreException;
import io.github.bams22.outboxer.spi.AdminCursor;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.storage.postgres.internal.FlatMapJson;
import io.github.bams22.outboxer.storage.postgres.internal.OutboxJdbcRunner;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * PostgreSQL {@link OutboxAdmin}. Query shapes are documented in STORAGE.md §Admin operations;
 * {@code findByStatus} for {@code DISABLED} and both purge/reenable sweeps are served by the
 * partial index {@code idx_events_disabled_created_at} (migration V003).
 *
 * <p>{@link #findInArchive} and {@link #purgeArchive} require the archive migration (V002) to be
 * applied; calling them without it surfaces as an {@link EventStoreException}.
 */
public final class PostgresOutboxAdmin implements OutboxAdmin {

    private static final String REENABLE_SET =
            " SET status = 'PENDING', attempts = 0, claimed_by = NULL, claimed_at = NULL, version ="
                    + " version + 1, last_fail_reason = 'reenabled by operator', run_at = now() ";

    private final OutboxJdbcRunner jdbc;
    private final SchemaResolver tables;

    public PostgresOutboxAdmin(
            ConnectionSupplier connections, PostgresStorageProperties properties) {
        this.jdbc = new OutboxJdbcRunner(Objects.requireNonNull(connections, "connections"));
        this.tables = new SchemaResolver(Objects.requireNonNull(properties, "properties"));
    }

    @Override
    public List<Event> findByStatus(
            EventStatus status,
            @Nullable String eventType,
            int limit,
            @Nullable AdminCursor after) {
        Objects.requireNonNull(status, "status must not be null");
        requirePositive(limit);
        StringBuilder sql =
                new StringBuilder(
                        "SELECT id, event_type, payload, payload_binary, payload_format,"
                            + " payload_class, priority, attempts, status, created_at, run_at,"
                            + " claimed_by, claimed_at, last_fail_reason, trace_context, version,"
                            + " dedup_key FROM ");
        sql.append(tables.events()).append(" WHERE status = ?");
        List<Object> params = new ArrayList<>();
        params.add(status.name());
        if (eventType != null) {
            sql.append(" AND event_type = ?");
            params.add(eventType);
        }
        if (after != null) {
            // Keyset: strictly after the cursor row in (created_at DESC, id DESC) order.
            sql.append(" AND (created_at, id) < (?, ?)");
            params.add(Timestamp.from(after.createdAt()));
            params.add(after.id());
        }
        sql.append(" ORDER BY created_at DESC, id DESC LIMIT ?");
        params.add(limit);
        try {
            return jdbc.queryList(sql.toString(), bind(params), PostgresEventStore::readEvent);
        } catch (SQLException ex) {
            throw new EventStoreException("findByStatus(" + status + ") failed", ex);
        }
    }

    @Override
    public Optional<ArchivedEvent> findInArchive(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        String sql =
                "SELECT id, event_type, payload, payload_binary, payload_format, payload_class,"
                    + " priority, attempts, created_at, run_at, last_fail_reason, trace_context,"
                    + " archived_at, archived_by FROM "
                        + tables.archive()
                        + " WHERE id = ?";
        try {
            return jdbc.queryOne(sql, ps -> ps.setObject(1, id), PostgresOutboxAdmin::readArchived);
        } catch (SQLException ex) {
            throw new EventStoreException("findInArchive(" + id + ") failed", ex);
        }
    }

    @Override
    public boolean reenable(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        String sql =
                "UPDATE " + tables.events() + REENABLE_SET + "WHERE id = ? AND status = 'DISABLED'";
        try {
            return jdbc.update(sql, ps -> ps.setObject(1, id)) > 0;
        } catch (SQLException ex) {
            throw new EventStoreException("reenable(" + id + ") failed", ex);
        }
    }

    @Override
    public int reenableAll(String eventType, @Nullable Instant createdBefore, int limit) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        requirePositive(limit);
        StringBuilder sub =
                new StringBuilder("SELECT id FROM ")
                        .append(tables.events())
                        .append(" WHERE status = 'DISABLED' AND event_type = ?");
        List<Object> params = new ArrayList<>();
        params.add(eventType);
        if (createdBefore != null) {
            sub.append(" AND created_at < ?");
            params.add(Timestamp.from(createdBefore));
        }
        sub.append(" LIMIT ?");
        params.add(limit);
        String sql = "UPDATE " + tables.events() + REENABLE_SET + "WHERE id IN (" + sub + ")";
        try {
            return jdbc.update(sql, bind(params));
        } catch (SQLException ex) {
            throw new EventStoreException("reenableAll(" + eventType + ") failed", ex);
        }
    }

    @Override
    public int purgeDisabled(@Nullable String eventType, Instant olderThan, int limit) {
        Objects.requireNonNull(olderThan, "olderThan must not be null");
        requirePositive(limit);
        StringBuilder sub =
                new StringBuilder("SELECT id FROM ")
                        .append(tables.events())
                        .append(" WHERE status = 'DISABLED' AND created_at < ?");
        List<Object> params = new ArrayList<>();
        params.add(Timestamp.from(olderThan));
        if (eventType != null) {
            sub.append(" AND event_type = ?");
            params.add(eventType);
        }
        sub.append(" LIMIT ?");
        params.add(limit);
        String sql = "DELETE FROM " + tables.events() + " WHERE id IN (" + sub + ")";
        try {
            return jdbc.update(sql, bind(params));
        } catch (SQLException ex) {
            throw new EventStoreException("purgeDisabled failed", ex);
        }
    }

    @Override
    public int purgeArchive(Instant archivedBefore, int limit) {
        Objects.requireNonNull(archivedBefore, "archivedBefore must not be null");
        requirePositive(limit);
        String sql =
                "DELETE FROM "
                        + tables.archive()
                        + " WHERE id IN (SELECT id FROM "
                        + tables.archive()
                        + " WHERE archived_at < ? LIMIT ?)";
        try {
            return jdbc.update(
                    sql,
                    ps -> {
                        ps.setTimestamp(1, Timestamp.from(archivedBefore));
                        ps.setInt(2, limit);
                    });
        } catch (SQLException ex) {
            throw new EventStoreException("purgeArchive failed", ex);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static OutboxJdbcRunner.ParameterBinder bind(List<Object> params) {
        return ps -> {
            for (int i = 0; i < params.size(); i++) {
                bindOne(ps, i + 1, params.get(i));
            }
        };
    }

    private static void bindOne(PreparedStatement ps, int index, Object value) throws SQLException {
        switch (value) {
            case String s -> ps.setString(index, s);
            case Timestamp t -> ps.setTimestamp(index, t);
            case Integer n -> ps.setInt(index, n);
            default -> ps.setObject(index, value);
        }
    }

    private static ArchivedEvent readArchived(ResultSet rs) throws SQLException {
        String traceJson = rs.getString("trace_context");
        Map<String, String> traceContext =
                traceJson == null || traceJson.isEmpty() ? Map.of() : FlatMapJson.parse(traceJson);
        return new ArchivedEvent(
                (UUID) rs.getObject("id"),
                rs.getString("event_type"),
                PostgresEventStore.readPayload(rs),
                rs.getString("payload_format"),
                rs.getString("payload_class"),
                rs.getShort("priority"),
                rs.getInt("attempts"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("run_at").toInstant(),
                rs.getString("last_fail_reason"),
                traceContext,
                rs.getTimestamp("archived_at").toInstant(),
                rs.getString("archived_by"));
    }

    private static void requirePositive(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
    }
}
