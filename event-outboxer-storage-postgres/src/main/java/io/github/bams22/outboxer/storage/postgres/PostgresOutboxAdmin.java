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
import io.github.bams22.outboxer.spi.ArchiveCursor;
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

    private static final String REPLAY_REASON = "replayed from archive";

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
                    + " archived_at, archived_by, dedup_key FROM "
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

    @Override
    public ReplayOutcome replayFromArchive(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        String sql = replaySql("WHERE id = ?");
        try {
            ReplayCounts counts =
                    jdbc.queryOne(sql, ps -> ps.setObject(1, id), PostgresOutboxAdmin::readCounts)
                            .orElseThrow();
            if (counts.found() == 0) {
                return ReplayOutcome.NOT_FOUND;
            }
            if (counts.inserted() > 0) {
                return ReplayOutcome.REPLAYED;
            }
            return counts.idInUse() > 0 ? ReplayOutcome.ID_IN_USE : ReplayOutcome.COALESCED;
        } catch (SQLException ex) {
            throw new EventStoreException("replayFromArchive(" + id + ") failed", ex);
        }
    }

    @Override
    public ReplayAllResult replayAllFromArchive(
            String eventType,
            @Nullable Instant archivedAfter,
            @Nullable Instant archivedBefore,
            int limit,
            @Nullable ArchiveCursor after) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        requirePositive(limit);
        requireWindow(archivedAfter, archivedBefore);
        StringBuilder where = new StringBuilder("WHERE event_type = ?");
        List<Object> params = new ArrayList<>();
        params.add(eventType);
        if (archivedAfter != null) {
            where.append(" AND archived_at > ?");
            params.add(Timestamp.from(archivedAfter));
        }
        if (archivedBefore != null) {
            where.append(" AND archived_at < ?");
            params.add(Timestamp.from(archivedBefore));
        }
        if (after != null) {
            // Keyset on the pair, not on archived_at alone: rows can share an archived_at, and a
            // timestamp-only cursor would skip whichever tied row the previous LIMIT cut off.
            where.append(" AND (archived_at, id) > (?, ?)");
            params.add(Timestamp.from(after.archivedAt()));
            params.add(after.id());
        }
        // Oldest-archived first: with duplicate dedup keys in one batch, the oldest row replays
        // and the newer ones coalesce against it — deterministic instead of scan-order luck.
        where.append(" ORDER BY archived_at, id LIMIT ?");
        params.add(limit);
        try {
            ReplayCounts counts =
                    jdbc.queryOne(
                                    replaySql(where.toString()),
                                    bind(params),
                                    PostgresOutboxAdmin::readCounts)
                            .orElseThrow();
            return new ReplayAllResult(
                    counts.inserted(),
                    counts.found() - counts.inserted() - counts.idInUse(),
                    counts.idInUse(),
                    counts.cursor());
        } catch (SQLException ex) {
            throw new EventStoreException("replayAllFromArchive(" + eventType + ") failed", ex);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    /**
     * Single-statement replay (ADR-0033): the hot-table INSERT and the archive DELETE run as one
     * atomic CTE, and the INSERT comes first — {@code del} deletes only the ids the INSERT actually
     * returned, so a row that does not move stays archived. A delete-first variant would silently
     * drop the audit row.
     *
     * <p>Two things can stop a row from moving, and neither is fatal to the batch:
     *
     * <ul>
     *   <li><b>Coalescing</b> — {@code ON CONFLICT ... DO NOTHING} on the V004 partial index (the
     *       same arbiter clause the publisher's insert uses): a live {@code PENDING} row with the
     *       same {@code (event_type, dedup_key)} already has the work scheduled. Duplicate keys
     *       within one batch resolve the same way, the second row's speculative insert conflicting
     *       against the first's.
     *   <li><b>Id already live</b> — the {@code blocked} anti-join. The arbiter above names one
     *       index, so a primary-key collision is <em>not</em> swallowed by it: without the
     *       anti-join a single archive row whose id the application re-published would abort the
     *       whole statement, replaying none of the batch and leaving the sweep permanently stuck on
     *       that window. Excluding those ids up front turns a fatal batch abort into a counted,
     *       skipped row. (A concurrent publish of the same id after this statement's snapshot still
     *       raises — that is a genuine race, and it surfaces rather than passing silently.)
     * </ul>
     *
     * <p>{@code src} is {@code MATERIALIZED} on purpose: the "oldest archived row wins a duplicate
     * key" guarantee relies on the INSERT consuming it in {@code (archived_at, id)} order, which
     * only holds for a materialized CTE — an inlined one may be reordered by the planner.
     *
     * <p>{@code created_at} is the moment of the replay, not the original publish time: it is the
     * column {@link #purgeDisabled} ages rows by, {@link #reenableAll} bounds on and {@link
     * #findByStatus} pages by, so an archived-long-ago event re-entered with its old timestamp
     * would be purged by the next retention sweep and would land on the last admin page. The
     * original publish time stays readable in the archive until the row is actually moved.
     *
     * <p>The trailing {@code cursor_*} columns carry the {@code (archived_at, id)} of the last row
     * the batch considered — replayed, coalesced or skipped alike — so the caller's sweep advances
     * past rows that stayed archived instead of finding them again forever.
     */
    private String replaySql(String srcWhere) {
        return "WITH src AS MATERIALIZED ("
                + "  SELECT id, event_type, payload, payload_binary, payload_format,"
                + " payload_class, priority, trace_context, dedup_key, archived_at FROM "
                + tables.archive()
                + " "
                + srcWhere
                + "), blocked AS ("
                + "  SELECT s.id FROM src s WHERE EXISTS (SELECT 1 FROM "
                + tables.events()
                + " e WHERE e.id = s.id)"
                + "), ins AS ("
                + "  INSERT INTO "
                + tables.events()
                + " (id, event_type, payload, payload_binary, payload_format, payload_class,"
                + " priority, attempts, status, created_at, run_at, last_fail_reason,"
                + " trace_context, version, dedup_key)"
                + "  SELECT id, event_type, payload, payload_binary, payload_format,"
                + " payload_class, priority, 0, 'PENDING', now(), now(), '"
                + REPLAY_REASON
                + "', trace_context, 0, dedup_key FROM src"
                + "  WHERE id NOT IN (SELECT id FROM blocked)"
                + "  ON CONFLICT (event_type, dedup_key) WHERE status = 'PENDING' AND dedup_key IS"
                + " NOT NULL DO NOTHING  RETURNING id"
                + "), del AS ("
                + "  DELETE FROM "
                + tables.archive()
                + " a USING ins WHERE a.id = ins.id RETURNING a.id"
                + ") SELECT (SELECT count(*) FROM src) AS found,"
                + " (SELECT count(*) FROM ins) AS inserted,"
                + " (SELECT count(*) FROM blocked) AS id_in_use,"
                + " (SELECT archived_at FROM src ORDER BY archived_at DESC, id DESC LIMIT 1)"
                + " AS cursor_archived_at,"
                + " (SELECT id FROM src ORDER BY archived_at DESC, id DESC LIMIT 1) AS cursor_id";
    }

    private static ReplayCounts readCounts(ResultSet rs) throws SQLException {
        Timestamp cursorAt = rs.getTimestamp("cursor_archived_at");
        UUID cursorId = rs.getObject("cursor_id", UUID.class);
        return new ReplayCounts(
                rs.getInt("found"),
                rs.getInt("inserted"),
                rs.getInt("id_in_use"),
                cursorAt == null || cursorId == null
                        ? null
                        : new ArchiveCursor(cursorAt.toInstant(), cursorId));
    }

    /**
     * Counter row of the replay CTE's final SELECT: archive rows matched, actually inserted, and
     * skipped because their id is already live, plus the cursor of the last row considered.
     */
    private record ReplayCounts(
            int found, int inserted, int idInUse, @Nullable ArchiveCursor cursor) {}

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
                rs.getString("archived_by"),
                rs.getString("dedup_key"));
    }

    private static void requirePositive(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
    }

    /**
     * Both window bounds are exclusive, so a window whose lower bound is not strictly below its
     * upper bound cannot match a row. Rejecting it is the difference between "you swapped the
     * dates" and a zeroed result an operator reads as "this window was already replayed".
     */
    private static void requireWindow(@Nullable Instant after, @Nullable Instant before) {
        if (after != null && before != null && !after.isBefore(before)) {
            throw new IllegalArgumentException(
                    "archivedAfter must be strictly before archivedBefore, got "
                            + after
                            + " and "
                            + before);
        }
    }
}
