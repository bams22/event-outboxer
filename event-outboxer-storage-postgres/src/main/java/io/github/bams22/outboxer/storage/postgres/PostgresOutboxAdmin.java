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

import static io.github.bams22.outboxer.storage.postgres.internal.EventRows.ARCHIVE_COLUMNS;
import static io.github.bams22.outboxer.storage.postgres.internal.EventRows.EVENT_COLUMNS;
import static io.github.bams22.outboxer.storage.postgres.internal.EventRows.IMMUTABLE_COLUMNS;

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.exception.EventStoreException;
import io.github.bams22.outboxer.spi.AdminCursor;
import io.github.bams22.outboxer.spi.ArchiveCursor;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.storage.postgres.internal.EventRows;
import io.github.bams22.outboxer.storage.postgres.internal.OutboxJdbcRunner;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL {@link OutboxAdmin}. Query shapes are documented in STORAGE.md §Admin operations;
 * {@code findByStatus} for {@code DISABLED} and both purge/reenable sweeps are served by the
 * partial index {@code idx_events_disabled_created_at} (migration V003).
 *
 * <p>A sibling of {@link PostgresEventStore} over the same schema, not a layer above it: the
 * operations here (a {@code DISABLED} row back to {@code PENDING}, an archive row back to the hot
 * table, paged listings, deletes by age) have no counterpart in the {@code EventStore} port, so the
 * admin carries its own statements. What the two classes share they share through {@link EventRows}
 * — the column lists and row mappers — and through the {@link MetricsSnapshotCache}: the store
 * serves {@code metricsSnapshot()} from that cache for up to its TTL, and every mutation here that
 * changed rows invalidates it, so a re-enable, purge or replay shows on the next scrape rather than
 * after the TTL. {@link #purgeArchive} does not invalidate: the snapshot does not cover the
 * archive.
 *
 * <p>That freshness is a property of this adapter pair, not of the {@code OutboxAdmin} port — no
 * other implementation is required to invalidate anything, and the in-memory admin used by the
 * starter's test configuration does not. How far it reaches depends on the cache: the default
 * per-JVM cache is invalidated only on the replica that served the admin call, while {@code
 * cache.type=redis} makes it fleet-wide. When the admin runs inside a caller transaction the
 * invalidation belongs after the commit, which the Spring Boot starter arranges for the admin bean
 * it wires; plain-Java wiring that calls the admin inside its own transaction should invalidate
 * once it has committed.
 *
 * <p>{@link #findInArchive} and {@link #purgeArchive} require the archive migration (V002) to be
 * applied; calling them without it surfaces as an {@link EventStoreException}.
 */
public final class PostgresOutboxAdmin implements OutboxAdmin {

    private static final Logger log = LoggerFactory.getLogger(PostgresOutboxAdmin.class);

    private static final String REENABLE_SET =
            " SET status = 'PENDING', attempts = 0, claimed_by = NULL, claimed_at = NULL, version ="
                    + " version + 1, last_fail_reason = 'reenabled by operator', run_at = now() ";

    private static final String REPLAY_REASON = "replayed from archive";

    private final OutboxJdbcRunner jdbc;
    private final SchemaResolver tables;
    private final MetricsSnapshotCache metricsCache;

    /**
     * @param metricsCache the cache the {@link PostgresEventStore} of the same schema serves its
     *     {@code metricsSnapshot()} from — pass the same instance, or {@link
     *     MetricsSnapshotCache#noop()} when the store does not cache
     */
    public PostgresOutboxAdmin(
            ConnectionSupplier connections,
            PostgresStorageProperties properties,
            MetricsSnapshotCache metricsCache) {
        this.jdbc = new OutboxJdbcRunner(Objects.requireNonNull(connections, "connections"));
        this.tables = new SchemaResolver(Objects.requireNonNull(properties, "properties"));
        this.metricsCache = Objects.requireNonNull(metricsCache, "metricsCache must not be null");
    }

    @Override
    public List<Event> findByStatus(
            EventStatus status,
            @Nullable String eventType,
            int limit,
            @Nullable AdminCursor after) {
        Objects.requireNonNull(status, "status must not be null");
        requirePositive(limit);
        StringBuilder sql = new StringBuilder("SELECT " + EVENT_COLUMNS + " FROM ");
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
            return jdbc.queryList(sql.toString(), bind(params), EventRows::readEvent);
        } catch (SQLException ex) {
            throw new EventStoreException("findByStatus(" + status + ") failed", ex);
        }
    }

    @Override
    public Optional<ArchivedEvent> findInArchive(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        String sql = "SELECT " + ARCHIVE_COLUMNS + " FROM " + tables.archive() + " WHERE id = ?";
        try {
            return jdbc.queryOne(sql, ps -> ps.setObject(1, id), EventRows::readArchived);
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
            int rows = jdbc.update(sql, ps -> ps.setObject(1, id));
            invalidateIfChanged(rows);
            return rows > 0;
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
            int rows = jdbc.update(sql, bind(params));
            invalidateIfChanged(rows);
            return rows;
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
            int rows = jdbc.update(sql, bind(params));
            invalidateIfChanged(rows);
            return rows;
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
            invalidateIfChanged(counts.inserted());
            if (counts.inserted() > 0) {
                return ReplayOutcome.REPLAYED;
            }
            return ReplayOutcome.ID_IN_USE;
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
        // Oldest-archived first, so the cursor walks the window in a stable order.
        where.append(" ORDER BY archived_at, id LIMIT ?");
        params.add(limit);
        try {
            ReplayCounts counts =
                    jdbc.queryOne(
                                    replaySql(where.toString()),
                                    bind(params),
                                    PostgresOutboxAdmin::readCounts)
                            .orElseThrow();
            invalidateIfChanged(counts.inserted());
            return new ReplayAllResult(counts.inserted(), counts.idInUse(), counts.cursor());
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
     * <p>One thing can stop a row from moving, and it is not fatal to the batch: <b>an id already
     * live</b> — the {@code blocked} anti-join. Without it a single archive row whose id the
     * application re-published would abort the whole statement, replaying none of the batch and
     * leaving the sweep permanently stuck on that window. Excluding those ids up front turns a
     * fatal batch abort into a counted, skipped row. (A concurrent publish of the same id after
     * this statement's snapshot still raises — that is a genuine race, and it surfaces rather than
     * passing silently.) A dedup key never blocks a replay (ADR-0037): the row inserts plainly, and
     * if a due {@code PENDING} event with the same key exists the next claim collapses the two.
     *
     * <p>{@code src} is {@code MATERIALIZED} so that the cursor columns below describe the same
     * batch the INSERT consumed.
     *
     * <p>{@code created_at} is the moment of the replay, not the original publish time: it is the
     * column {@link #purgeDisabled} ages rows by, {@link #reenableAll} bounds on and {@link
     * #findByStatus} pages by, so an archived-long-ago event re-entered with its old timestamp
     * would be purged by the next retention sweep and would land on the last admin page. The
     * original publish time stays readable in the archive until the row is actually moved.
     *
     * <p>The trailing {@code cursor_*} columns carry the {@code (archived_at, id)} of the last row
     * the batch considered — replayed or skipped alike — so the caller's sweep advances past rows
     * that stayed archived instead of finding them again forever.
     */
    private String replaySql(String srcWhere) {
        return "WITH src AS MATERIALIZED ("
                + "  SELECT "
                + IMMUTABLE_COLUMNS
                + ", archived_at FROM "
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
                + " ("
                + IMMUTABLE_COLUMNS
                + ", attempts, status, created_at, run_at, last_fail_reason, version)"
                + "  SELECT "
                + IMMUTABLE_COLUMNS
                + ", 0, 'PENDING', now(), now(), '"
                + REPLAY_REASON
                + "', 0 FROM src"
                + "  WHERE id NOT IN (SELECT id FROM blocked)"
                + "  RETURNING id"
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

    /**
     * Every mutation of the hot table reports its row count through here: when rows changed, the
     * shared {@link MetricsSnapshotCache} is invalidated so that the store's next {@code
     * metricsSnapshot()} recomputes instead of serving the pre-mutation counts until the TTL. A
     * sweep that matched nothing leaves the cache alone.
     *
     * <p>The row change is already applied when this runs, so a cache that throws must not turn a
     * successful operation into a failed one — the caller would retry a replay that had in fact
     * moved the row, and a retention sweep would abort mid-pass. {@link
     * MetricsSnapshotCache#invalidate()} is contractually forbidden from propagating backend
     * failures; this guard is what keeps a custom implementation that ignores the contract from
     * costing correctness. Stale gauges for up to one TTL are the whole downside.
     *
     * <p>A bulk sweep invalidates once per batch, not once per pass: {@code RetentionTask} calls
     * {@link #purgeDisabled} in a loop, and each batch genuinely changes the counts, so the cache
     * would be serving numbers it knows to be wrong if the invalidation waited for the last one.
     * The cost of a long catch-up pass is one extra invalidation per batch.
     */
    private void invalidateIfChanged(int rows) {
        if (rows <= 0) {
            return;
        }
        try {
            metricsCache.invalidate();
        } catch (RuntimeException ex) {
            log.warn(
                    "metrics snapshot cache invalidation failed after an admin mutation of {}"
                        + " row(s); backlog gauges may stay stale for up to one metrics-cache-ttl:"
                        + " {}",
                    rows,
                    ex.toString());
        }
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
