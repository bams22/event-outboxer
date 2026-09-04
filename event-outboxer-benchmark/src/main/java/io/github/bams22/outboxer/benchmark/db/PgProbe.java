/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.jspecify.annotations.Nullable;

/**
 * Read-only questions to PostgreSQL that the report needs: server version, write counters of the
 * outbox schema, and what is left in it after the drain. Opens a fresh connection per call so every
 * read sees current statistics rather than a cached snapshot.
 *
 * <p>Statistics are flushed by backends asynchronously (about once a second on PostgreSQL 15), so
 * the caller lets the fleet settle before the closing sample; {@link #tableWrites} does not sleep
 * itself.
 */
public final class PgProbe {

    private final DatabaseCoordinates coordinates;

    public PgProbe(DatabaseCoordinates coordinates) {
        this.coordinates = Objects.requireNonNull(coordinates, "coordinates must not be null");
    }

    /** {@code SHOW server_version}. */
    public String serverVersion() {
        try (Connection c = open();
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SHOW server_version")) {
            return rs.next() ? rs.getString(1) : "unknown";
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read PostgreSQL version", e);
        }
    }

    /** Row-write counters summed over every table of {@code schema}. */
    public TableWrites tableWrites(String schema) {
        String sql =
                "SELECT coalesce(sum(n_tup_ins), 0), coalesce(sum(n_tup_upd), 0),"
                    + " coalesce(sum(n_tup_del), 0) FROM pg_stat_user_tables WHERE schemaname = ?";
        try (Connection c = open();
                PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return new TableWrites(rs.getLong(1), rs.getLong(2), rs.getLong(3));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read pg_stat_user_tables for " + schema, e);
        }
    }

    /**
     * Bytes of WAL written since server start ({@code pg_current_wal_lsn} as an offset). Two
     * samples bracket a run; the difference is the write-ahead volume the run generated, every
     * table and index included.
     */
    public long walBytes() {
        try (Connection c = open();
                Statement st = c.createStatement();
                ResultSet rs =
                        st.executeQuery("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')")) {
            rs.next();
            return rs.getBigDecimal(1).longValue();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read the WAL position", e);
        }
    }

    /** {@code pg_total_relation_size} of a table: heap, indexes and TOAST. */
    public long relationBytes(String qualifiedTable) {
        try (Connection c = open();
                PreparedStatement ps = c.prepareStatement("SELECT pg_total_relation_size(?)")) {
            ps.setString(1, qualifiedTable);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read the size of " + qualifiedTable, e);
        }
    }

    /**
     * {@code VACUUM FULL} on a table if it exists: rewrites heap and indexes without the dead
     * tuples a previous run left behind. Without it the next run claims through a bloated index and
     * reports a table size that includes the previous run's garbage — the run order would
     * masquerade as a difference between variants. The harness assumes a dedicated database.
     *
     * @return {@code true} when the table existed and was vacuumed
     */
    public boolean vacuumFull(String qualifiedTable) {
        try (Connection c = open()) {
            if (!exists(c, qualifiedTable)) {
                return false;
            }
            try (Statement st = c.createStatement()) {
                st.execute("VACUUM FULL " + qualifiedTable);
            }
            return true;
        } catch (SQLException e) {
            throw new IllegalStateException("VACUUM FULL failed for " + qualifiedTable, e);
        }
    }

    /**
     * Makes {@code pg_stat_statements} available if the server preloads it: creates the extension
     * when the role may, and reports whether the view can be read. {@code false} means the
     * statement figures stay out of the report — the module needs {@code shared_preload_libraries =
     * 'pg_stat_statements'}, a server setting the harness cannot change.
     */
    public boolean enableStatementStats() {
        try (Connection c = open();
                Statement st = c.createStatement()) {
            try {
                st.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            } catch (SQLException ignored) {
                // Not preloaded, or the role may not create extensions; probe the view below.
            }
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_stat_statements")) {
                return rs.next();
            }
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Calls and rows per statement class for statements touching {@code schema}, cumulative since
     * the last reset. Only meaningful after {@link #enableStatementStats()} returned true.
     */
    public StatementStats statementStats(String schema) {
        Map<String, Long> calls = new TreeMap<>();
        Map<String, Long> rows = new TreeMap<>();
        String sql = "SELECT query, calls, rows FROM pg_stat_statements WHERE query LIKE ?";
        try (Connection c = open();
                PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, "%" + schema + ".%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String cls = classify(rs.getString(1), schema);
                    calls.merge(cls, rs.getLong(2), Long::sum);
                    rows.merge(cls, rs.getLong(3), Long::sum);
                }
            }
            return new StatementStats(calls, rows);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read pg_stat_statements", e);
        }
    }

    private static String classify(String query, String schema) {
        String q = query.replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT);
        String events = schema.toLowerCase(Locale.ROOT) + ".events";
        if (q.startsWith("with picked as")) {
            return "claim";
        }
        if (q.startsWith("insert into " + events)) {
            return "insert";
        }
        if (q.startsWith("delete from " + events + " e using (values")
                || (q.startsWith("with") && q.contains("event_archive") && q.contains("values"))) {
            return "finalizeBatch";
        }
        if (q.startsWith("delete from " + events + " where id = ")
                || (q.startsWith("with") && q.contains("event_archive"))) {
            return "finalizeSingle";
        }
        // pg_stat_statements normalises literals to $n (and may keep some), so the UPDATE shapes
        // are told apart by their SET lists rather than by the status value.
        java.util.regex.Matcher m =
                java.util.regex.Pattern.compile(
                                "^update "
                                        + java.util.regex.Pattern.quote(events)
                                        + "( e)? set status = \\S+, (.*)$")
                        .matcher(q);
        if (m.matches()) {
            String rest = m.group(2);
            if (rest.contains("where id in (select")) {
                return "other"; // stale-claim sweep
            }
            if (rest.contains("attempts = attempts + ")) {
                return "retry"; // markForRetry, markForRetryAll, orphan reclaim
            }
            if (rest.contains("run_at = ")) {
                return "release";
            }
            if (rest.contains("last_fail_reason")) {
                return "disabled";
            }
        }
        return "other";
    }

    /** Blocks until a connection succeeds or {@code timeout} passes. */
    public void awaitReady(Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        SQLException last = null;
        while (Instant.now().isBefore(deadline)) {
            try (Connection c = open();
                    Statement st = c.createStatement()) {
                st.execute("SELECT 1");
                return;
            } catch (SQLException e) {
                last = e;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for PostgreSQL", ie);
                }
            }
        }
        throw new IllegalStateException("PostgreSQL did not come back within " + timeout, last);
    }

    /** Rows left in the events table and live leases in the lease table after a clean run. */
    public StorageState storageState(String eventsTable, @Nullable String leaseTable) {
        return storageState(eventsTable, leaseTable, List.of(), null);
    }

    /**
     * Rows left after a run with chaos. Leases are counted only while live ({@code expires_at >
     * now()}), and a lease is not held against the run when its owner was killed or when it was
     * acquired before a database outage — nobody could have released either; they expire.
     *
     * @param ignoredLeaseOwners worker ids that were {@code SIGKILL}ed
     * @param ignoreLeasesAcquiredBefore moment of the last database restart, {@code null} = none
     */
    public StorageState storageState(
            String eventsTable,
            @Nullable String leaseTable,
            Collection<String> ignoredLeaseOwners,
            @Nullable Instant ignoreLeasesAcquiredBefore) {
        try (Connection c = open()) {
            long events = count(c, eventsTable);
            long locks =
                    leaseTable != null && exists(c, leaseTable)
                            ? liveLeases(
                                    c, leaseTable, ignoredLeaseOwners, ignoreLeasesAcquiredBefore)
                            : -1;
            return new StorageState(events, locks);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect " + eventsTable, e);
        }
    }

    private static long liveLeases(
            Connection c,
            String table,
            Collection<String> ignoredOwners,
            @Nullable Instant ignoreAcquiredBefore)
            throws SQLException {
        StringBuilder sql =
                new StringBuilder("SELECT count(*) FROM " + table + " WHERE expires_at > now()");
        List<String> owners = List.copyOf(ignoredOwners);
        if (!owners.isEmpty()) {
            sql.append(" AND (owner_worker IS NULL OR owner_worker <> ALL (?))");
        }
        if (ignoreAcquiredBefore != null) {
            sql.append(" AND acquired_at >= ?");
        }
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
            int i = 1;
            if (!owners.isEmpty()) {
                ps.setArray(i++, c.createArrayOf("varchar", owners.toArray()));
            }
            if (ignoreAcquiredBefore != null) {
                ps.setTimestamp(i, java.sql.Timestamp.from(ignoreAcquiredBefore));
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static boolean exists(Connection c, String qualifiedTable) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT to_regclass(?)")) {
            ps.setString(1, qualifiedTable);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getString(1) != null;
            }
        }
    }

    private static long count(Connection c, String qualifiedTable) throws SQLException {
        // Identifier, not a value: the schema name comes from the harness, not from user input.
        try (Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM " + qualifiedTable)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(
                coordinates.jdbcUrl(), coordinates.username(), coordinates.password());
    }
}
