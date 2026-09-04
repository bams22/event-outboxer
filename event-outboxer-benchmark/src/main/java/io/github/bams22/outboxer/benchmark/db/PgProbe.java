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
import java.util.Objects;

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

    /** Rows left in {@code <schema>.events} and {@code <schema>.entity_locks}. */
    public StorageState storageState(String schema) {
        try (Connection c = open()) {
            long events = count(c, schema + ".events");
            long locks =
                    exists(c, schema + ".entity_locks") ? count(c, schema + ".entity_locks") : -1;
            return new StorageState(events, locks);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect schema " + schema, e);
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
