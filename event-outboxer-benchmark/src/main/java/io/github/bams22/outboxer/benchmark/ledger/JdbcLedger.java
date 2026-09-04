/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.ledger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.bams22.outboxer.benchmark.db.DatabaseCoordinates;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Ledger for the forked fleet: the {@code bench.handled} table in the benchmark database, one row
 * per handler invocation. Handlers in other JVMs write it, the driver reads it.
 *
 * <p>The table lives in its own {@code bench} schema so its writes never count towards the outbox
 * schema's {@code pg_stat} figures. Each JVM owns a small HikariCP pool of its own — the library's
 * pool and its metrics stay untouched, and Hikari's reconnect logic carries the ledger through a
 * PostgreSQL restart.
 *
 * <p>A failed {@link #record} propagates: for the harness the ledger row <em>is</em> the handler's
 * side effect, so an invocation that could not be recorded must be retried by the library, not
 * silently counted. Reads fail while the database is down; the driver's drain loop tolerates that.
 */
public final class JdbcLedger implements Ledger, AutoCloseable {

    /** Fully qualified table name. */
    public static final String TABLE = "bench.handled";

    private static final String INSERT =
            "INSERT INTO "
                    + TABLE
                    + " (seq, event_type, attempt, worker_id, thread, lock_key, started_at,"
                    + " finished_at, outcome) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SELECT_ALL =
            "SELECT seq, event_type, attempt, worker_id, thread, lock_key, started_at, finished_at,"
                    + " outcome FROM "
                    + TABLE;

    private final HikariDataSource pool;

    /**
     * Opens a pool of {@code poolSize} connections. Call {@link #install()} once from the driver
     * before any writer starts.
     */
    public JdbcLedger(DatabaseCoordinates database, int poolSize, String poolName) {
        Objects.requireNonNull(database, "database must not be null");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(database.username());
        config.setPassword(database.password());
        config.setMaximumPoolSize(Math.max(1, poolSize));
        config.setPoolName(poolName);
        config.setAutoCommit(true);
        this.pool = new HikariDataSource(config);
    }

    /**
     * Creates the schema and table if needed and empties the table, so an external database can be
     * reused across runs.
     */
    public void install() {
        try (Connection c = pool.getConnection();
                Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS bench");
            st.execute(
                    "CREATE TABLE IF NOT EXISTS "
                            + TABLE
                            + " (id BIGSERIAL PRIMARY KEY, seq BIGINT NOT NULL, event_type TEXT NOT"
                            + " NULL, attempt INT NOT NULL, worker_id TEXT NOT NULL, thread TEXT"
                            + " NOT NULL, lock_key TEXT, started_at TIMESTAMPTZ NOT NULL,"
                            + " finished_at TIMESTAMPTZ NOT NULL, outcome TEXT NOT NULL)");
            st.execute(
                    "CREATE INDEX IF NOT EXISTS handled_outcome_seq ON "
                            + TABLE
                            + " (outcome, seq)");
            st.execute("TRUNCATE " + TABLE);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot install the ledger table " + TABLE, e);
        }
    }

    @Override
    public void record(Handling h) {
        Objects.requireNonNull(h, "handling must not be null");
        try (Connection c = pool.getConnection();
                PreparedStatement ps = c.prepareStatement(INSERT)) {
            ps.setLong(1, h.seq());
            ps.setString(2, h.eventType());
            ps.setInt(3, h.attempt());
            ps.setString(4, h.workerId());
            ps.setString(5, h.thread());
            ps.setString(6, h.lockKey());
            ps.setTimestamp(7, Timestamp.from(h.startedAt()));
            ps.setTimestamp(8, Timestamp.from(h.finishedAt()));
            ps.setString(9, h.outcome().name());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Ledger insert failed for seq " + h.seq(), e);
        }
    }

    @Override
    public long distinctSuccesses() {
        return scalar("SELECT count(DISTINCT seq) FROM " + TABLE + " WHERE outcome = 'SUCCESS'");
    }

    @Override
    public long total() {
        return scalar("SELECT count(*) FROM " + TABLE);
    }

    @Override
    public List<Handling> snapshot() {
        List<Handling> out = new ArrayList<>();
        try (Connection c = pool.getConnection();
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(SELECT_ALL)) {
            while (rs.next()) {
                out.add(
                        Handling.builder()
                                .seq(rs.getLong(1))
                                .eventType(rs.getString(2))
                                .attempt(rs.getInt(3))
                                .workerId(rs.getString(4))
                                .thread(rs.getString(5))
                                .lockKey(rs.getString(6))
                                .startedAt(instant(rs, 7))
                                .finishedAt(instant(rs, 8))
                                .outcome(Handling.Outcome.valueOf(rs.getString(9)))
                                .build());
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read the ledger table " + TABLE, e);
        }
    }

    @Override
    public void close() {
        pool.close();
    }

    private long scalar(String sql) {
        try (Connection c = pool.getConnection();
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException("Ledger query failed: " + sql, e);
        }
    }

    private static Instant instant(ResultSet rs, int column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
