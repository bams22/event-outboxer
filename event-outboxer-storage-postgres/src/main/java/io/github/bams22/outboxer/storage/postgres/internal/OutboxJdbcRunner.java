/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres.internal;

import io.github.bams22.outboxer.spi.ConnectionSupplier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Minimalist JDBC helper used by the PostgreSQL adapter. Holds no per-call state, every method is
 * thread-safe. Connection lifecycle is delegated to {@link ConnectionSupplier}: obtain via {@link
 * ConnectionSupplier#get()}, close the {@link PreparedStatement}, release via {@link
 * ConnectionSupplier#release(Connection)}. The supplier is responsible for honouring the caller's
 * transaction (ADR-0002) — in a tx context {@code release} is a no-op.
 *
 * <p>Statements are built with {@code ?} positional placeholders; callers bind via the supplied
 * {@link ParameterBinder}. SQLException is surfaced to callers as a raw {@code SQLException}; the
 * domain layer (event store, worker registry) is responsible for wrapping it into the appropriate
 * {@code StorageException} subclass.
 */
public final class OutboxJdbcRunner {

  private final ConnectionSupplier connections;

  public OutboxJdbcRunner(ConnectionSupplier connections) {
    this.connections = Objects.requireNonNull(connections, "connections must not be null");
  }

  /** Run a query that returns at most one row. */
  public <T> Optional<T> queryOne(String sql, ParameterBinder binder, ResultSetMapper<T> mapper)
      throws SQLException {
    Connection conn = connections.get();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      binder.bind(ps);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
        return Optional.of(mapper.map(rs));
      }
    } finally {
      connections.release(conn);
    }
  }

  /** Run a query that may return any number of rows. */
  public <T> List<T> queryList(String sql, ParameterBinder binder, ResultSetMapper<T> mapper)
      throws SQLException {
    Connection conn = connections.get();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      binder.bind(ps);
      try (ResultSet rs = ps.executeQuery()) {
        List<T> out = new ArrayList<>();
        while (rs.next()) {
          out.add(mapper.map(rs));
        }
        return out;
      }
    } finally {
      connections.release(conn);
    }
  }

  /** Run an INSERT / UPDATE / DELETE; returns the number of affected rows. */
  public int update(String sql, ParameterBinder binder) throws SQLException {
    Connection conn = connections.get();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      binder.bind(ps);
      return ps.executeUpdate();
    } finally {
      connections.release(conn);
    }
  }

  /**
   * Run an INSERT / UPDATE / DELETE with a RETURNING clause and map each returned row. Uses {@code
   * executeQuery} rather than {@code executeUpdate} — PostgreSQL treats {@code ... RETURNING} as a
   * query.
   */
  public <T> List<T> updateReturning(String sql, ParameterBinder binder, ResultSetMapper<T> mapper)
      throws SQLException {
    Connection conn = connections.get();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      binder.bind(ps);
      try (ResultSet rs = ps.executeQuery()) {
        List<T> out = new ArrayList<>();
        while (rs.next()) {
          out.add(mapper.map(rs));
        }
        return out;
      }
    } finally {
      connections.release(conn);
    }
  }

  /** Run a one-shot statement (batch INSERT, DDL). */
  public void execute(String sql) throws SQLException {
    Connection conn = connections.get();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.execute();
    } finally {
      connections.release(conn);
    }
  }

  /**
   * Run a callback against a freshly-leased (or already transactional) connection. Callers may
   * issue multiple {@link PreparedStatement}s against the same connection — useful for
   * archive-on-success, where the INSERT and DELETE must be atomic with respect to each other.
   */
  public <T> @Nullable T doWork(ConnectionCallback<T> callback) throws SQLException {
    Connection conn = connections.get();
    try {
      return callback.run(conn);
    } finally {
      connections.release(conn);
    }
  }

  /**
   * Like {@link #doWork(ConnectionCallback)} but wraps the callback in a local transaction <em>if
   * and only if</em> the connection was not already in one. Preserves the caller's existing
   * transaction untouched (the "participate in the caller's transaction" contract of ADR-0002) —
   * when the adapter discovers an active transaction it assumes the caller is managing commit /
   * rollback and only executes the work.
   */
  public <T> @Nullable T doWorkInTransaction(ConnectionCallback<T> callback) throws SQLException {
    Connection conn = connections.get();
    boolean owned = false;
    boolean restoreAutoCommit = false;
    try {
      if (conn.getAutoCommit()) {
        conn.setAutoCommit(false);
        owned = true;
        restoreAutoCommit = true;
      }
      try {
        T result = callback.run(conn);
        if (owned) {
          conn.commit();
        }
        return result;
      } catch (SQLException | RuntimeException ex) {
        if (owned) {
          try {
            conn.rollback();
          } catch (SQLException rollbackEx) {
            ex.addSuppressed(rollbackEx);
          }
        }
        throw ex;
      } finally {
        if (restoreAutoCommit) {
          try {
            conn.setAutoCommit(true);
          } catch (SQLException _) {
            // best-effort restore; the connection is about to be released anyway
          }
        }
      }
    } finally {
      connections.release(conn);
    }
  }

  /** Callback that runs against a single JDBC connection. */
  @FunctionalInterface
  public interface ConnectionCallback<T> {
    @Nullable T run(Connection conn) throws SQLException;
  }

  /** Binds positional parameters to a {@link PreparedStatement}. */
  @FunctionalInterface
  public interface ParameterBinder {
    void bind(PreparedStatement ps) throws SQLException;

    /** No-op binder for statements with no parameters. */
    ParameterBinder EMPTY = ps -> {};
  }

  /** Maps a {@link ResultSet} row to a domain object. */
  @FunctionalInterface
  public interface ResultSetMapper<T> {
    T map(ResultSet rs) throws SQLException;
  }
}
