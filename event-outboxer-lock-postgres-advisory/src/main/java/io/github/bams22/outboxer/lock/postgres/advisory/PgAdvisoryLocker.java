/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.postgres.advisory;

import io.github.bams22.outboxer.domain.exception.LockAcquisitionException;
import io.github.bams22.outboxer.domain.exception.LockReleaseException;
import io.github.bams22.outboxer.spi.EntityLocker;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link EntityLocker} backed by PostgreSQL session-scoped advisory locks (see ADR-0012 and risk #7
 * in the implementation plan).
 *
 * <h2>Scope — session, not transaction</h2>
 *
 * The handler runs on a worker thread outside any transaction that the caller opened for the
 * publisher; a transaction-scoped {@code pg_advisory_xact_lock} would therefore need the adapter to
 * synthesize a dummy transaction. Session scope sidesteps that complexity: {@link
 * LockHandle#close()} explicitly calls {@code pg_advisory_unlock(key)} and returns the pool
 * connection.
 *
 * <h2>Same-connection invariant</h2>
 *
 * PostgreSQL advisory locks are tracked per-connection. To avoid a bug where HikariCP hands out
 * different physical connections to {@code tryLock} and {@code close}, this implementation keeps
 * the exact connection used for acquisition alive inside the returned {@link LockHandle} and closes
 * it only after the unlock succeeds.
 *
 * <h2>Key hashing</h2>
 *
 * {@code pg_advisory_lock} takes a signed {@code bigint}. The adapter derives a stable 64-bit hash
 * from the caller-supplied key via SHA-256 and reuses its first 8 bytes. This is deterministic: the
 * same key always maps to the same lock id. Collisions across completely unrelated keys are
 * astronomically improbable at the scale we operate.
 *
 * <h2>TTL</h2>
 *
 * The {@code ttl} parameter is documented but unused — PostgreSQL advisory locks have no built-in
 * timeout. The engine's {@code handlerMaxRuntime} + watchdog force-reclaim lets the <em>event</em>
 * move on, but the lock itself (and the pooled connection pinned inside its {@link LockHandle})
 * stays held until {@code close()} runs or the connection dies — the pool never recycles a
 * checked-out connection. After a hard crash (power loss, network partition) the backend holds the
 * lock until TCP keepalive reaps it. See ADR-0022 §Guarantee table.
 */
public final class PgAdvisoryLocker implements EntityLocker {

  private static final Logger log = LoggerFactory.getLogger(PgAdvisoryLocker.class);

  private final DataSource dataSource;

  public PgAdvisoryLocker(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
  }

  @Override
  public Optional<LockHandle> tryLock(String key, Duration ttl) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(ttl, "ttl must not be null");
    long hash = hash(key);

    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      boolean acquired;
      try (PreparedStatement ps = conn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
        ps.setLong(1, hash);
        try (ResultSet rs = ps.executeQuery()) {
          rs.next();
          acquired = rs.getBoolean(1);
        }
      }
      if (!acquired) {
        conn.close();
        return Optional.empty();
      }
      return Optional.of(new PgLockHandle(conn, key, hash));
    } catch (SQLException ex) {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException _) {
          // best-effort
        }
      }
      throw new LockAcquisitionException("pg_try_advisory_lock failed for key '" + key + "'", ex);
    }
  }

  /** Deterministic 64-bit hash of {@code key}. Public for cross-checking against SQL queries. */
  public static long hash(String key) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
      return ByteBuffer.wrap(digest, 0, 8).getLong();
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is guaranteed present by the JDK spec.
      throw new IllegalStateException("SHA-256 must be available", e);
    }
  }

  private static final class PgLockHandle implements LockHandle {

    private final Connection connection;
    private final String key;
    private final long hash;
    private volatile boolean closed;

    PgLockHandle(Connection connection, String key, long hash) {
      this.connection = connection;
      this.key = key;
      this.hash = hash;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      try (PreparedStatement ps = connection.prepareStatement("SELECT pg_advisory_unlock(?)")) {
        ps.setLong(1, hash);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next() && !rs.getBoolean(1)) {
            log.warn(
                "pg_advisory_unlock returned false for key '{}' (hash={}) — "
                    + "lock was not owned by this session",
                key,
                hash);
          }
        }
      } catch (SQLException ex) {
        throw new LockReleaseException("pg_advisory_unlock failed for key '" + key + "'", ex);
      } finally {
        try {
          connection.close();
        } catch (SQLException ex) {
          log.warn("failed to release advisory-lock connection: {}", ex.toString());
        }
      }
    }
  }
}
