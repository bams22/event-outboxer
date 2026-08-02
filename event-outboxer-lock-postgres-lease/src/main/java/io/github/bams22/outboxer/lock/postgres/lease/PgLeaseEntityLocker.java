/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.postgres.lease;

import io.github.bams22.outboxer.domain.exception.LockAcquisitionException;
import io.github.bams22.outboxer.domain.exception.LockReleaseException;
import io.github.bams22.outboxer.spi.EntityLocker;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link EntityLocker} backed by a lease row in the {@code entity_locks} table (ADR-0022).
 * Recommended PostgreSQL locker ({@code event-outboxer.lock.type=postgres-lease}).
 *
 * <h2>Why a table, not advisory locks</h2>
 *
 * Lock state is a committed row, not session state. Both operations — acquire and release — are
 * single autocommit statements on a briefly borrowed pooled connection, so (unlike the
 * session-scoped {@code PgAdvisoryLocker}) no connection is pinned while the handler runs, the
 * fleet cannot deadlock against its own handlers, and every statement is safe behind pgBouncer
 * transaction (or statement) pooling.
 *
 * <h2>Acquire</h2>
 *
 * One atomic {@code INSERT ... ON CONFLICT (lock_key) DO UPDATE ... WHERE expired ... RETURNING}: a
 * fresh key inserts, an expired lease is taken over, a live lease returns zero rows (= busy, never
 * an exception). Concurrent contenders resolve to exactly one winner via PostgreSQL's speculative
 * insertion and the EvalPlanQual re-check. All expiry arithmetic uses the database clock ({@code
 * now()}), mirroring the workers-heartbeat design — application-JVM clock skew can neither extend
 * nor shorten exclusion.
 *
 * <h2>Release</h2>
 *
 * A token-guarded {@code DELETE}: the per-acquisition random UUID token in the predicate makes the
 * release an atomic compare-and-delete (same semantics as the Redis adapter's Lua script), so a
 * zombie's late {@code close()} can never release its successor's lease. The deleting connection
 * may be a different physical connection — or a different pgBouncer backend — than the acquiring
 * one; it does not matter.
 *
 * <h2>Connection discipline (load-bearing, see ADR-0022 §JDBC contract)</h2>
 *
 * <ul>
 *   <li><b>Autocommit is forced.</b> {@code ON CONFLICT DO UPDATE} locks the conflicting tuple
 *       BEFORE evaluating the {@code WHERE} and holds that lock until the transaction ends, even
 *       when zero rows are returned. Inside a longer transaction a mere busy probe would block
 *       other contenders and the holder's release.
 *   <li><b>READ COMMITTED is forced</b> and {@code SQLState 40001} maps to busy: under a {@code
 *       repeatable read} default a contended upsert can raise a serialization failure even as a
 *       single statement; it must degrade to the clean busy path.
 *   <li><b>{@code setQueryTimeout} is normative.</b> It bounds the contender's tuple-lock wait and
 *       thereby the lease shortening ({@code expires_at} is computed from transaction-start {@code
 *       now()}, so a contender that waited {@code W} gets an effective lease of {@code ttl - W}).
 *       Never replace it with {@code SET statement_timeout} — session-sticky under transaction
 *       pooling.
 * </ul>
 *
 * <h2>TTL</h2>
 *
 * Honoured: exclusion holds until {@code min(close(), ttl)} and the lease self-releases at {@code
 * expires_at} via the next acquirer's takeover — the crash-release mechanism. The engine enforces
 * {@code lockTtl >= handlerMaxRuntime} (default 2x); the margin also absorbs JVM-clock (watchdog)
 * vs DB-clock (lease) divergence.
 */
public final class PgLeaseEntityLocker implements EntityLocker {

    private static final Logger log = LoggerFactory.getLogger(PgLeaseEntityLocker.class);

    /** Default schema name, aligned with the storage adapter and the classpath migrations. */
    public static final String DEFAULT_SCHEMA = "event_outboxer";

    /** Maximum accepted lock-key length; matches the {@code VARCHAR(512)} column (V005). */
    public static final int MAX_KEY_LENGTH = 512;

    /**
     * Bounds the tuple-lock wait under contention and, transitively, the lease shortening {@code
     * ttl - wait}. Must stay well below {@code lockTtl - handlerMaxRuntime}.
     */
    private static final int QUERY_TIMEOUT_SECONDS = 5;

    /**
     * SQLState raised by PostgreSQL on a serialization failure ("could not serialize access due to
     * concurrent update") — reachable on the acquire path under non-READ-COMMITTED defaults.
     */
    private static final String SERIALIZATION_FAILURE = "40001";

    private final DataSource dataSource;
    private final String ownerWorker;
    private final String acquireSql;
    private final String releaseSql;
    private final String sweepSql;
    private final String countSql;

    public PgLeaseEntityLocker(DataSource dataSource) {
        this(dataSource, DEFAULT_SCHEMA, null);
    }

    public PgLeaseEntityLocker(DataSource dataSource, String schema) {
        this(dataSource, schema, null);
    }

    /**
     * Full constructor.
     *
     * @param dataSource the application pool. Must NOT be a transaction-aware proxy: lock
     *     statements must run in their own autocommit transactions, never joined to the caller's
     *     transaction (see the connection-discipline notes above)
     * @param schema schema holding {@code entity_locks}; concatenated into SQL verbatim, same
     *     convention as the storage adapter
     * @param ownerWorker informational worker identity written to {@code entity_locks.owner_worker}
     *     for operator forensics; {@code null} derives {@code {hostname}-{pid}}. Never used in
     *     predicates
     */
    public PgLeaseEntityLocker(DataSource dataSource, String schema, @Nullable String ownerWorker) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        this.ownerWorker = ownerWorker != null ? ownerWorker : defaultOwnerWorker();
        String table = schema + ".entity_locks";
        this.acquireSql =
                "INSERT INTO "
                        + table
                        + " AS l (lock_key, owner_token, owner_worker, acquired_at, expires_at)"
                        + " VALUES (?, ?, ?, now(), now() + make_interval(secs => ?))"
                        + " ON CONFLICT (lock_key) DO UPDATE"
                        + " SET owner_token = EXCLUDED.owner_token,"
                        + "     owner_worker = EXCLUDED.owner_worker,"
                        + "     acquired_at = EXCLUDED.acquired_at,"
                        + "     expires_at = EXCLUDED.expires_at"
                        + " WHERE l.expires_at <= now()"
                        + " RETURNING lock_key";
        this.releaseSql = "DELETE FROM " + table + " WHERE lock_key = ? AND owner_token = ?";
        this.sweepSql = "DELETE FROM " + table + " WHERE expires_at <= now()";
        this.countSql = "SELECT count(*) FROM " + table + " WHERE expires_at > now()";
    }

    @Override
    public Optional<LockHandle> tryLock(String key, Duration ttl) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        if (ttl.compareTo(Duration.ofMillis(1)) < 0) {
            // Sub-millisecond TTLs bind make_interval(secs => 0.0), producing
            // expires_at == acquired_at and a confusing CHECK violation (23514).
            throw new IllegalArgumentException("ttl must be >= 1ms, got " + ttl);
        }
        if (key.length() > MAX_KEY_LENGTH) {
            throw new LockAcquisitionException(
                    "lock key exceeds "
                            + MAX_KEY_LENGTH
                            + " characters ("
                            + key.length()
                            + "): entity_locks.lock_key is VARCHAR("
                            + MAX_KEY_LENGTH
                            + ") — shorten the key returned by extractLockKey()",
                    null);
        }
        String token = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            prepareConnection(conn);
            try (PreparedStatement ps = conn.prepareStatement(acquireSql)) {
                ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                ps.setString(1, key);
                ps.setString(2, token);
                ps.setString(3, ownerWorker);
                ps.setDouble(4, ttl.toMillis() / 1000.0);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                }
            }
            return Optional.of(new PgLeaseLockHandle(key, token));
        } catch (SQLException ex) {
            if (SERIALIZATION_FAILURE.equals(ex.getSQLState())) {
                log.debug(
                        "lease acquire for key '{}' hit a serialization failure (40001) — treating"
                                + " as busy",
                        key);
                return Optional.empty();
            }
            throw new LockAcquisitionException("lease acquire failed for key '" + key + "'", ex);
        }
    }

    /**
     * Deletes leases whose {@code expires_at} has passed. Cosmetic garbage collection only:
     * correctness never depends on it — expired rows are overwritten in place by the next acquirer;
     * the sweep merely collects rows of keys that are never contended again. Safe at any cadence.
     *
     * @return the number of rows removed
     * @throws LockReleaseException if the backend is unreachable or refuses the delete
     */
    public int sweepExpired() {
        try (Connection conn = dataSource.getConnection()) {
            prepareConnection(conn);
            try (PreparedStatement ps = conn.prepareStatement(sweepSql)) {
                ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                return ps.executeUpdate();
            }
        } catch (SQLException ex) {
            throw new LockReleaseException("entity_locks sweep failed", ex);
        }
    }

    /**
     * Number of currently live leases ({@code expires_at > now()}). Observability only — feeds the
     * starter's Micrometer gauge.
     *
     * @throws LockAcquisitionException if the backend is unreachable ({@code
     *     LockAcquisitionException} is this SPI's generic "locker backend error" signal, see {@link
     *     EntityLocker#tryLock})
     */
    public long countLiveLeases() {
        try (Connection conn = dataSource.getConnection()) {
            prepareConnection(conn);
            try (PreparedStatement ps = conn.prepareStatement(countSql)) {
                ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        } catch (SQLException ex) {
            throw new LockAcquisitionException("live-lease count failed", ex);
        }
    }

    /**
     * Forces the connection discipline the protocol depends on: autocommit (a busy probe's tuple
     * lock is held until the transaction ends) and READ COMMITTED (avoids 40001 noise under
     * stricter pool-level defaults). HikariCP tracks and resets both on pool return.
     */
    private static void prepareConnection(Connection conn) throws SQLException {
        if (!conn.getAutoCommit()) {
            conn.setAutoCommit(true);
        }
        if (conn.getTransactionIsolation() != Connection.TRANSACTION_READ_COMMITTED) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }

    private static String defaultOwnerWorker() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown";
        }
        String worker = host + "-" + ProcessHandle.current().pid();
        return worker.length() <= 64 ? worker : worker.substring(0, 64);
    }

    private final class PgLeaseLockHandle implements LockHandle {

        private final String key;
        private final String token;
        private volatile boolean closed;

        PgLeaseLockHandle(String key, String token) {
            this.key = key;
            this.token = token;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try (Connection conn = dataSource.getConnection()) {
                prepareConnection(conn);
                try (PreparedStatement ps = conn.prepareStatement(releaseSql)) {
                    ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                    ps.setString(1, key);
                    ps.setString(2, token);
                    if (ps.executeUpdate() == 0) {
                        // Lease expired and was taken over (token mismatch — we must not touch the
                        // successor's row) or was already swept.
                        log.debug(
                                "lease '{}' was already released (expired or taken over) by the"
                                        + " time close() ran",
                                key);
                    }
                }
            } catch (SQLException ex) {
                throw new LockReleaseException("lease release failed for key '" + key + "'", ex);
            }
        }
    }
}
