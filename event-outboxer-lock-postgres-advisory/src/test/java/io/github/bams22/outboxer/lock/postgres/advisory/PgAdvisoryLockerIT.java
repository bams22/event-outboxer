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

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import io.github.bams22.outboxer.spi.contracts.AbstractEntityLockerContractTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

class PgAdvisoryLockerIT extends AbstractEntityLockerContractTest {

    private static PostgreSQLContainer<?> container;
    private static HikariDataSource dataSource;

    @BeforeAll
    static void boot() {
        container =
                new PostgreSQLContainer<>("postgres:15")
                        .withDatabaseName("lockerit")
                        .withUsername("lockerit")
                        .withPassword("lockerit");
        container.start();

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(container.getJdbcUrl());
        cfg.setUsername(container.getUsername());
        cfg.setPassword(container.getPassword());
        // Pool >= threads used by the contract's concurrent exclusivity test.
        cfg.setMaximumPoolSize(64);
        dataSource = new HikariDataSource(cfg);
    }

    @AfterAll
    static void shutdown() {
        if (dataSource != null) {
            dataSource.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    @Override
    protected EntityLocker newLocker() {
        return new PgAdvisoryLocker(dataSource());
    }

    /** The native wait blocks in pg_advisory_lock; a platform thread's interrupt cannot cut it. */
    @Override
    protected boolean interruptEndsWaitEarly() {
        return false;
    }

    @Test
    @DisplayName("a timed-out native wait leaves no advisory lock behind on the pooled session")
    void nativeWait_timeout_leaksNoLock() throws SQLException {
        long hash = PgAdvisoryLocker.hash("w-leak");
        try (LockHandle _ = locker.tryLock("w-leak", Duration.ofSeconds(30)).orElseThrow()) {
            Optional<LockHandle> waiter =
                    locker.tryLock("w-leak", Duration.ofSeconds(30), Duration.ofMillis(300));
            assertThat(waiter).isEmpty();
            // Exactly the holder's lock: the timed-out waiter's session holds nothing.
            assertThat(advisoryLocksHeld(hash)).isEqualTo(1);
        }
        assertThat(advisoryLocksHeld(hash)).isZero();
        // ... and the pool's connections are usable for a fresh acquisition.
        Optional<LockHandle> fresh =
                locker.tryLock("w-leak", Duration.ofSeconds(30), Duration.ZERO);
        assertThat(fresh).isPresent();
        fresh.orElseThrow().close();
    }

    @Test
    @DisplayName("the statement timeout is transaction-local: the pinned session keeps no timeout")
    void nativeWait_statementTimeoutDoesNotStick() throws SQLException {
        LockHandle held =
                locker.tryLock("w-sticky", Duration.ofSeconds(30), Duration.ofMillis(200))
                        .orElseThrow();
        // Same physical connection (pinned in the handle) via pg_locks → pid; check its setting
        // through a pool connection instead, and through the handle's own session after release.
        held.close();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SHOW statement_timeout");
                ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("0");
        }
    }

    /**
     * Granted advisory locks for a 64-bit key: pg_locks splits it into {@code classid} (high 32
     * bits) and {@code objid} (low 32 bits) with {@code objsubid = 1}.
     */
    private static int advisoryLocksHeld(long hash) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND"
                                    + " granted AND classid::bigint = ? AND objid::bigint = ? AND"
                                    + " objsubid = 1")) {
            ps.setLong(1, hash >>> 32);
            ps.setLong(2, hash & 0xFFFFFFFFL);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private static DataSource dataSource() {
        return dataSource;
    }
}
