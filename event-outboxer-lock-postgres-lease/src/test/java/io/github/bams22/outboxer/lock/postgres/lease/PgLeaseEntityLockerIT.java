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

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import io.github.bams22.outboxer.spi.contracts.AbstractEntityLockerContractTest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Contract + lease-specific integration tests against a real PostgreSQL 15, using the actual V005
 * migration file (placeholder-substituted) so the DDL the users run is the DDL under test. The
 * locker under test runs its LISTEN/NOTIFY release listener (ADR-0035), so the contract's
 * bounded-wait cases exercise the notification path once the listener has verified itself.
 */
class PgLeaseEntityLockerIT extends AbstractEntityLockerContractTest {

    private static final String SCHEMA = "event_outboxer";

    private static PostgreSQLContainer<?> container;
    private static HikariDataSource dataSource;

    @BeforeAll
    static void boot() throws Exception {
        container =
                new PostgreSQLContainer<>("postgres:15")
                        .withDatabaseName("leaseit")
                        .withUsername("leaseit")
                        .withPassword("leaseit");
        container.start();

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(container.getJdbcUrl());
        cfg.setUsername(container.getUsername());
        cfg.setPassword(container.getPassword());
        // Pool >= threads used by the contract's concurrent exclusivity test.
        cfg.setMaximumPoolSize(64);
        dataSource = new HikariDataSource(cfg);

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute(migrationSql());
        }
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

    @BeforeEach
    void cleanTable() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("DELETE FROM " + SCHEMA + ".entity_locks");
        }
    }

    @AfterEach
    void stopListener() {
        if (locker instanceof PgLeaseEntityLocker lease) {
            assertThat(lease.waitingKeys()).as("no waiter left registered").isZero();
            lease.close();
        }
    }

    @Override
    protected EntityLocker newLocker() {
        return PgLeaseEntityLocker.builder()
                .dataSource(dataSource)
                .releaseNotifications(true)
                .build();
    }

    private static void awaitActive(PgLeaseEntityLocker lease) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!lease.wakeupActive()) {
            assertThat(System.nanoTime() < deadline)
                    .as("listener must verify itself, state=%s", lease.listener().state())
                    .isTrue();
            Thread.sleep(20);
        }
    }

    private static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ==================== LISTEN/NOTIFY wake-up (ADR-0035) ====================

    @Test
    @DisplayName("the listener verifies itself with a probe and reports active")
    void wakeup_listenerVerifies() throws Exception {
        PgLeaseEntityLocker lease = (PgLeaseEntityLocker) locker;
        assertThat(lease.wakeupEnabled()).isTrue();
        assertThat(lease.channel()).isEqualTo("event_outboxer.entity_locks");
        awaitActive(lease);
        assertThat(lease.listener().state()).isEqualTo(PgLeaseReleaseListener.State.ACTIVE);
        assertThat(lease.listener().backendPid()).isPositive();
    }

    @Test
    @DisplayName("a waiter wakes on the release notification, not on the fallback probe")
    void wakeup_beatsTheFallbackProbe() throws Exception {
        // A fallback of 5 s: if the waiter acquires well before that, the notification did it.
        try (PgLeaseEntityLocker slowFallback =
                PgLeaseEntityLocker.builder()
                        .dataSource(dataSource)
                        .releaseNotifications(true)
                        .fallbackProbeInterval(Duration.ofSeconds(5))
                        .build()) {
            awaitActive(slowFallback);
            LockHandle holder = slowFallback.tryLock("wake", Duration.ofSeconds(30)).orElseThrow();
            Duration holdFor = Duration.ofMillis(100);
            Thread releaser =
                    new Thread(
                            () -> {
                                sleepQuietly(holdFor);
                                holder.close();
                            },
                            "releaser");
            long start = System.nanoTime();
            releaser.start();

            Optional<LockHandle> waiter =
                    slowFallback.tryLock("wake", Duration.ofSeconds(30), Duration.ofSeconds(10));
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
            releaser.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(waiter).isPresent();
            assertThat(elapsed).isGreaterThanOrEqualTo(holdFor);
            assertThat(elapsed)
                    .as("woken by NOTIFY, not by the 5 s fallback")
                    .isLessThan(Duration.ofSeconds(2));
            waiter.orElseThrow().close();
            assertThat(slowFallback.waitingKeys()).isZero();
        }
    }

    @Test
    @DisplayName("a crowd of waiters on one key all get their turn, one at a time")
    void wakeup_manyWaitersTakeTurns() throws Exception {
        PgLeaseEntityLocker lease = (PgLeaseEntityLocker) locker;
        awaitActive(lease);
        int waiters = 16;
        String key = "crowd";
        LockHandle holder = lease.tryLock(key, Duration.ofSeconds(30)).orElseThrow();
        CountDownLatch ready = new CountDownLatch(waiters);
        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger overlaps = new AtomicInteger();
        AtomicInteger inside = new AtomicInteger();
        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            Thread t =
                    new Thread(
                            () -> {
                                ready.countDown();
                                Optional<LockHandle> h =
                                        lease.tryLock(
                                                key,
                                                Duration.ofSeconds(30),
                                                Duration.ofSeconds(20));
                                if (h.isEmpty()) {
                                    return;
                                }
                                if (inside.incrementAndGet() > 1) {
                                    overlaps.incrementAndGet();
                                }
                                sleepQuietly(Duration.ofMillis(5));
                                inside.decrementAndGet();
                                acquired.incrementAndGet();
                                h.get().close();
                            },
                            "waiter-" + i);
            threads.add(t);
            t.start();
        }
        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(150);
        assertThat(lease.waitingKeys()).isEqualTo(1);

        holder.close();
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(t.isAlive()).isFalse();
        }

        assertThat(acquired).hasValue(waiters);
        assertThat(overlaps).hasValue(0);
    }

    @Test
    @DisplayName("the listener survives its session being terminated and keeps waking waiters")
    void wakeup_listenerReconnectsAfterTerminate() throws Exception {
        try (PgLeaseEntityLocker slowFallback =
                PgLeaseEntityLocker.builder()
                        .dataSource(dataSource)
                        .releaseNotifications(true)
                        .fallbackProbeInterval(Duration.ofSeconds(5))
                        .build()) {
            awaitActive(slowFallback);
            int pid = slowFallback.listener().backendPid();
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement ps =
                            conn.prepareStatement("SELECT pg_terminate_backend(?)")) {
                ps.setInt(1, pid);
                ps.execute();
            }
            // Reconnects with back-off (200 ms, 400 ms, ...) and re-verifies.
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (slowFallback.listener().backendPid() == pid || !slowFallback.wakeupActive()) {
                assertThat(System.nanoTime() < deadline).as("listener must reconnect").isTrue();
                Thread.sleep(50);
            }

            LockHandle holder = slowFallback.tryLock("after", Duration.ofSeconds(30)).orElseThrow();
            Thread releaser =
                    new Thread(
                            () -> {
                                sleepQuietly(Duration.ofMillis(100));
                                holder.close();
                            });
            long start = System.nanoTime();
            releaser.start();
            Optional<LockHandle> waiter =
                    slowFallback.tryLock("after", Duration.ofSeconds(30), Duration.ofSeconds(10));
            releaser.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(waiter).isPresent();
            assertThat(Duration.ofNanos(System.nanoTime() - start))
                    .isLessThan(Duration.ofSeconds(2));
            waiter.orElseThrow().close();
        }
    }

    @Test
    @DisplayName("without release notifications the bounded wait still works by polling")
    void polling_withoutNotifications() throws Exception {
        try (PgLeaseEntityLocker polling = new PgLeaseEntityLocker(dataSource)) {
            assertThat(polling.wakeupEnabled()).isFalse();
            assertThat(polling.wakeupActive()).isFalse();
            LockHandle holder = polling.tryLock("poll", Duration.ofSeconds(30)).orElseThrow();
            Thread releaser =
                    new Thread(
                            () -> {
                                sleepQuietly(Duration.ofMillis(60));
                                holder.close();
                            });
            releaser.start();

            Optional<LockHandle> waiter =
                    polling.tryLock("poll", Duration.ofSeconds(30), Duration.ofSeconds(5));
            releaser.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(waiter).isPresent();
            waiter.orElseThrow().close();
        }
    }

    @Test
    @DisplayName("the release statement notifies the released key on the schema's channel")
    void release_notifiesOnTheChannel() throws Exception {
        try (Connection probe = dataSource.getConnection();
                Statement st = probe.createStatement()) {
            probe.setAutoCommit(true);
            st.execute("LISTEN \"event_outboxer.entity_locks\"");
            PGConnection pg = probe.unwrap(PGConnection.class);
            pg.getNotifications(); // drain

            locker.tryLock("notified", Duration.ofSeconds(30)).orElseThrow().close();

            PGNotification[] batch = pg.getNotifications(5_000);
            assertThat(batch).isNotNull();
            assertThat(batch).extracting(PGNotification::getParameter).contains("notified");
            assertThat(batch[0].getName()).isEqualTo("event_outboxer.entity_locks");
        }
    }

    @Override
    protected boolean supportsTtlExpiry() {
        return true;
    }

    @Override
    protected void forceExpire(String key) {
        // Backdate BOTH timestamps: the entity_locks_expiry_after_acquire CHECK
        // requires expires_at > acquired_at.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "UPDATE "
                                        + SCHEMA
                                        + ".entity_locks SET acquired_at = now() - interval '1"
                                        + " hour', expires_at = now() - interval '1 second' WHERE"
                                        + " lock_key = ?")) {
            ps.setString(1, key);
            int updated = ps.executeUpdate();
            assertThat(updated).as("forceExpire must find the lease row").isEqualTo(1);
        } catch (SQLException ex) {
            throw new IllegalStateException("forceExpire failed for key '" + key + "'", ex);
        }
    }

    @Test
    @DisplayName("a stampede on an EXPIRED key resolves to exactly one winner")
    void expiredKeyStampede() throws Exception {
        String key = "stampede-expired";
        LockHandle dead = locker.tryLock(key, Duration.ofSeconds(30)).orElseThrow();
        forceExpire(key);

        int threads = 32;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger acquired = new AtomicInteger();
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < threads; i++) {
                futures.add(
                        exec.submit(
                                () -> {
                                    try {
                                        go.await();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        return;
                                    }
                                    locker.tryLock(key, Duration.ofSeconds(30))
                                            .ifPresent(h -> acquired.incrementAndGet());
                                }));
            }
            go.countDown();
            for (var f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            assertThat(acquired)
                    .as("exactly one contender takes over an expired lease")
                    .hasValue(1);
        } finally {
            exec.shutdownNow();
        }
        dead.close();
    }

    @Test
    @DisplayName("sweepExpired() removes only expired leases and never a live one")
    void sweepRemovesOnlyExpired() {
        PgLeaseEntityLocker lease = (PgLeaseEntityLocker) locker;
        LockHandle live = lease.tryLock("sweep-live", Duration.ofSeconds(30)).orElseThrow();
        LockHandle dead = lease.tryLock("sweep-dead", Duration.ofSeconds(30)).orElseThrow();
        forceExpire("sweep-dead");

        assertThat(lease.sweepExpired()).isEqualTo(1);

        // The live lease survived the sweep — its key is still busy.
        assertThat(lease.tryLock("sweep-live", Duration.ofSeconds(30))).isEmpty();
        // The swept key is immediately acquirable again.
        Optional<LockHandle> reacquired = lease.tryLock("sweep-dead", Duration.ofSeconds(30));
        assertThat(reacquired).isPresent();
        reacquired.orElseThrow().close();
        live.close();
        dead.close();
    }

    @Test
    @DisplayName("countLiveLeases() counts only unexpired rows")
    void countLiveLeases() {
        PgLeaseEntityLocker lease = (PgLeaseEntityLocker) locker;
        LockHandle a = lease.tryLock("count-a", Duration.ofSeconds(30)).orElseThrow();
        LockHandle b = lease.tryLock("count-b", Duration.ofSeconds(30)).orElseThrow();
        assertThat(lease.countLiveLeases()).isEqualTo(2);

        forceExpire("count-b");
        assertThat(lease.countLiveLeases()).isEqualTo(1);
        a.close();
        b.close();
    }

    @Test
    @DisplayName("owner_worker forensics column is populated on acquire")
    void ownerWorkerRecorded() throws SQLException {
        try (LockHandle _ = locker.tryLock("forensics", Duration.ofSeconds(30)).orElseThrow()) {
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement ps =
                            conn.prepareStatement(
                                    "SELECT owner_worker FROM "
                                            + SCHEMA
                                            + ".entity_locks WHERE lock_key = 'forensics'");
                    ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isNotBlank();
            }
        }
    }

    @Test
    @DisplayName(
            "a taken-over lease belongs to the successor: old close() leaves it busy for others")
    void takeoverKeepsExclusion() {
        LockHandle zombie = locker.tryLock("takeover", Duration.ofSeconds(30)).orElseThrow();
        forceExpire("takeover");
        LockHandle successor = locker.tryLock("takeover", Duration.ofSeconds(30)).orElseThrow();

        zombie.close();
        assertThat(locker.tryLock("takeover", Duration.ofSeconds(30))).isEmpty();

        successor.close();
        Optional<LockHandle> next = locker.tryLock("takeover", Duration.ofSeconds(30));
        assertThat(next).isPresent();
        next.orElseThrow().close();
    }

    private static String migrationSql() throws IOException {
        String resource = "/event-outboxer/migration/lock/V005__outbox_entity_locks.sql";
        try (InputStream in = PgLeaseEntityLockerIT.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("migration resource not found: " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("${eventOutboxerSchema}", SCHEMA);
        }
    }
}
