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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Contract + lease-specific integration tests against a real PostgreSQL 15, using the actual V005
 * migration file (placeholder-substituted) so the DDL the users run is the DDL under test.
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

  @Override
  protected EntityLocker newLocker() {
    return new PgLeaseEntityLocker(dataSource);
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
                    + ".entity_locks SET acquired_at = now() - interval '1 hour', "
                    + "expires_at = now() - interval '1 second' WHERE lock_key = ?")) {
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
                  locker
                      .tryLock(key, Duration.ofSeconds(30))
                      .ifPresent(h -> acquired.incrementAndGet());
                }));
      }
      go.countDown();
      for (var f : futures) {
        f.get(10, TimeUnit.SECONDS);
      }
      assertThat(acquired).as("exactly one contender takes over an expired lease").hasValue(1);
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
  @DisplayName("a taken-over lease belongs to the successor: old close() leaves it busy for others")
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
    String resource = "/db/migration/outbox/lock/V005__outbox_entity_locks.sql";
    try (InputStream in = PgLeaseEntityLockerIT.class.getResourceAsStream(resource)) {
      if (in == null) {
        throw new IllegalStateException("migration resource not found: " + resource);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8)
          .replace("${eventOutboxerSchema}", SCHEMA);
    }
  }
}
