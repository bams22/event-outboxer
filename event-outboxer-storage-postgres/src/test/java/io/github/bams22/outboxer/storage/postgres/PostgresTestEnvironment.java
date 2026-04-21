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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Shared Testcontainers + HikariCP + Flyway bootstrap for PostgreSQL integration tests. One
 * container per JVM — the singleton pattern amortises the ~2 s container startup across every
 * IT class in this module (JUnit 5's static {@code @Container} gives per-class lifecycle; a
 * shared singleton is noticeably faster).
 */
public final class PostgresTestEnvironment {

  private static final PostgreSQLContainer<?> CONTAINER;
  private static final HikariDataSource DATA_SOURCE;

  static {
    CONTAINER =
        new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("outboxer")
            .withUsername("outboxer")
            .withPassword("outboxer");
    CONTAINER.start();
    Runtime.getRuntime()
        .addShutdownHook(new Thread(CONTAINER::stop, "pg-testcontainer-shutdown"));

    HikariConfig cfg = new HikariConfig();
    cfg.setJdbcUrl(CONTAINER.getJdbcUrl());
    cfg.setUsername(CONTAINER.getUsername());
    cfg.setPassword(CONTAINER.getPassword());
    cfg.setMaximumPoolSize(16);
    DATA_SOURCE = new HikariDataSource(cfg);

    Flyway.configure()
        .dataSource(DATA_SOURCE)
        .locations(
            "classpath:db/migration/outbox/core", "classpath:db/migration/outbox/archive")
        .load()
        .migrate();
  }

  private PostgresTestEnvironment() {}

  public static DataSource dataSource() {
    return DATA_SOURCE;
  }

  /**
   * Simple {@link ConnectionSupplier} that leases a pool connection per call and closes it on
   * release. The tests never span multiple statements under a single tx, so this is enough.
   */
  public static ConnectionSupplier connectionSupplier() {
    return new ConnectionSupplier() {
      @Override
      public Connection get() throws SQLException {
        return DATA_SOURCE.getConnection();
      }

      @Override
      public void release(Connection connection) throws SQLException {
        if (connection != null && !connection.isClosed()) {
          connection.close();
        }
      }
    };
  }

  /** Wipe outbox tables between tests for isolation. */
  public static void truncate() {
    try (Connection conn = DATA_SOURCE.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("TRUNCATE TABLE outbox.events, outbox.workers RESTART IDENTITY");
      st.execute("TRUNCATE TABLE outbox.event_archive RESTART IDENTITY");
    } catch (SQLException e) {
      throw new IllegalStateException("failed to truncate outbox tables", e);
    }
  }
}
