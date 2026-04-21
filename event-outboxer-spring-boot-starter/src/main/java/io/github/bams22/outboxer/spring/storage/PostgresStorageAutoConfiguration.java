/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.storage;

import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.github.bams22.outboxer.storage.postgres.PostgresEventStore;
import io.github.bams22.outboxer.storage.postgres.PostgresStorageProperties;
import io.github.bams22.outboxer.storage.postgres.PostgresWorkerRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

/**
 * Autoconfiguration for the PostgreSQL storage adapter. Kicks in when {@code
 * outbox.storage.type=postgres}, the adapter classes are on the classpath, and a
 * {@link DataSource} bean is available.
 *
 * <h2>ADR-0002 — participate in the caller's transaction</h2>
 *
 * The starter wraps the application's {@code DataSource} in
 * {@link TransactionAwareDataSourceProxy} and resolves JDBC connections through
 * {@link DataSourceUtils#getConnection(DataSource)}. With this wiring, {@code
 * OutboxEventPublisher.publish(...)} called inside a {@code @Transactional} method shares the
 * caller's connection and commits (or rolls back) atomically with the business INSERT/UPDATE.
 */
@AutoConfiguration(after = org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class)
@ConditionalOnClass({PostgresEventStore.class, TransactionAwareDataSourceProxy.class})
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(prefix = "outbox.storage", name = "type", havingValue = "postgres")
public class PostgresStorageAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(ConnectionSupplier.class)
  public ConnectionSupplier outboxConnectionSupplier(DataSource dataSource) {
    DataSource txAware =
        dataSource instanceof TransactionAwareDataSourceProxy proxy
            ? proxy
            : new TransactionAwareDataSourceProxy(dataSource);
    return new DataSourceConnectionSupplier(txAware);
  }

  @Bean
  @ConditionalOnMissingBean
  public PostgresStorageProperties outboxPostgresStorageProperties(OutboxProperties props) {
    OutboxProperties.Storage s = props.getStorage();
    return PostgresStorageProperties.builder()
        .schema(s.getSchema())
        .tablePrefix(s.getTablePrefix())
        .archiveEnabled(s.isArchiveEnabled())
        .metricsCacheTtl(s.getMetricsCacheTtl())
        .build();
  }

  @Bean
  @ConditionalOnMissingBean(EventStore.class)
  public PostgresEventStore outboxEventStore(
      ConnectionSupplier connections, PostgresStorageProperties properties, Clock clock) {
    return new PostgresEventStore(connections, properties, clock);
  }

  @Bean
  @ConditionalOnMissingBean(WorkerRegistry.class)
  public PostgresWorkerRegistry outboxWorkerRegistry(
      ConnectionSupplier connections, PostgresStorageProperties properties) {
    return new PostgresWorkerRegistry(connections, properties);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  /**
   * {@link ConnectionSupplier} that leases connections via
   * {@link DataSourceUtils#getConnection(DataSource)}; inside a Spring-managed transaction the
   * pool connection is shared with the caller, outside it a fresh pool connection is obtained
   * and closed on {@code release(...)}.
   */
  private static final class DataSourceConnectionSupplier implements ConnectionSupplier {

    private final DataSource dataSource;

    DataSourceConnectionSupplier(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    @Override
    public Connection get() throws SQLException {
      return DataSourceUtils.getConnection(dataSource);
    }

    @Override
    public void release(Connection connection) throws SQLException {
      DataSourceUtils.releaseConnection(connection, dataSource);
    }
  }
}
