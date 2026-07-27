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
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.spring.OutboxDataSource;
import io.github.bams22.outboxer.spring.OutboxDataSourceResolver;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.github.bams22.outboxer.storage.postgres.PostgresEventStore;
import io.github.bams22.outboxer.storage.postgres.PostgresOutboxAdmin;
import io.github.bams22.outboxer.storage.postgres.PostgresStorageProperties;
import io.github.bams22.outboxer.storage.postgres.PostgresWorkerRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.flyway.FlywayConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

/**
 * Autoconfiguration for the PostgreSQL storage adapter. Kicks in when {@code
 * outbox.storage.type=postgres}, the adapter classes are on the classpath, and a {@link DataSource}
 * bean is available.
 *
 * <h2>ADR-0002 — participate in the caller's transaction</h2>
 *
 * The starter wraps the application's {@code DataSource} in {@link TransactionAwareDataSourceProxy}
 * and resolves JDBC connections through {@link DataSourceUtils#getConnection(DataSource)}. With
 * this wiring, {@code OutboxEventPublisher.publish(...)} called inside a {@code @Transactional}
 * method shares the caller's connection and commits (or rolls back) atomically with the business
 * INSERT/UPDATE.
 *
 * <h2>ADR-0024 — DataSource selection</h2>
 *
 * With several {@code DataSource} beans, the one marked {@link OutboxDataSource @OutboxDataSource}
 * wins; otherwise the unique/{@code @Primary} bean is used; otherwise startup fails fast naming the
 * candidates (see {@link OutboxDataSourceResolver}).
 */
@AutoConfiguration(
    after = org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class)
@ConditionalOnClass({PostgresEventStore.class, TransactionAwareDataSourceProxy.class})
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(prefix = "event-outboxer.storage", name = "type", havingValue = "postgres")
public class PostgresStorageAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(OutboxAdmin.class)
  public PostgresOutboxAdmin outboxAdmin(
      ConnectionSupplier connections, PostgresStorageProperties properties) {
    return new PostgresOutboxAdmin(connections, properties);
  }

  @Bean
  @ConditionalOnMissingBean(ConnectionSupplier.class)
  public ConnectionSupplier outboxConnectionSupplier(
      @OutboxDataSource ObjectProvider<DataSource> qualifiedDataSources,
      ObjectProvider<DataSource> dataSources,
      ListableBeanFactory beanFactory) {
    DataSource dataSource =
        OutboxDataSourceResolver.resolve(qualifiedDataSources, dataSources, beanFactory);
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
      ConnectionSupplier connections,
      PostgresStorageProperties properties,
      Clock clock,
      io.github.bams22.outboxer.spi.MetricsSnapshotCache metricsCache) {
    return new PostgresEventStore(connections, properties, clock, metricsCache);
  }

  @Bean
  @ConditionalOnMissingBean(WorkerRegistry.class)
  public PostgresWorkerRegistry outboxWorkerRegistry(
      ConnectionSupplier connections, PostgresStorageProperties properties) {
    return new PostgresWorkerRegistry(connections, properties);
  }

  /**
   * Feeds {@code outbox.storage.schema} into Flyway as the {@code ${eventOutboxerSchema}}
   * placeholder so the library's classpath migrations pick up the same schema name as the adapter
   * at runtime.
   *
   * <p>Merges with any other placeholders the user has configured via {@code
   * spring.flyway.placeholders.*} — existing keys win on conflict, so users can always override the
   * schema name explicitly.
   */
  @Bean
  @ConditionalOnClass(name = "org.flywaydb.core.Flyway")
  public FlywayConfigurationCustomizer outboxFlywayPlaceholderCustomizer(
      OutboxProperties properties) {
    String schema = properties.getStorage().getSchema();
    return configuration -> {
      Map<String, String> merged = new HashMap<>();
      merged.put("eventOutboxerSchema", schema);
      // Existing placeholders win — do not clobber user overrides.
      Map<String, String> existing = configuration.getPlaceholders();
      if (existing != null) {
        merged.putAll(existing);
      }
      configuration.placeholders(merged);
    };
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  /**
   * {@link ConnectionSupplier} that leases connections via {@link
   * DataSourceUtils#getConnection(DataSource)}; inside a Spring-managed transaction the pool
   * connection is shared with the caller, outside it a fresh pool connection is obtained and closed
   * on {@code release(...)}.
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
