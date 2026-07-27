/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.lock;

import io.github.bams22.outboxer.lock.postgres.lease.PgLeaseEntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Registers {@link PgLeaseEntityLocker} — the default PostgreSQL entity locker since ADR-0022 —
 * when {@code event-outboxer.lock.type=postgres-lease}, the adapter class is on the classpath
 * ({@code event-outboxer-lock-postgres-lease}), and a {@link DataSource} bean is available.
 *
 * <p>Also wires the operational surface around the lease table:
 *
 * <ul>
 *   <li>a fail-fast startup probe that converts a missing {@code entity_locks} table (migration
 *       V005 not applied) into an actionable error, ordered after Flyway/Liquibase via {@link
 *       DependsOnDatabaseInitialization};
 *   <li>a fixed-cadence sweep of expired lease rows (cosmetic garbage collection — correctness
 *       never depends on it, ADR-0022 §Starter integration);
 *   <li>an optional Micrometer gauge for currently held leases.
 * </ul>
 *
 * <p>The locker intentionally uses the raw {@code DataSource} (never the transaction-aware
 * proxy). For the lease protocol this is a correctness requirement, not a preference: acquire and
 * release must each run as their own autocommit statement — inside a caller's transaction a mere
 * busy probe would hold the row's tuple lock until that transaction ends, blocking other
 * contenders and the holder's release (ADR-0022 §JDBC contract).
 */
@AutoConfiguration(after = org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class)
@ConditionalOnClass(PgLeaseEntityLocker.class)
@ConditionalOnBean(DataSource.class)
@Conditional(OnPostgresLeaseLockCondition.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class PostgresLeaseLockAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(EntityLocker.class)
  public PgLeaseEntityLocker outboxEntityLocker(
      DataSource dataSource, OutboxProperties properties) {
    return new PgLeaseEntityLocker(
        dataSource,
        properties.getStorage().getSchema(),
        properties.getWorker().getId());
  }

  /**
   * Skipped silently when a user-defined {@code EntityLocker} displaced the lease locker — the
   * probe (and the sweep below) belong to the lease table, not to the lock SPI.
   */
  @Bean
  @DependsOnDatabaseInitialization
  public PgLeaseTableProbe pgLeaseTableProbe(
      ObjectProvider<PgLeaseEntityLocker> lockerProvider,
      DataSource dataSource,
      OutboxProperties properties) {
    return new PgLeaseTableProbe(
        lockerProvider.getIfAvailable() != null ? dataSource : null,
        properties.getStorage().getSchema());
  }

  @Bean
  public PgLeaseSweepScheduler pgLeaseSweepScheduler(
      ObjectProvider<PgLeaseEntityLocker> lockerProvider) {
    return new PgLeaseSweepScheduler(lockerProvider.getIfAvailable());
  }

  /**
   * Gauge for currently held leases ({@code <metrics.prefix>.entity_locks.held}). Registered as a
   * {@link MeterBinder} so Boot applies it to every registry; reads {@code NaN} when the count
   * query fails so a flaky database never breaks scrapes.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass({MeterRegistry.class, MeterBinder.class})
  static class LeaseMetricsConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "outboxEntityLockLeaseGauge")
    public MeterBinder outboxEntityLockLeaseGauge(
        ObjectProvider<PgLeaseEntityLocker> lockerProvider, OutboxProperties properties) {
      String metricName = properties.getMetrics().getPrefix() + ".entity_locks.held";
      return registry -> {
        PgLeaseEntityLocker locker = lockerProvider.getIfAvailable();
        if (locker == null) {
          return;
        }
        Gauge.builder(
                metricName,
                locker,
                l -> {
                  try {
                    return (double) l.countLiveLeases();
                  } catch (RuntimeException ex) {
                    return Double.NaN;
                  }
                })
            .description("Currently held entity-lock leases (expires_at > now())")
            .register(registry);
      };
    }
  }
}
