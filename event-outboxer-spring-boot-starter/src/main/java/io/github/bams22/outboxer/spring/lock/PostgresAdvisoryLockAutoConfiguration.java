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

import io.github.bams22.outboxer.lock.postgres.advisory.PgAdvisoryLocker;
import io.github.bams22.outboxer.spi.EntityLocker;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

/**
 * Registers {@link PgAdvisoryLocker} when {@code event-outboxer.lock.type=postgres-advisory}, the
 * adapter class is on the classpath, and a {@link DataSource} bean is available.
 *
 * <p>Since ADR-0022 the {@code postgres-lease} value selects the lease-table locker (see {@link
 * PostgresLeaseLockAutoConfiguration}); the session-scoped advisory locker stays available as the
 * explicit {@code postgres-advisory} opt-out for users who want immediate lock release on clean
 * process death and accept one pinned pooled connection per held lock plus the pgBouncer
 * transaction-pooling incompatibility.
 *
 * <p>The locker intentionally uses the raw {@code DataSource} (not the transaction-aware proxy):
 * advisory locks must run on a connection that is NOT bound to the caller's transaction, so that a
 * lock acquired before handler work survives the caller's commit/rollback boundary.
 */
@AutoConfiguration(
    after = org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class)
@ConditionalOnClass(PgAdvisoryLocker.class)
@ConditionalOnBean(DataSource.class)
@Conditional(OnPostgresAdvisoryLockCondition.class)
public class PostgresAdvisoryLockAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(EntityLocker.class)
  public EntityLocker outboxEntityLocker(DataSource dataSource) {
    return new PgAdvisoryLocker(dataSource);
  }
}
