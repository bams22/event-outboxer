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

import io.github.bams22.outboxer.lock.postgres.PgAdvisoryLocker;
import io.github.bams22.outboxer.spi.EntityLocker;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Registers {@link PgAdvisoryLocker} when {@code outbox.lock.type=postgres}, the adapter class
 * is on the classpath, and a {@link DataSource} bean is available.
 *
 * <p>The locker intentionally uses the raw {@code DataSource} (not the transaction-aware proxy):
 * advisory locks must run on a connection that is NOT bound to the caller's transaction, so
 * that a lock acquired before handler work survives the caller's commit/rollback boundary.
 */
@AutoConfiguration(after = org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class)
@ConditionalOnClass(PgAdvisoryLocker.class)
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(prefix = "event-outboxer.lock", name = "type", havingValue = "postgres")
public class PostgresLockAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(EntityLocker.class)
  public EntityLocker outboxEntityLocker(DataSource dataSource) {
    return new PgAdvisoryLocker(dataSource);
  }
}
