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
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Schedules {@link PgLeaseEntityLocker#sweepExpired()} on a dedicated single-thread daemon executor
 * at a fixed 10-minute cadence (ADR-0022 §Starter integration — deliberately not configurable in
 * MVP). The sweep is cosmetic garbage collection: expired rows are overwritten in place by the next
 * acquirer, so any cadence — and any missed run — is safe; failures are logged and never rethrown.
 *
 * <p>Core's {@code MaintenanceScheduler} has no task-registration hook, and adding one for a
 * cosmetic cleanup is not justified — hence the starter-owned thread.
 */
final class PgLeaseSweepScheduler implements InitializingBean, DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(PgLeaseSweepScheduler.class);

  private static final Duration CADENCE = Duration.ofMinutes(10);

  private final @Nullable PgLeaseEntityLocker locker;
  private @Nullable ScheduledExecutorService executor;

  /**
   * A {@code null} locker disables the sweep — used when a user-defined {@code EntityLocker}
   * displaced the lease locker.
   */
  PgLeaseSweepScheduler(@Nullable PgLeaseEntityLocker locker) {
    this.locker = locker;
  }

  @Override
  public void afterPropertiesSet() {
    if (locker == null) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("outbox-entity-locks-sweep").daemon().factory());
    executor.scheduleWithFixedDelay(
        this::sweepQuietly, CADENCE.toMillis(), CADENCE.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void sweepQuietly() {
    try {
      int removed = locker.sweepExpired();
      if (removed > 0) {
        log.debug("entity_locks sweep removed {} expired lease(s)", removed);
      }
    } catch (RuntimeException ex) {
      log.warn("entity_locks sweep failed (will retry next cycle): {}", ex.toString());
    }
  }

  @Override
  public void destroy() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}
