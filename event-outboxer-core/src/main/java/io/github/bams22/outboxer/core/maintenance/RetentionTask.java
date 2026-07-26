/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.maintenance;

import io.github.bams22.outboxer.core.config.RetentionConfig;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic retention pass over the archive table and the accumulated {@code DISABLED} rows
 * (ADR-0019). Scheduled by {@link MaintenanceScheduler} only when at least one threshold of
 * {@link RetentionConfig} is set — retention is strictly opt-in.
 *
 * <p>Each pass loops in {@code batchSize}-bounded DELETE statements until a batch comes back
 * short, so a single pass fully catches up regardless of backlog while never holding a
 * long-running transaction.
 */
public final class RetentionTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(RetentionTask.class);

  private final OutboxAdmin admin;
  private final Clock clock;
  private final RetentionConfig config;

  public RetentionTask(OutboxAdmin admin, Clock clock, RetentionConfig config) {
    this.admin = Objects.requireNonNull(admin, "admin must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.config = Objects.requireNonNull(config, "config must not be null");
  }

  /** Interval between passes, exposed for the scheduler. */
  public Duration interval() {
    return config.interval();
  }

  @Override
  public void run() {
    Duration archiveAge = config.archiveOlderThan();
    if (archiveAge != null) {
      Instant threshold = clock.now().minus(archiveAge);
      sweep("archive", () -> admin.purgeArchive(threshold, config.batchSize()));
    }
    Duration disabledAge = config.disabledOlderThan();
    if (disabledAge != null) {
      Instant threshold = clock.now().minus(disabledAge);
      sweep("disabled", () -> admin.purgeDisabled(null, threshold, config.batchSize()));
    }
  }

  private void sweep(String what, IntSupplier purgeBatch) {
    long total = 0;
    try {
      int purged;
      do {
        purged = purgeBatch.getAsInt();
        total += purged;
      } while (purged >= config.batchSize());
      if (total > 0) {
        log.info("retention: purged {} {} row(s)", total, what);
      }
    } catch (RuntimeException ex) {
      log.warn(
          "retention sweep of {} failed after {} row(s); will retry next pass: {}",
          what,
          total,
          ex.toString());
    }
  }
}
