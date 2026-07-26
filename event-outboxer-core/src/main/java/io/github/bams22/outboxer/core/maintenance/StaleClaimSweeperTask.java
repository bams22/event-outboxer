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

import io.github.bams22.outboxer.spi.EventStore;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Last-line safety net over the whole fleet: periodically returns {@code PROCESSING} rows whose
 * {@code claimed_at} is older than the threshold to {@code PENDING} — rows invisible to both the
 * watchdog (never entered the in-flight registry) and orphan recovery (the owning worker is
 * alive and heartbeating). Uses the partial index over {@code PROCESSING} rows created for
 * exactly this scan in migration V001.
 *
 * <p>The threshold is guaranteed by {@code OutboxEngineBuilder} to exceed every per-type
 * {@code handlerMaxRuntime}: registered in-flight rows are force-reclaimed by the watchdog long
 * before this task could see them, so anything swept here was genuinely abandoned. Swept rows
 * are logged at WARN — they indicate a bug or an incident, not normal operation.
 */
public final class StaleClaimSweeperTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(StaleClaimSweeperTask.class);

  private final EventStore store;
  private final Duration threshold;
  private final Duration interval;
  private final int batchSize;

  public StaleClaimSweeperTask(
      EventStore store, Duration threshold, Duration interval, int batchSize) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.threshold = Objects.requireNonNull(threshold, "threshold must not be null");
    this.interval = Objects.requireNonNull(interval, "interval must not be null");
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive, got " + batchSize);
    }
    this.batchSize = batchSize;
  }

  /** Cadence of the sweep, exposed for the scheduler. */
  public Duration interval() {
    return interval;
  }

  @Override
  public void run() {
    long total = 0;
    try {
      int swept;
      do {
        swept = store.sweepStale(threshold, batchSize);
        total += swept;
      } while (swept >= batchSize);
      if (total > 0) {
        log.warn(
            "swept {} stale PROCESSING claim(s) older than {} back to PENDING — "
                + "these rows were invisible to the watchdog and orphan recovery",
            total,
            threshold);
      }
    } catch (RuntimeException ex) {
      log.warn(
          "stale-claim sweep failed after {} row(s); will retry next pass: {}",
          total,
          ex.toString());
    }
  }
}
