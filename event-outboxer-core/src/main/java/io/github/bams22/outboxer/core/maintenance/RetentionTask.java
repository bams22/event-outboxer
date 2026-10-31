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

import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.RetentionPurgedInfo;
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
 * (ADR-0019). Scheduled by {@link MaintenanceScheduler} only when at least one threshold of {@link
 * RetentionConfig} is set — retention is strictly opt-in.
 *
 * <p>Each pass runs at most 16 {@code batchSize}-bounded DELETE statements per table, yielding
 * earlier when a batch comes back short. Further backlog is handled on the next tick.
 *
 * <p>A failing archive sweep does not skip the disabled sweep (and vice versa); after both sweeps
 * ran — and any partial progress was reported through {@code onRetentionPurged} — the first failure
 * propagates so the {@link MaintenanceScheduler} wrapper reports the run as failed.
 */
public final class RetentionTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(RetentionTask.class);

    private final OutboxAdmin admin;
    private final OutboxListener listener;
    private final Clock clock;
    private final RetentionConfig config;

    public RetentionTask(
            OutboxAdmin admin, OutboxListener listener, Clock clock, RetentionConfig config) {
        this.admin = Objects.requireNonNull(admin, "admin must not be null");
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    /** Interval between passes, exposed for the scheduler. */
    public Duration interval() {
        return config.interval();
    }

    @Override
    public void run() {
        RuntimeException firstFailure = null;
        long archivedPurged = 0;
        Duration archiveAge = config.archiveOlderThan();
        if (archiveAge != null) {
            Instant threshold = clock.now().minus(archiveAge);
            try {
                archivedPurged =
                        sweep("archive", () -> admin.purgeArchive(threshold, config.batchSize()));
            } catch (RuntimeException ex) {
                firstFailure = ex;
            }
        }
        long disabledPurged = 0;
        Duration disabledAge = config.disabledOlderThan();
        if (disabledAge != null) {
            Instant threshold = clock.now().minus(disabledAge);
            try {
                disabledPurged =
                        sweep(
                                "disabled",
                                () -> admin.purgeDisabled(null, threshold, config.batchSize()));
            } catch (RuntimeException ex) {
                if (firstFailure == null) {
                    firstFailure = ex;
                }
            }
        }
        if (archivedPurged + disabledPurged > 0) {
            listener.onRetentionPurged(new RetentionPurgedInfo(archivedPurged, disabledPurged));
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private long sweep(String what, IntSupplier purgeBatch) {
        long total = 0;
        try {
            // Bound each pass: yield to the scheduler even if the backlog keeps growing.
            for (int batches = 0;
                    batches < 16 && !Thread.currentThread().isInterrupted();
                    batches++) {
                int purged = purgeBatch.getAsInt();
                total += purged;
                if (purged < config.batchSize()) break;
            }
            if (total > 0) {
                log.info("retention: purged {} {} row(s)", total, what);
            }
        } catch (RuntimeException ex) {
            log.warn(
                    "retention sweep of {} failed after {} row(s): {}", what, total, ex.toString());
            throw ex;
        }
        return total;
    }
}
