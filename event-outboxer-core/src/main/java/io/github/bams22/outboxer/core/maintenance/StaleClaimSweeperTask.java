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
import io.github.bams22.outboxer.api.observer.StaleClaimsSweptInfo;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.spi.EventStore;
import java.time.Duration;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Last-line safety net over the whole fleet: periodically returns {@code PROCESSING} rows whose
 * {@code claimed_at} is older than the threshold to {@code PENDING} — rows invisible to both the
 * watchdog (never entered the in-flight registry) and orphan recovery (the owning worker is alive
 * and heartbeating). Uses the partial index over {@code PROCESSING} rows created for exactly this
 * scan in migration V001.
 *
 * <p>The threshold is guaranteed by {@code OutboxEngineBuilder} to exceed every per-type {@code
 * handlerMaxRuntime}: registered in-flight rows are force-reclaimed by the watchdog long before
 * this task could see them, so anything swept here was genuinely abandoned. Swept rows are logged
 * at WARN — they indicate a bug or an incident, not normal operation.
 *
 * <p><b>Construction.</b> {@code StaleClaimSweeperTask.builder()} — see the constructor for
 * required collaborators and defaults.
 */
public final class StaleClaimSweeperTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(StaleClaimSweeperTask.class);

    private final EventStore store;
    private final OutboxListener listener;
    private final Duration threshold;
    private final Duration interval;
    private final int batchSize;

    /**
     * Builder-backed constructor; parameter names are the builder's method names. Required: {@code
     * store} and {@code threshold} (the engine derives it from {@code
     * MaintenanceConfig.staleClaimThreshold} and every per-type {@code handlerMaxRuntime}).
     * Defaults: {@link OutboxListener#NOOP}, {@code interval} = {@code
     * MaintenanceConfig.defaults().staleClaimSweepInterval()}, {@code batchSize} = {@code
     * MaintenanceConfig.defaults().reclaimBatchSize()}; an explicit batch size must be positive.
     */
    @Builder
    private StaleClaimSweeperTask(
            EventStore store,
            @Nullable OutboxListener listener,
            Duration threshold,
            @Nullable Duration interval,
            @Nullable Integer batchSize) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.listener = listener != null ? listener : OutboxListener.NOOP;
        this.threshold = Objects.requireNonNull(threshold, "threshold must not be null");
        this.interval =
                interval != null
                        ? interval
                        : MaintenanceConfig.defaults().staleClaimSweepInterval();
        int resolvedBatchSize =
                batchSize != null ? batchSize : MaintenanceConfig.defaults().reclaimBatchSize();
        if (resolvedBatchSize <= 0) {
            throw new IllegalArgumentException(
                    "batchSize must be positive, got " + resolvedBatchSize);
        }
        this.batchSize = resolvedBatchSize;
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
                listener.onStaleClaimsSwept(new StaleClaimsSweptInfo(total, threshold));
            }
        } catch (RuntimeException ex) {
            log.warn("stale-claim sweep failed after {} row(s): {}", total, ex.toString());
            throw ex;
        }
    }
}
