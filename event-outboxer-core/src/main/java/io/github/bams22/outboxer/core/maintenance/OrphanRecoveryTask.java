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

import io.github.bams22.outboxer.api.observer.OrphansReclaimedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic task that finds stale-heartbeat workers, reclaims their claimed events back to {@code
 * PENDING}, and removes the dead worker rows. {@code reclaimOrphans} and {@code removeDead} are two
 * independent calls (each atomic on its own, not jointly): if {@code removeDead} fails after a
 * successful reclaim, the dead worker rows survive until the next pass, where reclaiming their (now
 * zero) events again is a no-op — the flow is idempotent, not transactional.
 *
 * <p><b>Construction.</b> {@code OrphanRecoveryTask.builder()} — see the constructor for required
 * collaborators and defaults.
 */
public final class OrphanRecoveryTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OrphanRecoveryTask.class);

    private final WorkerRegistry registry;
    private final EventStore store;
    private final Clock clock;
    private final MaintenanceConfig config;
    private final OutboxListener listener;

    /**
     * Builder-backed constructor; parameter names are the builder's method names. Required: {@code
     * registry} and {@code store}. Defaults: {@link Clock#system()}, {@link
     * MaintenanceConfig#defaults()}, {@link OutboxListener#NOOP}.
     */
    @Builder
    private OrphanRecoveryTask(
            WorkerRegistry registry,
            EventStore store,
            @Nullable Clock clock,
            @Nullable MaintenanceConfig config,
            @Nullable OutboxListener listener) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.clock = clock != null ? clock : Clock.system();
        this.config = config != null ? config : MaintenanceConfig.defaults();
        this.listener = listener != null ? listener : OutboxListener.NOOP;
    }

    @Override
    public void run() {
        try {
            List<WorkerInfo> dead;
            try {
                dead = registry.findDead(config.deadThreshold(), config.reclaimBatchSize());
            } catch (RuntimeException ex) {
                log.warn("findDead failed: {}", ex.toString());
                return;
            }
            if (dead.isEmpty()) {
                return;
            }
            List<WorkerId> ids = dead.stream().map(WorkerInfo::id).toList();
            int reclaimed = store.reclaimOrphans(ids, clock.now());
            try {
                registry.removeDead(ids);
            } catch (RuntimeException ex) {
                log.warn(
                        "removeDead failed after reclaiming {} events; will retry next pass: {}",
                        reclaimed,
                        ex.toString());
            }
            listener.onOrphansReclaimed(new OrphansReclaimedInfo(ids, reclaimed));
        } catch (RuntimeException ex) {
            log.warn("orphan recovery pass failed: {}", ex.toString(), ex);
        }
    }
}
