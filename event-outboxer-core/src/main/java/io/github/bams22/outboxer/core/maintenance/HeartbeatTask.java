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

import io.github.bams22.outboxer.api.observer.HeartbeatFailedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.WorkerRegisteredInfo;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic task that writes a fresh heartbeat for the current worker in the {@code
 * event_outboxer.workers} table. One instance per running engine; scheduled by {@link
 * MaintenanceScheduler} at the cadence defined in {@code MaintenanceConfig.heartbeatInterval()}.
 *
 * <p>If the heartbeat reports that the worker row no longer exists — a peer's orphan recovery
 * reaped it during a long GC pause or database outage exceeding the dead threshold — the task
 * re-registers the worker immediately. Without that, a reaped-but-alive worker keeps claiming
 * events with no registry row, and a later crash would leave those events invisible to orphan
 * recovery forever.
 */
public final class HeartbeatTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatTask.class);

    private final WorkerRegistry registry;
    private final WorkerInfo workerInfo;
    private final Clock clock;
    private final OutboxListener listener;

    public HeartbeatTask(
            WorkerRegistry registry, WorkerInfo workerInfo, Clock clock, OutboxListener listener) {
        this.registry = Objects.requireNonNull(registry);
        this.workerInfo = Objects.requireNonNull(workerInfo);
        this.clock = Objects.requireNonNull(clock);
        this.listener = Objects.requireNonNull(listener);
    }

    @Override
    public void run() {
        try {
            boolean updated = registry.heartbeat(workerInfo.id(), clock.now());
            if (!updated) {
                log.warn(
                        "heartbeat row not found for worker {} (reaped by peer orphan recovery?) — "
                                + "re-registering",
                        workerInfo.id());
                registry.register(workerInfo);
                listener.onWorkerRegistered(new WorkerRegisteredInfo(workerInfo));
            }
        } catch (RuntimeException ex) {
            listener.onHeartbeatFailed(new HeartbeatFailedInfo(workerInfo.id(), ex));
            log.warn("heartbeat failed for worker {}: {}", workerInfo.id(), ex.toString());
        }
    }
}
