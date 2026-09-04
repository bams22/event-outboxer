/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.target.outboxer;

import io.github.bams22.outboxer.api.observer.EventRetryScheduledInfo;
import io.github.bams22.outboxer.api.observer.HandlerAbandonedInfo;
import io.github.bams22.outboxer.api.observer.OrphansReclaimedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.StaleClaimsSweptInfo;
import io.github.bams22.outboxer.api.observer.StorageErrorInfo;
import io.github.bams22.outboxer.api.observer.StuckHandlerReclaimedInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs every recovery action the engine takes — orphan reclaim, stale-claim sweep, stuck-handler
 * reclaim, abandoned handler, retry scheduling other than lock-busy, storage errors — at WARN, so a
 * chaos run's duplicates can be traced to the mechanism that re-queued the event. Root logging in
 * the worker contexts is WARN; the engine itself reports these only through the listener.
 */
public final class RecoveryLogListener implements OutboxListener {

    private static final Logger log = LoggerFactory.getLogger("bench.recovery");

    private final String workerId;

    public RecoveryLogListener(String workerId) {
        this.workerId = workerId;
        log.warn("RECOVERY [{}] listener registered", workerId);
    }

    @Override
    public void onOrphansReclaimed(OrphansReclaimedInfo info) {
        log.warn(
                "RECOVERY orphans-reclaimed dead={} events={}",
                info.deadWorkers(),
                info.eventCount());
    }

    @Override
    public void onStaleClaimsSwept(StaleClaimsSweptInfo info) {
        log.warn(
                "RECOVERY [{}] stale-swept count={} threshold={}",
                workerId,
                info.count(),
                info.threshold());
    }

    @Override
    public void onStuckHandlerReclaimed(StuckHandlerReclaimedInfo info) {
        log.warn("RECOVERY [{}] stuck-reclaimed {}", workerId, info);
    }

    @Override
    public void onHandlerAbandoned(HandlerAbandonedInfo info) {
        log.warn("RECOVERY [{}] handler-abandoned {}", workerId, info);
    }

    @Override
    public void onEventRetryScheduled(EventRetryScheduledInfo info) {
        if (info.trigger() != EventRetryScheduledInfo.Trigger.LOCK_BUSY) {
            log.warn(
                    "RECOVERY [{}] retry-scheduled id={} attempts={} trigger={} reason={}",
                    workerId,
                    info.eventId(),
                    info.attempts(),
                    info.trigger(),
                    info.reason());
        }
    }

    @Override
    public void onMaintenanceRunCompleted(
            io.github.bams22.outboxer.api.observer.MaintenanceRunInfo info) {
        if (info.result() != io.github.bams22.outboxer.api.observer.MaintenanceRunInfo.Result.OK) {
            log.warn("RECOVERY [{}] maintenance {}", workerId, info);
        }
    }

    @Override
    public void onStorageError(StorageErrorInfo info) {
        log.warn("RECOVERY [{}] storage-error {}", workerId, info);
    }
}
