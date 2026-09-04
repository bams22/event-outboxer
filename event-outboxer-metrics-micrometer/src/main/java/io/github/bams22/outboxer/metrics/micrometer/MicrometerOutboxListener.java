/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.metrics.micrometer;

import io.github.bams22.outboxer.api.observer.EngineCrashedInfo;
import io.github.bams22.outboxer.api.observer.EventClaimedInfo;
import io.github.bams22.outboxer.api.observer.EventCoalescedInfo;
import io.github.bams22.outboxer.api.observer.EventDeletedInfo;
import io.github.bams22.outboxer.api.observer.EventDisabledInfo;
import io.github.bams22.outboxer.api.observer.EventProcessedInfo;
import io.github.bams22.outboxer.api.observer.EventPublishedInfo;
import io.github.bams22.outboxer.api.observer.EventRetryScheduledInfo;
import io.github.bams22.outboxer.api.observer.EventSkippedInfo;
import io.github.bams22.outboxer.api.observer.HandlerAbandonedInfo;
import io.github.bams22.outboxer.api.observer.HandlerErrorInfo;
import io.github.bams22.outboxer.api.observer.HeartbeatFailedInfo;
import io.github.bams22.outboxer.api.observer.LockAcquiredInfo;
import io.github.bams22.outboxer.api.observer.LockAcquisitionInfo;
import io.github.bams22.outboxer.api.observer.LockReleaseInfo;
import io.github.bams22.outboxer.api.observer.MaintenanceRunInfo;
import io.github.bams22.outboxer.api.observer.OrphansReclaimedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.PollCompletedInfo;
import io.github.bams22.outboxer.api.observer.PollerSaturatedInfo;
import io.github.bams22.outboxer.api.observer.RetentionPurgedInfo;
import io.github.bams22.outboxer.api.observer.SerializationErrorInfo;
import io.github.bams22.outboxer.api.observer.StaleClaimsSweptInfo;
import io.github.bams22.outboxer.api.observer.StorageErrorInfo;
import io.github.bams22.outboxer.api.observer.StuckHandlerReclaimedInfo;
import io.github.bams22.outboxer.api.observer.UnknownEventTypeInfo;
import io.github.bams22.outboxer.api.observer.WorkerGracefulStopInfo;
import io.github.bams22.outboxer.api.observer.WorkerRegisteredInfo;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * {@link OutboxListener} implementation that publishes engine callbacks to a Micrometer {@link
 * MeterRegistry}. Metric names follow the convention documented in STORAGE.md §Monitoring; every
 * event-keyed metric carries an {@code event_type} tag so dashboards can drill down.
 *
 * <p>All callbacks are O(1) — Micrometer's internal meter lookup is a concurrent map keyed on
 * {@code (name, tags)}, fast enough for the hot dispatcher path.
 */
public final class MicrometerOutboxListener implements OutboxListener {

    /**
     * Prefix applied to every metric name. Default: {@code event_outboxer} — a specific name chosen
     * to avoid clashing with other libraries that publish {@code outbox.*} metrics. Configurable
     * via the two-argument constructor; the Spring Boot starter binds {@code
     * event-outboxer.metrics.prefix} into the same slot.
     */
    public static final String DEFAULT_PREFIX = "event_outboxer";

    private final MeterRegistry registry;
    private final String prefix;

    public MicrometerOutboxListener(MeterRegistry registry) {
        this(registry, DEFAULT_PREFIX);
    }

    public MicrometerOutboxListener(MeterRegistry registry, String prefix) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.prefix = Objects.requireNonNull(prefix, "prefix must not be null");
    }

    private String metric(String name) {
        return prefix + "." + name;
    }

    private void incType(String name, String eventType) {
        registry.counter(metric(name), "event_type", eventType).increment();
    }

    private void inc(String name) {
        registry.counter(metric(name)).increment();
    }

    private static String tagValue(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }

    private void recordAttempts(String eventType, String outcome, int attempts) {
        registry.summary(metric("events.attempts"), "event_type", eventType, "outcome", outcome)
                .record(attempts);
    }

    // ==================== Publication ====================

    @Override
    public void onEventPublished(EventPublishedInfo info) {
        incType("events.published", info.eventType());
    }

    @Override
    public void onEventCoalesced(EventCoalescedInfo info) {
        incType("events.coalesced", info.eventType());
    }

    // ==================== Polling ====================

    @Override
    public void onPollCompleted(PollCompletedInfo info) {
        registry.counter(
                        metric("poller.polls"),
                        "event_type",
                        info.eventType(),
                        "result",
                        info.claimed() > 0 ? "claimed" : "empty")
                .increment();
        registry.timer(metric("poller.claim_time"), "event_type", info.eventType())
                .record(info.duration().toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void onPollerSaturated(PollerSaturatedInfo info) {
        incType("poller.saturated", info.eventType());
    }

    // ==================== Processing lifecycle ====================

    @Override
    public void onEventClaimed(EventClaimedInfo info) {
        Duration queueTime = Duration.between(info.createdAt(), info.claimedAt());
        if (queueTime.isNegative()) {
            // Clock skew between the publishing and the claiming node — clamp at zero rather
            // than poison the timer with a negative sample.
            queueTime = Duration.ZERO;
        }
        registry.timer(metric("events.queue_time"), "event_type", info.eventType())
                .record(queueTime.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void onEventProcessed(EventProcessedInfo info) {
        registry.timer(metric("events.processing_time"), "event_type", info.eventType())
                .record(info.duration().toNanos(), TimeUnit.NANOSECONDS);
        recordAttempts(info.eventType(), "processed", info.attempts());
    }

    @Override
    public void onEventRetryScheduled(EventRetryScheduledInfo info) {
        registry.counter(
                        metric("events.retry_scheduled"),
                        "event_type",
                        info.eventType(),
                        "reason",
                        tagValue(info.trigger()))
                .increment();
    }

    @Override
    public void onEventDisabled(EventDisabledInfo info) {
        registry.counter(
                        metric("events.disabled"),
                        "event_type",
                        info.eventType(),
                        "reason",
                        tagValue(info.trigger()))
                .increment();
        recordAttempts(info.eventType(), "disabled", info.attempts());
    }

    @Override
    public void onEventDeleted(EventDeletedInfo info) {
        incType("events.deleted", info.eventType());
        recordAttempts(info.eventType(), "deleted", info.attempts());
    }

    @Override
    public void onEventSkipped(EventSkippedInfo info) {
        incType("events.skipped", info.eventType());
    }

    // ==================== Errors & anomalies ====================

    @Override
    public void onHandlerError(HandlerErrorInfo info) {
        registry.counter(
                        metric("handler.errors"),
                        "event_type",
                        info.eventType(),
                        "exception",
                        info.cause().getClass().getSimpleName())
                .increment();
        registry.timer(metric("handler.error_time"), "event_type", info.eventType())
                .record(info.duration().toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void onUnknownEventType(UnknownEventTypeInfo info) {
        incType("events.unknown_type", info.eventType());
    }

    @Override
    public void onEventSerializationError(SerializationErrorInfo info) {
        incType("events.serialization_errors", info.eventType());
    }

    @Override
    public void onLockAcquired(LockAcquiredInfo info) {
        recordLockWait(info.eventType(), "acquired", info.waited());
    }

    @Override
    public void onLockAcquisitionFailed(LockAcquisitionInfo info) {
        registry.counter(
                        metric("lock.acquisition_failed"),
                        "event_type",
                        info.eventType(),
                        "outcome",
                        tagValue(info.outcome()))
                .increment();
        if (info.outcome() == LockAcquisitionInfo.Outcome.BUSY) {
            recordLockWait(info.eventType(), "busy", info.waited());
        }
    }

    /**
     * Time spent in {@code EntityLocker.tryLock(...)} per outcome (ADR-0035). Immediate
     * acquisitions record ~0, so the {@code acquired} series' {@code _count} is the total number of
     * acquisitions and its histogram shows which share needed the bounded wait; {@code busy}
     * records the whole spent {@code lockWait} budget.
     */
    private void recordLockWait(String eventType, String outcome, java.time.Duration waited) {
        registry.timer(metric("lock.wait_time"), "event_type", eventType, "outcome", outcome)
                .record(waited.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void onLockReleaseFailed(LockReleaseInfo info) {
        incType("lock.release_failed", info.eventType());
    }

    // ==================== Worker lifecycle ====================

    @Override
    public void onWorkerRegistered(WorkerRegisteredInfo info) {
        inc("workers.registered");
    }

    @Override
    public void onWorkerGracefulStop(WorkerGracefulStopInfo info) {
        inc("workers.graceful_stops");
    }

    @Override
    public void onHeartbeatFailed(HeartbeatFailedInfo info) {
        inc("heartbeat.failed");
    }

    // ==================== Recovery ====================

    @Override
    public void onOrphansReclaimed(OrphansReclaimedInfo info) {
        registry.counter(metric("orphans.reclaimed")).increment(info.eventCount());
    }

    @Override
    public void onStuckHandlerReclaimed(StuckHandlerReclaimedInfo info) {
        registry.timer(metric("handler.stuck_time"), "event_type", info.eventType())
                .record(info.elapsed().toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void onHandlerAbandoned(HandlerAbandonedInfo info) {
        incType("handler.abandoned", info.eventType());
    }

    // ==================== Maintenance ====================

    @Override
    public void onStaleClaimsSwept(StaleClaimsSweptInfo info) {
        registry.counter(metric("claims.stale_swept")).increment(info.count());
    }

    @Override
    public void onRetentionPurged(RetentionPurgedInfo info) {
        if (info.archivedPurged() > 0) {
            registry.counter(metric("retention.purged"), "kind", "archive")
                    .increment(info.archivedPurged());
        }
        if (info.disabledPurged() > 0) {
            registry.counter(metric("retention.purged"), "kind", "disabled")
                    .increment(info.disabledPurged());
        }
    }

    @Override
    public void onMaintenanceRunCompleted(MaintenanceRunInfo info) {
        registry.counter(
                        metric("maintenance.runs"),
                        "task",
                        info.task(),
                        "result",
                        tagValue(info.result()))
                .increment();
    }

    // ==================== Storage ====================

    @Override
    public void onStorageError(StorageErrorInfo info) {
        registry.counter(metric("storage.errors"), "operation", info.operation()).increment();
    }

    // ==================== Engine crash ====================

    @Override
    public void onEngineCrashed(EngineCrashedInfo info) {
        inc("engine.crashed");
    }
}
