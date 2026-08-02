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

import io.github.bams22.outboxer.api.observer.DispatchRejectedInfo;
import io.github.bams22.outboxer.api.observer.EngineCrashedInfo;
import io.github.bams22.outboxer.api.observer.EventClaimedInfo;
import io.github.bams22.outboxer.api.observer.EventDeletedInfo;
import io.github.bams22.outboxer.api.observer.EventDisabledInfo;
import io.github.bams22.outboxer.api.observer.EventProcessedInfo;
import io.github.bams22.outboxer.api.observer.EventPublishedInfo;
import io.github.bams22.outboxer.api.observer.EventRetryScheduledInfo;
import io.github.bams22.outboxer.api.observer.EventSkippedInfo;
import io.github.bams22.outboxer.api.observer.HandlerErrorInfo;
import io.github.bams22.outboxer.api.observer.HeartbeatFailedInfo;
import io.github.bams22.outboxer.api.observer.LockAcquisitionInfo;
import io.github.bams22.outboxer.api.observer.LockReleaseInfo;
import io.github.bams22.outboxer.api.observer.OrphansReclaimedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.SerializationErrorInfo;
import io.github.bams22.outboxer.api.observer.StorageErrorInfo;
import io.github.bams22.outboxer.api.observer.StuckHandlerReclaimedInfo;
import io.github.bams22.outboxer.api.observer.UnknownEventTypeInfo;
import io.github.bams22.outboxer.api.observer.WorkerDeregisteredInfo;
import io.github.bams22.outboxer.api.observer.WorkerGracefulStopInfo;
import io.github.bams22.outboxer.api.observer.WorkerRegisteredInfo;
import io.micrometer.core.instrument.MeterRegistry;
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
   * to avoid clashing with other libraries that publish {@code outbox.*} metrics. Configurable via
   * the two-argument constructor; the Spring Boot starter binds {@code
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

  // ==================== Publication ====================

  @Override
  public void onEventPublished(EventPublishedInfo info) {
    incType("events.published", info.eventType());
  }

  // ==================== Processing lifecycle ====================

  @Override
  public void onEventClaimed(EventClaimedInfo info) {
    incType("events.claimed", info.eventType());
  }

  @Override
  public void onEventProcessed(EventProcessedInfo info) {
    incType("events.processed", info.eventType());
    registry
        .timer(metric("events.processing_time"), "event_type", info.eventType())
        .record(info.duration().toNanos(), TimeUnit.NANOSECONDS);
    registry
        .summary(metric("events.attempts"), "event_type", info.eventType())
        .record(info.attempts());
  }

  @Override
  public void onEventRetryScheduled(EventRetryScheduledInfo info) {
    incType("events.retry_scheduled", info.eventType());
  }

  @Override
  public void onEventDisabled(EventDisabledInfo info) {
    incType("events.disabled", info.eventType());
  }

  @Override
  public void onEventDeleted(EventDeletedInfo info) {
    incType("events.deleted", info.eventType());
  }

  @Override
  public void onEventSkipped(EventSkippedInfo info) {
    incType("events.skipped", info.eventType());
  }

  // ==================== Errors & anomalies ====================

  @Override
  public void onHandlerError(HandlerErrorInfo info) {
    incType("handler.errors", info.eventType());
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
  public void onLockAcquisitionFailed(LockAcquisitionInfo info) {
    incType("lock.acquisition_failed", info.eventType());
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
  public void onWorkerDeregistered(WorkerDeregisteredInfo info) {
    inc("workers.deregistered");
  }

  @Override
  public void onHeartbeatFailed(HeartbeatFailedInfo info) {
    inc("heartbeat.failed");
  }

  // ==================== Recovery ====================

  @Override
  public void onOrphansReclaimed(OrphansReclaimedInfo info) {
    registry.counter(metric("orphans.reclaimed")).increment(info.eventCount());
    registry.counter(metric("orphans.dead_workers")).increment(info.deadWorkers().size());
  }

  @Override
  public void onStuckHandlerReclaimed(StuckHandlerReclaimedInfo info) {
    incType("handler.stuck_reclaimed", info.eventType());
  }

  // ==================== Storage ====================

  @Override
  public void onStorageError(StorageErrorInfo info) {
    registry.counter(metric("storage.errors"), "operation", info.operation()).increment();
  }

  // ==================== Dispatch ====================

  @Override
  public void onDispatchRejected(DispatchRejectedInfo info) {
    incType("dispatch.rejected", info.eventType());
  }

  // ==================== Engine crash ====================

  @Override
  public void onEngineCrashed(EngineCrashedInfo info) {
    inc("engine.crashed");
  }
}
