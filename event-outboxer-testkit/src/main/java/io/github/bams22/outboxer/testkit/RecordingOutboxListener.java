/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.testkit;

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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link OutboxListener} that captures every callback into per-type lists. Tests inspect the
 * captured records to assert that the engine emitted the expected events in the expected order.
 */
public final class RecordingOutboxListener implements OutboxListener {

  private final CopyOnWriteArrayList<EventPublishedInfo> published = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<EventClaimedInfo> claimed = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<EventProcessedInfo> processed = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<EventRetryScheduledInfo> retryScheduled =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<EventDisabledInfo> disabled = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<EventDeletedInfo> deleted = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<EventSkippedInfo> skipped = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<HandlerErrorInfo> handlerErrors = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<UnknownEventTypeInfo> unknownType =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<SerializationErrorInfo> serializationErrors =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<LockAcquisitionInfo> lockAcquisitionFailed =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<LockReleaseInfo> lockReleaseFailed =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<WorkerRegisteredInfo> workersRegistered =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<WorkerGracefulStopInfo> workersGracefulStop =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<WorkerDeregisteredInfo> workersDeregistered =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<HeartbeatFailedInfo> heartbeatsFailed =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<OrphansReclaimedInfo> orphansReclaimed =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<StuckHandlerReclaimedInfo> stuckReclaimed =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<StorageErrorInfo> storageErrors = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<DispatchRejectedInfo> dispatchRejected =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<EngineCrashedInfo> engineCrashed =
      new CopyOnWriteArrayList<>();

  /** Reset every captured list. */
  public void clear() {
    published.clear();
    claimed.clear();
    processed.clear();
    retryScheduled.clear();
    disabled.clear();
    deleted.clear();
    skipped.clear();
    handlerErrors.clear();
    unknownType.clear();
    serializationErrors.clear();
    lockAcquisitionFailed.clear();
    lockReleaseFailed.clear();
    workersRegistered.clear();
    workersGracefulStop.clear();
    workersDeregistered.clear();
    heartbeatsFailed.clear();
    orphansReclaimed.clear();
    stuckReclaimed.clear();
    storageErrors.clear();
    dispatchRejected.clear();
    engineCrashed.clear();
  }

  // ---------------------------------------------------------------------------------------------
  // callbacks — just append
  // ---------------------------------------------------------------------------------------------

  @Override
  public void onEventPublished(EventPublishedInfo info) {
    published.add(info);
  }

  @Override
  public void onEventClaimed(EventClaimedInfo info) {
    claimed.add(info);
  }

  @Override
  public void onEventProcessed(EventProcessedInfo info) {
    processed.add(info);
  }

  @Override
  public void onEventRetryScheduled(EventRetryScheduledInfo info) {
    retryScheduled.add(info);
  }

  @Override
  public void onEventDisabled(EventDisabledInfo info) {
    disabled.add(info);
  }

  @Override
  public void onEventDeleted(EventDeletedInfo info) {
    deleted.add(info);
  }

  @Override
  public void onEventSkipped(EventSkippedInfo info) {
    skipped.add(info);
  }

  @Override
  public void onHandlerError(HandlerErrorInfo info) {
    handlerErrors.add(info);
  }

  @Override
  public void onUnknownEventType(UnknownEventTypeInfo info) {
    unknownType.add(info);
  }

  @Override
  public void onEventSerializationError(SerializationErrorInfo info) {
    serializationErrors.add(info);
  }

  @Override
  public void onLockAcquisitionFailed(LockAcquisitionInfo info) {
    lockAcquisitionFailed.add(info);
  }

  @Override
  public void onLockReleaseFailed(LockReleaseInfo info) {
    lockReleaseFailed.add(info);
  }

  @Override
  public void onWorkerRegistered(WorkerRegisteredInfo info) {
    workersRegistered.add(info);
  }

  @Override
  public void onWorkerGracefulStop(WorkerGracefulStopInfo info) {
    workersGracefulStop.add(info);
  }

  @Override
  public void onWorkerDeregistered(WorkerDeregisteredInfo info) {
    workersDeregistered.add(info);
  }

  @Override
  public void onHeartbeatFailed(HeartbeatFailedInfo info) {
    heartbeatsFailed.add(info);
  }

  @Override
  public void onOrphansReclaimed(OrphansReclaimedInfo info) {
    orphansReclaimed.add(info);
  }

  @Override
  public void onStuckHandlerReclaimed(StuckHandlerReclaimedInfo info) {
    stuckReclaimed.add(info);
  }

  @Override
  public void onStorageError(StorageErrorInfo info) {
    storageErrors.add(info);
  }

  @Override
  public void onDispatchRejected(DispatchRejectedInfo info) {
    dispatchRejected.add(info);
  }

  @Override
  public void onEngineCrashed(EngineCrashedInfo info) {
    engineCrashed.add(info);
  }

  // ---------------------------------------------------------------------------------------------
  // accessors — snapshots for assertions
  // ---------------------------------------------------------------------------------------------

  public List<EventPublishedInfo> published() {
    return List.copyOf(published);
  }

  public List<EventClaimedInfo> claimed() {
    return List.copyOf(claimed);
  }

  public List<EventProcessedInfo> processed() {
    return List.copyOf(processed);
  }

  public List<EventRetryScheduledInfo> retryScheduled() {
    return List.copyOf(retryScheduled);
  }

  public List<EventDisabledInfo> disabled() {
    return List.copyOf(disabled);
  }

  public List<EventDeletedInfo> deleted() {
    return List.copyOf(deleted);
  }

  public List<EventSkippedInfo> skipped() {
    return List.copyOf(skipped);
  }

  public List<HandlerErrorInfo> handlerErrors() {
    return List.copyOf(handlerErrors);
  }

  public List<UnknownEventTypeInfo> unknownEventTypes() {
    return List.copyOf(unknownType);
  }

  public List<SerializationErrorInfo> serializationErrors() {
    return List.copyOf(serializationErrors);
  }

  public List<LockAcquisitionInfo> lockAcquisitionFailed() {
    return List.copyOf(lockAcquisitionFailed);
  }

  public List<LockReleaseInfo> lockReleaseFailed() {
    return List.copyOf(lockReleaseFailed);
  }

  public List<WorkerRegisteredInfo> workersRegistered() {
    return List.copyOf(workersRegistered);
  }

  public List<WorkerGracefulStopInfo> workersGracefulStop() {
    return List.copyOf(workersGracefulStop);
  }

  public List<WorkerDeregisteredInfo> workersDeregistered() {
    return List.copyOf(workersDeregistered);
  }

  public List<HeartbeatFailedInfo> heartbeatsFailed() {
    return List.copyOf(heartbeatsFailed);
  }

  public List<OrphansReclaimedInfo> orphansReclaimed() {
    return List.copyOf(orphansReclaimed);
  }

  public List<StuckHandlerReclaimedInfo> stuckReclaimed() {
    return List.copyOf(stuckReclaimed);
  }

  public List<StorageErrorInfo> storageErrors() {
    return List.copyOf(storageErrors);
  }

  public List<DispatchRejectedInfo> dispatchRejected() {
    return List.copyOf(dispatchRejected);
  }

  public List<EngineCrashedInfo> engineCrashed() {
    return List.copyOf(engineCrashed);
  }
}
