/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.listener;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link OutboxListener} that routes engine callbacks to SLF4J. Hot-path callbacks ({@code
 * onEventPublished}, {@code onEventClaimed}, {@code onEventProcessed}) log at DEBUG; lifecycle
 * transitions at INFO; storage / handler / lock failures at WARN.
 */
public final class LoggingOutboxListener implements OutboxListener {

  private static final Logger log = LoggerFactory.getLogger(LoggingOutboxListener.class);

  @Override
  public void onEventPublished(EventPublishedInfo info) {
    log.debug(
        "published eventId={} type={} runAt={} priority={}",
        info.eventId(),
        info.eventType(),
        info.runAt(),
        info.priority());
  }

  @Override
  public void onEventClaimed(EventClaimedInfo info) {
    log.debug("claimed {}", info);
  }

  @Override
  public void onEventProcessed(EventProcessedInfo info) {
    log.debug(
        "processed eventId={} type={} attempts={} duration={}",
        info.eventId(),
        info.eventType(),
        info.attempts(),
        info.duration());
  }

  @Override
  public void onEventRetryScheduled(EventRetryScheduledInfo info) {
    log.info("retry scheduled {}", info);
  }

  @Override
  public void onEventDisabled(EventDisabledInfo info) {
    log.warn("event disabled {}", info);
  }

  @Override
  public void onEventDeleted(EventDeletedInfo info) {
    log.info("event deleted {}", info);
  }

  @Override
  public void onEventSkipped(EventSkippedInfo info) {
    log.debug("skipped {}", info);
  }

  @Override
  public void onHandlerError(HandlerErrorInfo info) {
    log.warn("handler error {}", info);
  }

  @Override
  public void onUnknownEventType(UnknownEventTypeInfo info) {
    log.warn("unknown event type {}", info);
  }

  @Override
  public void onEventSerializationError(SerializationErrorInfo info) {
    log.warn("serialization error {}", info);
  }

  @Override
  public void onLockAcquisitionFailed(LockAcquisitionInfo info) {
    log.debug("lock busy {}", info);
  }

  @Override
  public void onLockReleaseFailed(LockReleaseInfo info) {
    log.warn("lock release error {}", info);
  }

  @Override
  public void onWorkerRegistered(WorkerRegisteredInfo info) {
    log.info("worker registered {}", info);
  }

  @Override
  public void onWorkerGracefulStop(WorkerGracefulStopInfo info) {
    log.info("worker graceful stop {}", info);
  }

  @Override
  public void onWorkerDeregistered(WorkerDeregisteredInfo info) {
    log.info("worker deregistered {}", info);
  }

  @Override
  public void onHeartbeatFailed(HeartbeatFailedInfo info) {
    log.warn("heartbeat failed {}", info);
  }

  @Override
  public void onOrphansReclaimed(OrphansReclaimedInfo info) {
    log.info("orphans reclaimed {}", info);
  }

  @Override
  public void onStuckHandlerReclaimed(StuckHandlerReclaimedInfo info) {
    log.warn("stuck handler reclaimed {}", info);
  }

  @Override
  public void onStorageError(StorageErrorInfo info) {
    log.warn("storage error {}", info);
  }

  @Override
  public void onDispatchRejected(DispatchRejectedInfo info) {
    log.warn("dispatch rejected {}", info);
  }

  @Override
  public void onEngineCrashed(EngineCrashedInfo info) {
    log.error(
        "ENGINE CRASHED on worker {} at {} — {}",
        info.workerId(),
        info.at(),
        info.reason(),
        info.cause());
  }
}
