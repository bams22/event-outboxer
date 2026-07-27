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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fan-out implementation of {@link OutboxListener}. The engine interacts with a single listener
 * instance; the registry delegates each callback to every registered implementation. Exceptions
 * thrown by one listener are caught and logged — they do not prevent other listeners from receiving
 * the same event, and they never propagate back into the engine's processing flow.
 */
public final class OutboxListenerRegistry implements OutboxListener {

  private static final Logger log = LoggerFactory.getLogger(OutboxListenerRegistry.class);

  private final List<OutboxListener> listeners;

  public OutboxListenerRegistry() {
    this(List.of());
  }

  public OutboxListenerRegistry(List<OutboxListener> initial) {
    Objects.requireNonNull(initial, "initial must not be null");
    this.listeners = new CopyOnWriteArrayList<>(initial);
  }

  public void add(OutboxListener listener) {
    Objects.requireNonNull(listener, "listener must not be null");
    listeners.add(listener);
  }

  public boolean remove(OutboxListener listener) {
    return listeners.remove(listener);
  }

  /** View of the current listeners, for diagnostics. */
  public List<OutboxListener> listeners() {
    return List.copyOf(listeners);
  }

  private <T> void broadcast(Consumer<OutboxListener> invoke, T info) {
    for (OutboxListener l : listeners) {
      try {
        invoke.accept(l);
      } catch (RuntimeException ex) {
        log.warn(
            "listener {} threw on callback with info={}: {}",
            l.getClass().getName(),
            info,
            ex.toString(),
            ex);
      }
    }
  }

  @Override
  public void onEventPublished(EventPublishedInfo info) {
    broadcast(l -> l.onEventPublished(info), info);
  }

  @Override
  public void onEventClaimed(EventClaimedInfo info) {
    broadcast(l -> l.onEventClaimed(info), info);
  }

  @Override
  public void onEventProcessed(EventProcessedInfo info) {
    broadcast(l -> l.onEventProcessed(info), info);
  }

  @Override
  public void onEventRetryScheduled(EventRetryScheduledInfo info) {
    broadcast(l -> l.onEventRetryScheduled(info), info);
  }

  @Override
  public void onEventDisabled(EventDisabledInfo info) {
    broadcast(l -> l.onEventDisabled(info), info);
  }

  @Override
  public void onEventDeleted(EventDeletedInfo info) {
    broadcast(l -> l.onEventDeleted(info), info);
  }

  @Override
  public void onEventSkipped(EventSkippedInfo info) {
    broadcast(l -> l.onEventSkipped(info), info);
  }

  @Override
  public void onHandlerError(HandlerErrorInfo info) {
    broadcast(l -> l.onHandlerError(info), info);
  }

  @Override
  public void onUnknownEventType(UnknownEventTypeInfo info) {
    broadcast(l -> l.onUnknownEventType(info), info);
  }

  @Override
  public void onEventSerializationError(SerializationErrorInfo info) {
    broadcast(l -> l.onEventSerializationError(info), info);
  }

  @Override
  public void onLockAcquisitionFailed(LockAcquisitionInfo info) {
    broadcast(l -> l.onLockAcquisitionFailed(info), info);
  }

  @Override
  public void onLockReleaseFailed(LockReleaseInfo info) {
    broadcast(l -> l.onLockReleaseFailed(info), info);
  }

  @Override
  public void onWorkerRegistered(WorkerRegisteredInfo info) {
    broadcast(l -> l.onWorkerRegistered(info), info);
  }

  @Override
  public void onWorkerGracefulStop(WorkerGracefulStopInfo info) {
    broadcast(l -> l.onWorkerGracefulStop(info), info);
  }

  @Override
  public void onWorkerDeregistered(WorkerDeregisteredInfo info) {
    broadcast(l -> l.onWorkerDeregistered(info), info);
  }

  @Override
  public void onHeartbeatFailed(HeartbeatFailedInfo info) {
    broadcast(l -> l.onHeartbeatFailed(info), info);
  }

  @Override
  public void onOrphansReclaimed(OrphansReclaimedInfo info) {
    broadcast(l -> l.onOrphansReclaimed(info), info);
  }

  @Override
  public void onStuckHandlerReclaimed(StuckHandlerReclaimedInfo info) {
    broadcast(l -> l.onStuckHandlerReclaimed(info), info);
  }

  @Override
  public void onStorageError(StorageErrorInfo info) {
    broadcast(l -> l.onStorageError(info), info);
  }

  @Override
  public void onDispatchRejected(DispatchRejectedInfo info) {
    broadcast(l -> l.onDispatchRejected(info), info);
  }

  @Override
  public void onEngineCrashed(EngineCrashedInfo info) {
    broadcast(l -> l.onEngineCrashed(info), info);
  }
}
