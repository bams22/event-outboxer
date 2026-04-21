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
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic task that writes a fresh heartbeat for the current {@link WorkerId} in the {@code
 * outbox.workers} table. One instance per running engine; scheduled by {@link
 * MaintenanceScheduler} at the cadence defined in {@code MaintenanceConfig.heartbeatInterval()}.
 */
public final class HeartbeatTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(HeartbeatTask.class);

  private final WorkerRegistry registry;
  private final WorkerId workerId;
  private final Clock clock;
  private final OutboxListener listener;

  public HeartbeatTask(
      WorkerRegistry registry, WorkerId workerId, Clock clock, OutboxListener listener) {
    this.registry = Objects.requireNonNull(registry);
    this.workerId = Objects.requireNonNull(workerId);
    this.clock = Objects.requireNonNull(clock);
    this.listener = Objects.requireNonNull(listener);
  }

  @Override
  public void run() {
    try {
      boolean updated = registry.heartbeat(workerId, clock.now());
      if (!updated) {
        log.warn(
            "heartbeat row not found for worker {} — was it reaped by peer orphan recovery?",
            workerId);
      }
    } catch (RuntimeException ex) {
      listener.onHeartbeatFailed(new HeartbeatFailedInfo(workerId, ex));
      log.warn("heartbeat failed for worker {}: {}", workerId, ex.toString());
    }
  }
}
