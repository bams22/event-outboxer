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

import io.github.bams22.outboxer.core.dispatch.HandlerDispatcher;
import io.github.bams22.outboxer.core.maintenance.HeartbeatTask;
import io.github.bams22.outboxer.core.maintenance.OrphanRecoveryTask;
import io.github.bams22.outboxer.core.maintenance.WatchdogTask;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventStore;
import java.util.List;
import java.util.Objects;

/**
 * Synchronous, step-through driver for the outbox engine. Unlike {@code OutboxEngine}, no poller
 * thread or maintenance scheduler runs in the background — the test explicitly advances time via
 * {@link SettableClock} and invokes {@link #tick()} to claim + dispatch a single round of events,
 * or the individual maintenance methods to exercise heartbeat / orphan recovery / watchdog in
 * isolation.
 *
 * <p>Construct via {@link OutboxTestContext}.
 */
public final class ManualEngine {

  private final EventStore store;
  private final WorkerId workerId;
  private final HandlerDispatcher dispatcher;
  private final HeartbeatTask heartbeat;
  private final OrphanRecoveryTask orphanRecovery;
  private final WatchdogTask watchdog;
  private final List<String> eventTypes;
  private final int defaultBatchSize;

  ManualEngine(
      EventStore store,
      WorkerId workerId,
      HandlerDispatcher dispatcher,
      HeartbeatTask heartbeat,
      OrphanRecoveryTask orphanRecovery,
      WatchdogTask watchdog,
      List<String> eventTypes,
      int defaultBatchSize) {
    this.store = Objects.requireNonNull(store);
    this.workerId = Objects.requireNonNull(workerId);
    this.dispatcher = Objects.requireNonNull(dispatcher);
    this.heartbeat = Objects.requireNonNull(heartbeat);
    this.orphanRecovery = Objects.requireNonNull(orphanRecovery);
    this.watchdog = Objects.requireNonNull(watchdog);
    this.eventTypes = List.copyOf(eventTypes);
    if (defaultBatchSize <= 0) {
      throw new IllegalArgumentException(
          "defaultBatchSize must be positive, got " + defaultBatchSize);
    }
    this.defaultBatchSize = defaultBatchSize;
  }

  /**
   * Claim + dispatch every currently-eligible event for every registered type, on the calling
   * thread. Returns the total number of events dispatched.
   */
  public int tick() {
    int total = 0;
    for (String type : eventTypes) {
      total += tick(type, defaultBatchSize);
    }
    return total;
  }

  /** Claim + dispatch up to {@code batchSize} events of {@code eventType}. */
  public int tick(String eventType, int batchSize) {
    Objects.requireNonNull(eventType, "eventType must not be null");
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive, got " + batchSize);
    }
    List<ClaimedEvent> claimed = store.claim(new ClaimRequest(eventType, workerId, batchSize));
    for (ClaimedEvent c : claimed) {
      dispatcher.dispatch(c);
    }
    return claimed.size();
  }

  /** Run one heartbeat cycle. */
  public void tickHeartbeat() {
    heartbeat.run();
  }

  /** Run one orphan-recovery cycle. */
  public void tickOrphanRecovery() {
    orphanRecovery.run();
  }

  /** Run one watchdog cycle. */
  public void tickWatchdog() {
    watchdog.run();
  }

  /** Worker id the engine runs under. */
  public WorkerId workerId() {
    return workerId;
  }

  /** Every event type with a registered handler. */
  public List<String> eventTypes() {
    return eventTypes;
  }
}
