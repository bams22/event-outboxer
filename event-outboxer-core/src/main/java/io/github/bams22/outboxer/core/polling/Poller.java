/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.polling;

import io.github.bams22.outboxer.api.observer.DispatchRejectedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.StorageErrorInfo;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.dispatch.HandlerDispatcher;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.exception.StorageException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-event-type polling loop. One {@code Poller} is created for each registered {@code
 * EventHandler}; it runs on a dedicated platform thread and claims events for the configured
 * type, dispatching each one to the handler executor.
 *
 * <p>Lifecycle: {@link #start()} spawns the thread; {@link #stop()} flips the running flag and
 * interrupts the thread so an in-progress {@code Thread.sleep} returns immediately. Waiting for
 * in-flight handlers to drain is done separately by the caller (see the engine's
 * {@code shutdownTimeout}).
 */
public final class Poller {

  private static final Logger log = LoggerFactory.getLogger(Poller.class);

  private final String eventType;
  private final WorkerId workerId;
  private final PollStrategy strategy;
  private final HandlerDispatcher dispatcher;
  private final Executor handlerExecutor;
  private final OutboxListener listener;
  private final EventTypeConfig config;
  private final AdaptiveWaiter waiter;

  private volatile boolean running;
  private @org.jspecify.annotations.Nullable Thread thread;

  public Poller(
      String eventType,
      WorkerId workerId,
      PollStrategy strategy,
      HandlerDispatcher dispatcher,
      Executor handlerExecutor,
      OutboxListener listener,
      EventTypeConfig config) {
    this.eventType = Objects.requireNonNull(eventType, "eventType must not be null");
    this.workerId = Objects.requireNonNull(workerId, "workerId must not be null");
    this.strategy = Objects.requireNonNull(strategy, "strategy must not be null");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
    this.handlerExecutor = Objects.requireNonNull(handlerExecutor, "handlerExecutor must not be null");
    this.listener = Objects.requireNonNull(listener, "listener must not be null");
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.waiter =
        new AdaptiveWaiter(
            config.pollMinInterval(), config.pollMaxInterval(), config.pollMultiplier());
  }

  /** Spawn the polling thread. Must be called exactly once. */
  public synchronized void start() {
    if (running) {
      throw new IllegalStateException("poller for '" + eventType + "' already started");
    }
    running = true;
    thread = new Thread(this::loop, "outbox-poller-" + eventType);
    thread.setDaemon(true);
    thread.start();
  }

  /** Request shutdown. Returns immediately — the caller is expected to {@code join()} if needed. */
  public synchronized void stop() {
    if (!running) {
      return;
    }
    running = false;
    Thread t = thread;
    if (t != null) {
      t.interrupt();
    }
  }

  /** Join the polling thread for at most {@code timeoutMillis} milliseconds. */
  public void join(long timeoutMillis) throws InterruptedException {
    Thread t = thread;
    if (t != null) {
      t.join(timeoutMillis);
    }
  }

  /** Event type this poller serves. */
  public String eventType() {
    return eventType;
  }

  /**
   * Returns {@code true} if this poller is supposed to be running but its backing thread has
   * died. Checked periodically by the engine health-check task to detect unrecoverable poller
   * crashes (uncaught {@code Error} from the strategy, JVM thread kill, etc.). Returns
   * {@code false} before {@link #start()} and after {@link #stop()} — only a live-should-be-alive
   * thread that isn't counts as crashed.
   */
  public boolean isCrashed() {
    if (!running) {
      return false;
    }
    Thread t = thread;
    return t != null && !t.isAlive();
  }

  private void loop() {
    log.debug("poller start: eventType={}, worker={}", eventType, workerId);
    try {
      while (running) {
        int dispatched = tick();
        waiter.record(dispatched);
        long waitNanos = waiter.nextWait().toNanos();
        if (waitNanos > 0) {
          LockSupport.parkNanos(waitNanos);
          if (Thread.interrupted()) {
            // stop() may have interrupted us mid-park; re-check running.
          }
        }
      }
    } finally {
      log.debug("poller stopped: eventType={}", eventType);
    }
  }

  private int tick() {
    List<ClaimedEvent> batch;
    try {
      batch = strategy.pollOnce(eventType, workerId, config.claimBatchSize());
    } catch (StorageException ex) {
      listener.onStorageError(new StorageErrorInfo("claim[" + eventType + "]", ex));
      log.warn("claim failed for type {}: {}", eventType, ex.toString());
      return 0;
    } catch (RuntimeException ex) {
      log.warn("unexpected claim error for type {}: {}", eventType, ex.toString(), ex);
      return 0;
    }
    if (batch.isEmpty()) {
      return 0;
    }
    int dispatched = 0;
    for (ClaimedEvent claimed : batch) {
      if (submit(claimed)) {
        dispatched++;
      }
    }
    return dispatched;
  }

  private boolean submit(ClaimedEvent claimed) {
    try {
      handlerExecutor.execute(() -> dispatcher.dispatch(claimed));
      return true;
    } catch (RejectedExecutionException ex) {
      listener.onDispatchRejected(
          new DispatchRejectedInfo(claimed.id(), claimed.eventType(), ex));
      log.debug(
          "handler executor rejected dispatch for eventId={} type={}: {}",
          claimed.id(),
          claimed.eventType(),
          ex.toString());
      return false;
    }
  }
}
