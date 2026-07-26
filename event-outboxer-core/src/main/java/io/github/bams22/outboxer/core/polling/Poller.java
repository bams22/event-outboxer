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
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final AtomicBoolean wakeRequested = new AtomicBoolean();

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
   * Ask the poller to claim as soon as possible instead of sleeping out its current adaptive
   * wait. Called (via {@link PollerWakeHub}) after a local transaction that published an event
   * of this type commits. Safe from any thread; a wake on a stopped poller is a no-op.
   *
   * <p>Race-free by construction: a wake arriving before the park makes the loop skip the park
   * (flag check); a wake arriving during the park unparks it; a wake arriving while a tick is
   * running leaves either the flag or the {@code LockSupport} permit set, so the next park
   * returns immediately.
   */
  public void wake() {
    wakeRequested.set(true);
    Thread t = thread;
    if (running && t != null) {
      LockSupport.unpark(t);
    }
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
        try {
          int dispatched = tick();
          waiter.record(dispatched);
          long waitNanos = waiter.nextWait().toNanos();
          // A wake that arrived during the tick cancels the park entirely; one that arrives
          // mid-park unparks it (and the leftover LockSupport permit covers the in-between).
          if (waitNanos > 0 && !wakeRequested.getAndSet(false)) {
            LockSupport.parkNanos(waitNanos);
            wakeRequested.set(false);
            if (Thread.interrupted()) {
              // stop() may have interrupted us mid-park; re-check running.
            }
          }
        } catch (Error err) {
          // Errors (OOM, StackOverflow, test-injected fakes, unexpected linkage
          // failures) terminate the poller thread deliberately through a clean exit
          // of the loop rather than escaping to the JVM's uncaught-exception
          // handler. Rationale:
          //   1. Production: the engine's EngineHealthCheckTask observes
          //      thread.isAlive()==false and flips state→STOPPED the same way as
          //      before — crash detection still engages.
          //   2. Test infrastructure: JUnit / surefire install handlers that
          //      attribute uncaught thread exceptions to the currently-running
          //      test; letting Errors escape makes crash-detection tests flap
          //      depending on JDK / surefire version.
          //   3. Observability: the Error is logged via SLF4J instead of the JVM's
          //      stderr print, fitting into the application's log pipeline.
          log.error("poller thread died from Error: {}", err.toString(), err);
          break;
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
      // The row is already PROCESSING/claimed but never reached the executor — release it back
      // to PENDING, otherwise it stays invisible to watchdog and orphan recovery for as long as
      // this worker lives.
      dispatcher.releaseRejected(claimed);
      return false;
    }
  }
}
