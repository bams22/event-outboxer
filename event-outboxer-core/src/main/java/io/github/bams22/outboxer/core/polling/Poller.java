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
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-event-type polling loop. One {@code Poller} is created for each registered {@code
 * EventHandler}; it runs on a dedicated platform thread and claims events for the configured type,
 * dispatching each one to the handler executor.
 *
 * <p>Lifecycle: {@link #start()} spawns the thread; {@link #stop()} flips the running flag and
 * interrupts the thread so an in-progress {@code Thread.sleep} returns immediately. Waiting for
 * in-flight handlers to drain is done separately by the caller (see the engine's {@code
 * shutdownTimeout}).
 */
public final class Poller {

  private static final Logger log = LoggerFactory.getLogger(Poller.class);

  private final String eventType;
  private final WorkerId workerId;
  private final PollStrategy strategy;
  private final HandlerDispatcher dispatcher;
  private final HandlerExecutorGate handlerExecutor;
  private final OutboxListener listener;
  private final EventTypeConfig config;
  private final AdaptiveWaiter waiter;

  private volatile boolean running;
  private @Nullable Thread thread;
  private final AtomicBoolean wakeRequested = new AtomicBoolean();

  public Poller(
      String eventType,
      WorkerId workerId,
      PollStrategy strategy,
      HandlerDispatcher dispatcher,
      HandlerExecutorGate handlerExecutor,
      OutboxListener listener,
      EventTypeConfig config) {
    this.eventType = Objects.requireNonNull(eventType, "eventType must not be null");
    this.workerId = Objects.requireNonNull(workerId, "workerId must not be null");
    this.strategy = Objects.requireNonNull(strategy, "strategy must not be null");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
    this.handlerExecutor =
        Objects.requireNonNull(handlerExecutor, "handlerExecutor must not be null");
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
    thread = Thread.ofPlatform().name("outbox-poller-" + eventType).daemon().unstarted(this::loop);
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
   * Ask the poller to claim as soon as possible instead of sleeping out its current adaptive wait.
   * Called (via {@link PollerWakeHub}) after a local transaction that published an event of this
   * type commits. Safe from any thread; a wake on a stopped poller is a no-op.
   *
   * <p>Race-free by construction: a wake arriving before the park makes the loop skip the park
   * (flag check); a wake arriving during the park unparks it; a wake arriving while a tick is
   * running leaves either the flag or the {@code LockSupport} permit set, so the next park returns
   * immediately.
   */
  public void wake() {
    wakeRequested.set(true);
    Thread t = thread;
    if (running && t != null) {
      LockSupport.unpark(t);
    }
  }

  /**
   * Returns {@code true} if this poller is supposed to be running but its backing thread has died.
   * Checked periodically by the engine health-check task to detect unrecoverable poller crashes
   * (uncaught {@code Error} from the strategy, JVM thread kill, etc.). Returns {@code false} before
   * {@link #start()} and after {@link #stop()} — only a live-should-be-alive thread that isn't
   * counts as crashed.
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
          int free = handlerExecutor.freeCapacity();
          if (free <= 0) {
            // Executor saturated: claiming now would only produce rejected dispatches and
            // claim/release churn. Park with a bounded fallback — the gate's
            // capacity-available callback wakes us the moment a slot frees.
            parkUnlessWoken(config.pollMinInterval());
            continue;
          }
          int requested = Math.min(config.claimBatchSize(), free);
          int claimed = tick(requested);
          waiter.record(claimed);
          if (claimed == requested && claimed > 0) {
            // Full batch: the store almost certainly has more ready rows — re-poll
            // immediately. Bounded by free capacity, so this cannot outrun the handlers.
            continue;
          }
          parkUnlessWoken(waiter.nextWait());
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

  /**
   * Park for at most {@code wait} unless a wake arrived. A wake that arrived before the park
   * cancels it entirely (flag check); one that arrives mid-park unparks it; the leftover {@code
   * LockSupport} permit covers the in-between race.
   */
  private void parkUnlessWoken(Duration wait) {
    long waitNanos = wait.toNanos();
    if (waitNanos > 0 && !wakeRequested.getAndSet(false)) {
      LockSupport.parkNanos(waitNanos);
      wakeRequested.set(false);
      if (Thread.interrupted()) {
        // stop() may have interrupted us mid-park; the loop re-checks running.
      }
    }
  }

  private int tick(int limit) {
    List<ClaimedEvent> batch;
    try {
      batch = strategy.pollOnce(eventType, workerId, limit);
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
    for (ClaimedEvent claimed : batch) {
      submit(claimed);
    }
    // Claimed count, not dispatched: a rejected dispatch (rare capacity race, already released
    // back to PENDING by submit) still means the store had a full batch of ready rows.
    return batch.size();
  }

  private boolean submit(ClaimedEvent claimed) {
    try {
      handlerExecutor.execute(() -> dispatcher.dispatch(claimed));
      return true;
    } catch (RejectedExecutionException ex) {
      listener.onDispatchRejected(new DispatchRejectedInfo(claimed.id(), claimed.eventType(), ex));
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
