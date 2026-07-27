/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.engine;

import io.github.bams22.outboxer.api.observer.EngineCrashedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.WorkerDeregisteredInfo;
import io.github.bams22.outboxer.api.observer.WorkerGracefulStopInfo;
import io.github.bams22.outboxer.api.observer.WorkerRegisteredInfo;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.core.maintenance.MaintenanceScheduler;
import io.github.bams22.outboxer.core.polling.Poller;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Root composition of the event-outboxer engine. Owns and sequences the lifecycle of the
 * publisher, per-type pollers, handler executors and maintenance scheduler.
 *
 * <p>Build instances through {@link OutboxEngineBuilder}; direct construction is public only so
 * the Spring Boot starter (P9) can wire Spring-managed collaborators without going through the
 * plain-Java builder.
 */
public final class OutboxEngine {

  private static final Logger log = LoggerFactory.getLogger(OutboxEngine.class);

  private final WorkerRegistry registry;
  private final WorkerInfo workerInfo;
  private final EventStore store;
  private final Clock clock;
  private final MaintenanceScheduler maintenance;
  private final List<Poller> pollers;
  private final HandlerExecutorManager handlerExecutors;
  private final OutboxEventPublisher publisher;
  private final OutboxListener listener;
  private final Duration shutdownTimeout;

  private volatile State state = State.STOPPED;

  /**
   * Flipped to {@code true} by {@link #markCrashed(String, Throwable)} when the background
   * health check determines that a poller thread has died. Independent of {@link #state} so
   * Spring's {@code SmartLifecycle.isRunning()} (via {@link #isLifecycleActive()}) keeps
   * returning {@code true} and {@code stop()} still runs the full cleanup.
   */
  private volatile boolean crashed = false;

  public OutboxEngine(
      WorkerRegistry registry,
      WorkerInfo workerInfo,
      EventStore store,
      Clock clock,
      OutboxEventPublisher publisher,
      MaintenanceScheduler maintenance,
      List<Poller> pollers,
      HandlerExecutorManager handlerExecutors,
      OutboxListener listener,
      Duration shutdownTimeout) {
    this.registry = Objects.requireNonNull(registry);
    this.workerInfo = Objects.requireNonNull(workerInfo);
    this.store = Objects.requireNonNull(store);
    this.clock = Objects.requireNonNull(clock);
    this.publisher = Objects.requireNonNull(publisher);
    this.maintenance = Objects.requireNonNull(maintenance);
    this.pollers = List.copyOf(pollers);
    this.handlerExecutors = Objects.requireNonNull(handlerExecutors);
    this.listener = Objects.requireNonNull(listener);
    this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout);
  }

  /** Publish port — hand this to application code. Safe to call even before {@link #start()}. */
  public OutboxEventPublisher publisher() {
    return publisher;
  }

  /** WorkerId under which this engine runs. */
  public WorkerId workerId() {
    return workerInfo.id();
  }

  /** Start worker registration, maintenance and pollers in the right order. */
  public synchronized void start() {
    if (state != State.STOPPED) {
      throw new IllegalStateException("engine already started (state=" + state + ")");
    }
    crashed = false;
    try {
      registry.register(workerInfo);
      listener.onWorkerRegistered(new WorkerRegisteredInfo(workerInfo));
      maintenance.start();
      handlerExecutors.start();
      for (Poller p : pollers) {
        p.start();
      }
      state = State.RUNNING;
      log.info("outbox engine started: worker={} pollers={}", workerInfo.id(), pollers.size());
    } catch (RuntimeException ex) {
      // Best-effort rollback so half-started state does not leak into tests / retries.
      try {
        pollers.forEach(Poller::stop);
      } catch (RuntimeException _) {
        // swallow
      }
      try {
        handlerExecutors.drain(Duration.ofSeconds(1));
      } catch (RuntimeException _) {
        // swallow
      }
      try {
        maintenance.stop(Duration.ofSeconds(1));
      } catch (RuntimeException _) {
        // swallow
      }
      try {
        registry.deregister(workerInfo.id());
      } catch (RuntimeException _) {
        // swallow
      }
      throw new IllegalStateException("failed to start engine: " + ex.getMessage(), ex);
    }
  }

  /** Graceful shutdown: stop pollers, drain handlers, stop maintenance, deregister worker. */
  public synchronized void stop() {
    stop(shutdownTimeout);
  }

  /** Graceful shutdown with an explicit timeout for handler drain. */
  public synchronized void stop(Duration timeout) {
    if (state != State.RUNNING) {
      return;
    }
    Objects.requireNonNull(timeout, "timeout must not be null");
    state = State.STOPPING;
    log.info("outbox engine stopping: worker={} timeout={}", workerInfo.id(), timeout);

    for (Poller p : pollers) {
      p.stop();
    }
    long perPollerJoinMillis = Math.max(10, timeout.toMillis() / Math.max(1, pollers.size()));
    for (Poller p : pollers) {
      try {
        p.join(perPollerJoinMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    handlerExecutors.drain(timeout);

    // Any row still PROCESSING under this worker was queued-but-never-started or interrupted by
    // the drain timeout. Once we deregister, no orphan recovery would ever find those rows —
    // release them back to PENDING first (without burning an attempt).
    boolean claimsReleased = false;
    try {
      int released = store.releaseClaimed(workerInfo.id(), clock.now());
      claimsReleased = true;
      if (released > 0) {
        log.info(
            "released {} unfinished event(s) back to PENDING during shutdown of worker {}",
            released,
            workerInfo.id());
      }
    } catch (RuntimeException ex) {
      log.warn(
          "releaseClaimed failed for {} — leftover PROCESSING rows will be reclaimed by peers "
              + "via the graceful_stop flag: {}",
          workerInfo.id(),
          ex.toString());
    }

    try {
      registry.markGracefulStop(workerInfo.id());
      listener.onWorkerGracefulStop(new WorkerGracefulStopInfo(workerInfo.id()));
    } catch (RuntimeException ex) {
      log.warn("markGracefulStop failed for {}: {}", workerInfo.id(), ex.toString());
    }

    maintenance.stop(timeout);

    // Invariant: the worker row must outlive its claims. If releaseClaimed failed, keep the
    // graceful_stop-flagged row so a peer's orphan recovery (which treats graceful_stop as
    // immediately dead) reclaims the leftover events and removes the row afterwards.
    if (claimsReleased) {
      try {
        registry.deregister(workerInfo.id());
        listener.onWorkerDeregistered(new WorkerDeregisteredInfo(workerInfo.id()));
      } catch (RuntimeException ex) {
        log.warn("deregister failed for {}: {}", workerInfo.id(), ex.toString());
      }
    } else {
      log.info(
          "skipping deregister of {} — worker row left flagged graceful_stop for peer recovery",
          workerInfo.id());
    }

    state = State.STOPPED;
    crashed = false;
    log.info("outbox engine stopped: worker={}", workerInfo.id());
  }

  /**
   * Observable engine state. Returns {@code STOPPED} whenever
   * {@link #markCrashed(String, Throwable)} has fired, regardless of the internal lifecycle
   * phase — this is the signal exposed to the Actuator health indicator and the
   * {@code event_outboxer.engine.state} gauge. Use {@link #isLifecycleActive()} for the
   * "has {@code start()} been called and {@code stop()} not yet completed" question.
   */
  public State state() {
    return crashed ? State.STOPPED : state;
  }

  /**
   * Returns {@code true} when the engine has been started and {@code stop()} has not yet
   * completed — ignoring any intervening crash. Used by Spring Boot's
   * {@code SmartLifecycle.isRunning()} so a crashed engine still goes through the normal
   * shutdown path (deregister worker, drain handlers).
   */
  public boolean isLifecycleActive() {
    return state == State.RUNNING || state == State.STOPPING;
  }

  /**
   * Mark the engine as crashed. Called by the background health-check task when it determines
   * that a critical component is no longer alive. Idempotent; firing twice produces exactly one
   * log entry and one {@link OutboxListener#onEngineCrashed} callback.
   *
   * @param reason human-readable description of what the health check detected
   * @param cause the underlying throwable, if known; may be {@code null}
   */
  public void markCrashed(String reason, @Nullable Throwable cause) {
    Objects.requireNonNull(reason, "reason must not be null");
    synchronized (this) {
      if (crashed) {
        return;
      }
      crashed = true;
    }
    log.error(
        "outbox engine CRASHED on worker {}: {}",
        workerInfo.id(),
        reason,
        cause);
    try {
      listener.onEngineCrashed(
          new EngineCrashedInfo(reason, cause, Instant.now(), workerInfo.id()));
    } catch (RuntimeException ex) {
      log.warn("onEngineCrashed listener threw: {}", ex.toString(), ex);
    }
  }

  /** Engine lifecycle state. */
  public enum State {
    STOPPED,
    RUNNING,
    STOPPING
  }
}
