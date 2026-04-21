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

import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.WorkerDeregisteredInfo;
import io.github.bams22.outboxer.api.observer.WorkerGracefulStopInfo;
import io.github.bams22.outboxer.api.observer.WorkerRegisteredInfo;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.core.maintenance.MaintenanceScheduler;
import io.github.bams22.outboxer.core.polling.Poller;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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
  private final MaintenanceScheduler maintenance;
  private final List<Poller> pollers;
  private final Map<String, ExecutorService> handlerExecutors;
  private final OutboxEventPublisher publisher;
  private final OutboxListener listener;
  private final Duration shutdownTimeout;

  private volatile State state = State.STOPPED;

  public OutboxEngine(
      WorkerRegistry registry,
      WorkerInfo workerInfo,
      OutboxEventPublisher publisher,
      MaintenanceScheduler maintenance,
      List<Poller> pollers,
      Map<String, ExecutorService> handlerExecutors,
      OutboxListener listener,
      Duration shutdownTimeout) {
    this.registry = Objects.requireNonNull(registry);
    this.workerInfo = Objects.requireNonNull(workerInfo);
    this.publisher = Objects.requireNonNull(publisher);
    this.maintenance = Objects.requireNonNull(maintenance);
    this.pollers = List.copyOf(pollers);
    this.handlerExecutors = Map.copyOf(handlerExecutors);
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
    try {
      registry.register(workerInfo);
      listener.onWorkerRegistered(new WorkerRegisteredInfo(workerInfo));
      maintenance.start();
      for (Poller p : pollers) {
        p.start();
      }
      state = State.RUNNING;
      log.info("outbox engine started: worker={} pollers={}", workerInfo.id(), pollers.size());
    } catch (RuntimeException ex) {
      // Best-effort rollback so half-started state does not leak into tests / retries.
      try {
        pollers.forEach(Poller::stop);
      } catch (RuntimeException ignored) {
        // swallow
      }
      try {
        maintenance.stop(Duration.ofSeconds(1));
      } catch (RuntimeException ignored) {
        // swallow
      }
      try {
        registry.deregister(workerInfo.id());
      } catch (RuntimeException ignored) {
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
    state = State.STOPPING;
    Objects.requireNonNull(timeout, "timeout must not be null");
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

    drainHandlers(timeout);

    try {
      registry.markGracefulStop(workerInfo.id());
      listener.onWorkerGracefulStop(new WorkerGracefulStopInfo(workerInfo.id()));
    } catch (RuntimeException ex) {
      log.warn("markGracefulStop failed for {}: {}", workerInfo.id(), ex.toString());
    }

    maintenance.stop(timeout);

    try {
      registry.deregister(workerInfo.id());
      listener.onWorkerDeregistered(new WorkerDeregisteredInfo(workerInfo.id()));
    } catch (RuntimeException ex) {
      log.warn("deregister failed for {}: {}", workerInfo.id(), ex.toString());
    }

    state = State.STOPPED;
    log.info("outbox engine stopped: worker={}", workerInfo.id());
  }

  /** Current lifecycle state. */
  public State state() {
    return state;
  }

  private void drainHandlers(Duration timeout) {
    for (ExecutorService exec : handlerExecutors.values()) {
      exec.shutdown();
    }
    long deadline = System.nanoTime() + timeout.toNanos();
    for (ExecutorService exec : handlerExecutors.values()) {
      long remaining = Math.max(0, deadline - System.nanoTime());
      try {
        if (!exec.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
          exec.shutdownNow();
        }
      } catch (InterruptedException e) {
        exec.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Engine lifecycle state. */
  public enum State {
    STOPPED,
    RUNNING,
    STOPPING
  }
}
