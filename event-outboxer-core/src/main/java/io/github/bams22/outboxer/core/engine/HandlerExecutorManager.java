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

import io.github.bams22.outboxer.core.config.EventTypeConfig;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * Owns the per-event-type handler executors on behalf of {@link OutboxEngine}. Executors are
 * created on {@link #start()} and drained on {@link #drain(Duration)} so that the engine can be
 * stopped and started again — a fresh {@code start()} installs fresh pools instead of reusing
 * the shut-down ones (a terminated {@code ExecutorService} rejects every submission, which
 * previously made a restarted engine claim batches it could never dispatch).
 *
 * <p>Pollers hold a stable per-type {@link Executor} facade (see {@link #executorFor(String)})
 * that delegates to whatever pool is currently installed; while the engine is stopped the facade
 * throws {@link RejectedExecutionException}, which the poller already treats as backpressure.
 */
public final class HandlerExecutorManager {

  private final Function<EventTypeConfig, ExecutorService> factory;
  private final Map<String, EventTypeConfig> configs;
  private final Map<String, Slot> slots;

  public HandlerExecutorManager(
      Function<EventTypeConfig, ExecutorService> factory, Map<String, EventTypeConfig> configs) {
    this.factory = Objects.requireNonNull(factory, "factory must not be null");
    this.configs = Map.copyOf(Objects.requireNonNull(configs, "configs must not be null"));
    Map<String, Slot> s = new LinkedHashMap<>();
    for (String type : configs.keySet()) {
      s.put(type, new Slot(type));
    }
    this.slots = s;
  }

  /**
   * Stable executor facade for the given event type. Safe to hand to a poller before
   * {@link #start()} has run.
   */
  public Executor executorFor(String eventType) {
    Slot slot = slots.get(eventType);
    if (slot == null) {
      throw new IllegalArgumentException("no handler executor configured for type " + eventType);
    }
    return slot;
  }

  /**
   * Create and install a fresh {@code ExecutorService} per event type.
   */
  public synchronized void start() {
    for (Map.Entry<String, Slot> e : slots.entrySet()) {
      e.getValue().install(factory.apply(configs.get(e.getKey())));
    }
  }

  /**
   * Drain all executors: reject new work, wait up to {@code timeout} for in-flight handlers,
   * then force-shutdown whatever is left. After this call the facades reject submissions until
   * the next {@link #start()}.
   */
  public synchronized void drain(Duration timeout) {
    for (Slot slot : slots.values()) {
      ExecutorService exec = slot.uninstall();
      if (exec != null) {
        exec.shutdown();
      }
    }
    long deadline = System.nanoTime() + timeout.toNanos();
    for (Slot slot : slots.values()) {
      ExecutorService exec = slot.drained;
      if (exec == null) {
        continue;
      }
      long remaining = Math.max(0, deadline - System.nanoTime());
      try {
        if (!exec.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
          exec.shutdownNow();
        }
      } catch (InterruptedException e) {
        exec.shutdownNow();
        Thread.currentThread().interrupt();
      }
      slot.drained = null;
    }
  }

  /**
   * Mutable holder pollers submit through. {@code current} is the live pool (or {@code null}
   * while the engine is stopped); {@code drained} keeps the shut-down pool referenced between
   * the reject-new-work and await-termination phases of {@link #drain(Duration)}.
   */
  private static final class Slot implements Executor {

    private final String eventType;
    private volatile @Nullable ExecutorService current;
    private @Nullable ExecutorService drained;

    private Slot(String eventType) {
      this.eventType = eventType;
    }

    private void install(ExecutorService exec) {
      current = exec;
    }

    private @Nullable ExecutorService uninstall() {
      ExecutorService exec = current;
      current = null;
      drained = exec;
      return exec;
    }

    @Override
    public void execute(Runnable command) {
      ExecutorService exec = current;
      if (exec == null) {
        throw new RejectedExecutionException(
            "handler executor for type " + eventType + " is not running");
      }
      exec.execute(command);
    }
  }
}
