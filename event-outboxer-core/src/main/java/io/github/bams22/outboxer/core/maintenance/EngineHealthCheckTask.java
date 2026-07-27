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

import io.github.bams22.outboxer.core.polling.Poller;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic task that detects unrecoverable engine-level failures — specifically, poller threads
 * that died from an uncaught {@code Error} after {@code Poller.start()} was called. Reports the
 * first detected crash to the supplied {@link CrashReporter} and returns; on subsequent ticks the
 * engine is already flagged as crashed so {@code report} becomes idempotent.
 *
 * <p>Scope limit in MVP: we check thread aliveness only. "Stuck-but-alive" pollers are covered by
 * the handler-level watchdog; maintenance-scheduler death is not detected here because this task
 * runs inside it (if the scheduler dies, the task dies with it — acceptable for 0.1.0).
 */
public final class EngineHealthCheckTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(EngineHealthCheckTask.class);

  private final List<Poller> pollers;
  private final CrashReporter reporter;

  public EngineHealthCheckTask(List<Poller> pollers, CrashReporter reporter) {
    this.pollers = List.copyOf(Objects.requireNonNull(pollers, "pollers must not be null"));
    this.reporter = Objects.requireNonNull(reporter, "reporter must not be null");
  }

  @Override
  public void run() {
    try {
      for (Poller poller : pollers) {
        if (poller.isCrashed()) {
          reporter.report("poller thread died for eventType=" + poller.eventType(), null);
          return;
        }
      }
    } catch (RuntimeException ex) {
      log.warn("engine health check pass failed: {}", ex.toString(), ex);
    }
  }

  /**
   * Bridge between the task and the engine — lets the maintenance scheduler drive crash reporting
   * without a direct reference to {@code OutboxEngine}, which eliminates the construction-order
   * circular dependency.
   */
  @FunctionalInterface
  public interface CrashReporter {
    void report(String reason, @Nullable Throwable cause);
  }
}
