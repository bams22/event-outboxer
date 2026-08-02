/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.config;

import java.time.Duration;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Engine-wide maintenance cadence: heartbeat, orphan recovery, watchdog, and graceful-shutdown
 * budget. See ADR-0005 / ADR-0014 / ARCHITECTURE.md §maintenance for the rationale.
 *
 * <p>Invariant: {@code deadThreshold >= 3 × heartbeatInterval}. The constructor enforces it so a
 * misconfiguration cannot silently start reclaiming live workers.
 *
 * @param heartbeatInterval cadence of {@code WorkerRegistry.heartbeat(...)} writes
 * @param deadThreshold how long a worker may be silent before peers treat it as dead; must be
 *     {@code >= 3 × heartbeatInterval}
 * @param orphanRecoveryInterval cadence of the orphan-recovery task that reclaims events of dead
 *     workers
 * @param watchdogInterval cadence of the watchdog that scans the in-flight registry for stuck
 *     handlers
 * @param reclaimBatchSize upper bound on dead workers processed per orphan-recovery pass
 * @param shutdownTimeout budget for in-flight handlers to complete during graceful stop
 * @param staleClaimThreshold age of a {@code PROCESSING} claim before the stale-claim sweeper
 *     returns it to {@code PENDING}; {@code null} (default) derives {@code 2 × max
 *     handlerMaxRuntime} across the registered types at engine build time. An explicit value must
 *     exceed every per-type {@code handlerMaxRuntime} — validated by the engine builder
 * @param staleClaimSweepInterval cadence of the stale-claim sweeper
 */
@Builder
public record MaintenanceConfig(
    Duration heartbeatInterval,
    Duration deadThreshold,
    Duration orphanRecoveryInterval,
    Duration watchdogInterval,
    int reclaimBatchSize,
    Duration shutdownTimeout,
    @Nullable Duration staleClaimThreshold,
    Duration staleClaimSweepInterval) {

  public MaintenanceConfig {
    Objects.requireNonNull(heartbeatInterval, "heartbeatInterval must not be null");
    Objects.requireNonNull(deadThreshold, "deadThreshold must not be null");
    Objects.requireNonNull(orphanRecoveryInterval, "orphanRecoveryInterval must not be null");
    Objects.requireNonNull(watchdogInterval, "watchdogInterval must not be null");
    Objects.requireNonNull(shutdownTimeout, "shutdownTimeout must not be null");
    if (heartbeatInterval.isNegative() || heartbeatInterval.isZero()) {
      throw new IllegalArgumentException(
          "heartbeatInterval must be positive, got " + heartbeatInterval);
    }
    if (deadThreshold.compareTo(heartbeatInterval.multipliedBy(3)) < 0) {
      throw new IllegalArgumentException(
          "deadThreshold must be >= 3 * heartbeatInterval, got heartbeat="
              + heartbeatInterval
              + ", dead="
              + deadThreshold);
    }
    if (orphanRecoveryInterval.isNegative() || orphanRecoveryInterval.isZero()) {
      throw new IllegalArgumentException(
          "orphanRecoveryInterval must be positive, got " + orphanRecoveryInterval);
    }
    if (watchdogInterval.isNegative() || watchdogInterval.isZero()) {
      throw new IllegalArgumentException(
          "watchdogInterval must be positive, got " + watchdogInterval);
    }
    if (reclaimBatchSize <= 0) {
      throw new IllegalArgumentException(
          "reclaimBatchSize must be positive, got " + reclaimBatchSize);
    }
    if (shutdownTimeout.isNegative()) {
      throw new IllegalArgumentException(
          "shutdownTimeout must not be negative, got " + shutdownTimeout);
    }
    if (staleClaimThreshold != null
        && (staleClaimThreshold.isNegative() || staleClaimThreshold.isZero())) {
      throw new IllegalArgumentException(
          "staleClaimThreshold must be positive, got " + staleClaimThreshold);
    }
    Objects.requireNonNull(staleClaimSweepInterval, "staleClaimSweepInterval must not be null");
    if (staleClaimSweepInterval.isNegative() || staleClaimSweepInterval.isZero()) {
      throw new IllegalArgumentException(
          "staleClaimSweepInterval must be positive, got " + staleClaimSweepInterval);
    }
  }

  /** Default cadence aligned with CONFIGURATION.md §maintenance. */
  public static MaintenanceConfig defaults() {
    return MaintenanceConfig.builder()
        .heartbeatInterval(Duration.ofSeconds(5))
        .deadThreshold(Duration.ofSeconds(30))
        .orphanRecoveryInterval(Duration.ofSeconds(30))
        .watchdogInterval(Duration.ofSeconds(10))
        .reclaimBatchSize(50)
        .shutdownTimeout(Duration.ofSeconds(30))
        .staleClaimSweepInterval(Duration.ofMinutes(5))
        .build();
  }
}
