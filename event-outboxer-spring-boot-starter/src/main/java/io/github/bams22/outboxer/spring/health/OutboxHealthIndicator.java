/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.health;

import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Health indicator exposed under {@code /actuator/health/outbox}. Reports:
 *
 * <ul>
 *   <li>{@code UP} when the engine is RUNNING and the store's metrics snapshot is reachable;
 *   <li>{@code DOWN} when the engine is STOPPED or metrics cannot be read;
 *   <li>metric totals (pending / processing / disabled) as extra details for quick triage.
 * </ul>
 */
public final class OutboxHealthIndicator implements HealthIndicator {

  private final OutboxEngine engine;
  private final EventStore store;

  public OutboxHealthIndicator(OutboxEngine engine, EventStore store) {
    this.engine = engine;
    this.store = store;
  }

  @Override
  public Health health() {
    Health.Builder builder;
    if (engine.state() != OutboxEngine.State.RUNNING) {
      builder = Health.down().withDetail("state", engine.state().name());
    } else {
      builder = Health.up().withDetail("state", engine.state().name());
    }
    try {
      OutboxMetricsSnapshot snapshot = store.metricsSnapshot();
      builder
          .withDetail("totalPending", snapshot.totalPending())
          .withDetail("totalProcessing", snapshot.totalProcessing())
          .withDetail("totalDisabled", snapshot.totalDisabled())
          .withDetail("takenAt", snapshot.takenAt().toString());
    } catch (RuntimeException ex) {
      // ex.toString(), not ex.getMessage(): withDetail rejects null values, and getMessage() can
      // legitimately be null (e.g. bare NullPointerException).
      builder.down().withDetail("metricsError", ex.toString());
    }
    builder.withDetail("workerId", engine.workerId().value());
    return builder.build();
  }
}
