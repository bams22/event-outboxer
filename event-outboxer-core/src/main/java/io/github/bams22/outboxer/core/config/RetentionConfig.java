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
 * Configuration of the optional retention task (ADR-0019). Both age thresholds default to
 * {@code null} = retention disabled: deleting data is never a surprise default. Enabling either
 * threshold activates the task on the maintenance scheduler.
 *
 * @param archiveOlderThan delete archive rows older than this; {@code null} disables archive
 *     retention
 * @param disabledOlderThan delete {@code DISABLED} events created earlier than this ago;
 *     {@code null} disables. Age is approximated by {@code created_at} — the schema does not
 *     record the moment of disabling
 * @param batchSize rows deleted per statement; a pass loops until a batch comes back short
 * @param interval delay between retention passes
 */
@Builder
public record RetentionConfig(
    @Nullable Duration archiveOlderThan,
    @Nullable Duration disabledOlderThan,
    int batchSize,
    Duration interval) {

  public RetentionConfig {
    Objects.requireNonNull(interval, "interval must not be null");
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive, got " + batchSize);
    }
    if (interval.isNegative() || interval.isZero()) {
      throw new IllegalArgumentException("interval must be positive, got " + interval);
    }
    if (archiveOlderThan != null && (archiveOlderThan.isNegative() || archiveOlderThan.isZero())) {
      throw new IllegalArgumentException(
          "archiveOlderThan must be positive, got " + archiveOlderThan);
    }
    if (disabledOlderThan != null
        && (disabledOlderThan.isNegative() || disabledOlderThan.isZero())) {
      throw new IllegalArgumentException(
          "disabledOlderThan must be positive, got " + disabledOlderThan);
    }
  }

  /** {@code true} when at least one retention dimension is switched on. */
  public boolean enabled() {
    return archiveOlderThan != null || disabledOlderThan != null;
  }

  /** Retention fully off — the library default. */
  public static RetentionConfig disabled() {
    return RetentionConfig.builder()
        .archiveOlderThan(null)
        .disabledOlderThan(null)
        .batchSize(1000)
        .interval(Duration.ofHours(1))
        .build();
  }
}
