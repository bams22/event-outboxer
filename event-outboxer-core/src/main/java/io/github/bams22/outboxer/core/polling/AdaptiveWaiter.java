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

import java.time.Duration;
import java.util.Objects;

/**
 * Computes the poll interval for a per-type poller, biased toward responsiveness when work is
 * flowing and toward low CPU cost when the store is idle (see ADR-0004).
 *
 * <p>Semantics: when the last poll claimed at least one event, the next poll runs immediately
 * (zero wait); when the last poll returned empty, the wait grows multiplicatively up to the
 * configured {@code max} before it is capped.
 *
 * <p>The class is not thread-safe — each {@code Poller} owns its own instance.
 */
public final class AdaptiveWaiter {

  private final Duration min;
  private final Duration max;
  private final double multiplier;
  private Duration current;

  public AdaptiveWaiter(Duration min, Duration max, double multiplier) {
    this.min = Objects.requireNonNull(min, "min must not be null");
    this.max = Objects.requireNonNull(max, "max must not be null");
    if (min.isNegative() || min.isZero()) {
      throw new IllegalArgumentException("min must be positive, got " + min);
    }
    if (max.compareTo(min) < 0) {
      throw new IllegalArgumentException("max must be >= min, got min=" + min + ", max=" + max);
    }
    if (multiplier <= 1.0) {
      throw new IllegalArgumentException(
          "multiplier must be > 1.0, got " + multiplier);
    }
    this.multiplier = multiplier;
    this.current = min;
  }

  /**
   * Convenience factory with a {@code multiplier} of {@code 1.5}. Matches the default documented
   * in CONFIGURATION.md §polling.
   */
  public static AdaptiveWaiter withDefaults(Duration min, Duration max) {
    return new AdaptiveWaiter(min, max, 1.5);
  }

  /**
   * Report the size of the batch returned by the last claim call. Positive result resets the
   * waiter to the minimum interval; empty result grows it toward the maximum.
   */
  public void record(int batchSize) {
    if (batchSize > 0) {
      current = min;
    } else {
      long nanos = Math.min(max.toNanos(), (long) (current.toNanos() * multiplier));
      current = Duration.ofNanos(nanos);
    }
  }

  /** Duration the poller should sleep before issuing its next claim. */
  public Duration nextWait() {
    return current;
  }

  /** Current {@code min} / {@code max} / {@code multiplier} exposed for diagnostics. */
  public Duration min() {
    return min;
  }

  public Duration max() {
    return max;
  }

  public double multiplier() {
    return multiplier;
  }
}
