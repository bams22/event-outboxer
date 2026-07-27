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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Computes the poll interval for a per-type poller, biased toward responsiveness when work is
 * flowing and toward low CPU cost when the store is idle (see ADR-0004).
 *
 * <p>Semantics: when the last poll claimed at least one event, the next poll runs immediately (zero
 * wait); when the last poll returned empty, the wait grows multiplicatively up to the configured
 * {@code max} before it is capped.
 *
 * <p>Every emitted wait carries a uniform ±10% jitter. A fleet of JVMs deployed together would
 * otherwise back off in lockstep and hit the store with synchronized claim bursts (the same
 * thundering-herd argument as the jitter in {@code ExponentialBackoffFailureHandler}); the jitter
 * desynchronizes the pollers within a few idle cycles. The backoff <em>state</em> stays
 * deterministic — jitter is applied on emission, not accumulated.
 *
 * <p>The class is not thread-safe — each {@code Poller} owns its own instance.
 */
public final class AdaptiveWaiter {

  /** Relative jitter applied to every emitted wait: {@code current × (1 ± JITTER)}. */
  static final double JITTER = 0.1;

  private final Duration min;
  private final Duration max;
  private final double multiplier;
  private final DoubleSupplier random;
  private Duration current;

  public AdaptiveWaiter(Duration min, Duration max, double multiplier) {
    this(min, max, multiplier, () -> ThreadLocalRandom.current().nextDouble());
  }

  /**
   * Test-visible constructor with an injectable randomness source producing values in {@code [0,
   * 1)}; {@code 0.5} yields exactly the unjittered wait.
   */
  AdaptiveWaiter(Duration min, Duration max, double multiplier, DoubleSupplier random) {
    this.min = Objects.requireNonNull(min, "min must not be null");
    this.max = Objects.requireNonNull(max, "max must not be null");
    this.random = Objects.requireNonNull(random, "random must not be null");
    if (min.isNegative() || min.isZero()) {
      throw new IllegalArgumentException("min must be positive, got " + min);
    }
    if (max.compareTo(min) < 0) {
      throw new IllegalArgumentException("max must be >= min, got min=" + min + ", max=" + max);
    }
    if (multiplier <= 1.0) {
      throw new IllegalArgumentException("multiplier must be > 1.0, got " + multiplier);
    }
    this.multiplier = multiplier;
    this.current = min;
  }

  /**
   * Convenience factory with a {@code multiplier} of {@code 1.5}. Matches the default documented in
   * CONFIGURATION.md §polling.
   */
  public static AdaptiveWaiter withDefaults(Duration min, Duration max) {
    return new AdaptiveWaiter(min, max, 1.5);
  }

  /**
   * Report the size of the batch returned by the last claim call. Positive result resets the waiter
   * to the minimum interval; empty result grows it toward the maximum.
   */
  public void record(int batchSize) {
    if (batchSize > 0) {
      current = min;
    } else {
      long nanos = Math.min(max.toNanos(), (long) (current.toNanos() * multiplier));
      current = Duration.ofNanos(nanos);
    }
  }

  /**
   * Duration the poller should sleep before issuing its next claim: the current backoff value with
   * ±10% uniform jitter applied.
   */
  public Duration nextWait() {
    double factor = 1.0 + JITTER * (2.0 * random.getAsDouble() - 1.0);
    return Duration.ofNanos((long) (current.toNanos() * factor));
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
