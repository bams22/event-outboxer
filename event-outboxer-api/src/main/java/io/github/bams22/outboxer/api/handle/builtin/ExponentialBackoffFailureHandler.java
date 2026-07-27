/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.handle.builtin;

import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.FailureContext;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Leaf handler that re-schedules using exponential backoff with jitter and an upper bound.
 *
 * <p>Formula: {@code delay = min(base * multiplier^(attempt-1), cap)}. The final value is adjusted
 * by random jitter in {@code [-jitter, +jitter]} of the computed delay ({@code jitter=0.2} means
 * ±20%). Jitter is critical for "thundering herd" protection when many events fail simultaneously
 * because of a shared downstream outage.
 *
 * <p>A {@code delayOverride} supplied by the handler through {@code EventOutcome.Retry} takes
 * precedence over the computed backoff.
 *
 * @param <T> payload type
 */
public final class ExponentialBackoffFailureHandler<T> implements FailureHandler<T> {

  private final Duration base;
  private final double multiplier;
  private final Duration cap;
  private final double jitter;

  /**
   * @param base delay for the first retry (must be positive)
   * @param multiplier growth factor applied per attempt (must be &gt; 1.0)
   * @param cap upper bound for the backoff (must be &gt;= base)
   * @param jitter random jitter fraction in {@code [0, 1]} (0 = deterministic, 0.2 = ±20%)
   */
  public ExponentialBackoffFailureHandler(
      Duration base, double multiplier, Duration cap, double jitter) {
    Objects.requireNonNull(base, "base must not be null");
    Objects.requireNonNull(cap, "cap must not be null");
    if (base.isNegative() || base.isZero()) {
      throw new IllegalArgumentException("base must be positive, got " + base);
    }
    if (!(multiplier > 1.0)) {
      throw new IllegalArgumentException("multiplier must be > 1.0, got " + multiplier);
    }
    if (cap.compareTo(base) < 0) {
      throw new IllegalArgumentException("cap must be >= base, got cap=" + cap + " base=" + base);
    }
    if (jitter < 0.0 || jitter > 1.0) {
      throw new IllegalArgumentException("jitter must be in [0, 1], got " + jitter);
    }
    this.base = base;
    this.multiplier = multiplier;
    this.cap = cap;
    this.jitter = jitter;
  }

  public Duration base() {
    return base;
  }

  public double multiplier() {
    return multiplier;
  }

  public Duration cap() {
    return cap;
  }

  public double jitter() {
    return jitter;
  }

  @Override
  public FailureDecision onFailure(FailureContext<T> ctx) {
    Duration effective;
    if (ctx.outcome() instanceof EventOutcome.Retry r && r.delayOverride() != null) {
      effective = r.delayOverride();
    } else {
      effective = computeBackoff(ctx.attempt());
    }
    return new FailureDecision.RetryAt(ctx.now().plus(effective), FailureUtil.reason(ctx));
  }

  private Duration computeBackoff(int attempt) {
    double factor = Math.pow(multiplier, Math.max(0, attempt - 1));
    long baseMillis = base.toMillis();
    long capMillis = cap.toMillis();
    long delayMillis = (long) Math.min(baseMillis * factor, (double) capMillis);
    if (jitter > 0.0) {
      double spread = delayMillis * jitter;
      long jitterMillis = (long) (ThreadLocalRandom.current().nextDouble(-spread, spread));
      delayMillis = Math.max(0L, delayMillis + jitterMillis);
    }
    return Duration.ofMillis(delayMillis);
  }
}
