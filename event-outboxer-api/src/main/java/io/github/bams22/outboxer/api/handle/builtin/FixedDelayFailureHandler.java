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

/**
 * Leaf handler that always re-schedules at a fixed delay. Suitable for handlers that do not
 * benefit from exponential backoff — for example when failures are expected to be resolved
 * quickly or when the caller wants constant pressure on an external system.
 *
 * <p>A {@code delayOverride} supplied by the handler through {@code EventOutcome.Retry} takes
 * precedence over the configured fixed delay.
 *
 * @param <T> payload type
 */
public final class FixedDelayFailureHandler<T> implements FailureHandler<T> {

  private final Duration delay;

  public FixedDelayFailureHandler(Duration delay) {
    Objects.requireNonNull(delay, "delay must not be null");
    if (delay.isNegative() || delay.isZero()) {
      throw new IllegalArgumentException("delay must be positive, got " + delay);
    }
    this.delay = delay;
  }

  public Duration delay() {
    return delay;
  }

  @Override
  public FailureDecision onFailure(FailureContext<T> ctx) {
    Duration effective = delay;
    if (ctx.outcome() instanceof EventOutcome.Retry r && r.delayOverride() != null) {
      effective = r.delayOverride();
    }
    return new FailureDecision.RetryAt(ctx.now().plus(effective), FailureUtil.reason(ctx));
  }
}
