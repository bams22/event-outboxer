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

import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.event.Level;

/**
 * Fluent builder for a {@link FailureHandler} chain. Decorators are applied in the order added
 * and wrap around the leaf created by one of the {@code withExponentialBackoff(...)},
 * {@code withFixedDelay(...)} or {@code withNoRetry()} terminators.
 *
 * <p>Typical use:
 *
 * <pre>
 * FailureHandler&lt;MyPayload&gt; fh = FailureHandlers.&lt;MyPayload&gt;builder()
 *     .withLogging(Level.WARN)
 *     .withListener(outboxListener)
 *     .withMaxAttempts(10, ExhaustedAction.DISABLE)
 *     .withExponentialBackoff(Duration.ofSeconds(5), 2.0, Duration.ofHours(1), 0.2);
 * </pre>
 *
 * @param <T> payload type
 */
public final class FailureHandlerBuilder<T> {

  private Level logLevel;
  private OutboxListener listener;
  private Integer maxAttempts;
  private MaxRetriesFailureHandler.ExhaustedAction exhaustedAction;

  FailureHandlerBuilder() {}

  /** Adds a {@link LogFailureHandler} decorator at the given level. */
  public FailureHandlerBuilder<T> withLogging(Level level) {
    this.logLevel = Objects.requireNonNull(level, "level must not be null");
    return this;
  }

  /** Adds a {@link NotifyListenerFailureHandler} decorator forwarding decisions to {@code listener}. */
  public FailureHandlerBuilder<T> withListener(OutboxListener listener) {
    this.listener = Objects.requireNonNull(listener, "listener must not be null");
    return this;
  }

  /** Adds a {@link MaxRetriesFailureHandler} decorator with the given attempt cap. */
  public FailureHandlerBuilder<T> withMaxAttempts(
      int max, MaxRetriesFailureHandler.ExhaustedAction onExhausted) {
    if (max < 1) {
      throw new IllegalArgumentException("max must be >= 1, got " + max);
    }
    this.maxAttempts = max;
    this.exhaustedAction = Objects.requireNonNull(onExhausted, "onExhausted must not be null");
    return this;
  }

  /**
   * Terminates the chain with an {@link ExponentialBackoffFailureHandler}. Call this last — the
   * returned value is a ready-to-use {@link FailureHandler}.
   */
  public FailureHandler<T> withExponentialBackoff(
      Duration base, double multiplier, Duration cap, double jitter) {
    return wrap(new ExponentialBackoffFailureHandler<>(base, multiplier, cap, jitter));
  }

  /** Terminates the chain with a {@link FixedDelayFailureHandler}. */
  public FailureHandler<T> withFixedDelay(Duration delay) {
    return wrap(new FixedDelayFailureHandler<>(delay));
  }

  /** Terminates the chain with a {@link NoRetryFailureHandler} (every failure goes to DISABLED). */
  public FailureHandler<T> withNoRetry() {
    return wrap(new NoRetryFailureHandler<>());
  }

  private FailureHandler<T> wrap(FailureHandler<T> leaf) {
    FailureHandler<T> chain = leaf;
    if (maxAttempts != null) {
      chain = new MaxRetriesFailureHandler<>(maxAttempts, exhaustedAction, chain);
    }
    if (listener != null) {
      chain = new NotifyListenerFailureHandler<>(listener, chain);
    }
    if (logLevel != null) {
      chain = new LogFailureHandler<>(logLevel, chain);
    }
    return chain;
  }
}
