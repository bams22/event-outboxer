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
import java.time.Duration;

/**
 * Convenience entry point for constructing {@link FailureHandler} chains. Provides
 * {@link #defaults()} — the library-wide default recipe — and {@link #builder()} for custom
 * combinations.
 */
public final class FailureHandlers {

  private FailureHandlers() {}

  /**
   * Returns the default failure handler: {@code Log(WARN) → MaxRetries(10, DISABLE) →
   * ExponentialBackoff(base=5s, multiplier=2, cap=1h, jitter=0.2)}. The listener decorator is not
   * included here — the Spring Boot starter prepends it when an {@link
   * io.github.bams22.outboxer.api.observer.OutboxListener} is available.
   *
   * @param <T> payload type inferred from the use site
   */
  public static <T> FailureHandler<T> defaults() {
    return FailureHandlers.<T>builder()
        .withLogging(org.slf4j.event.Level.WARN)
        .withMaxAttempts(10, MaxRetriesFailureHandler.ExhaustedAction.DISABLE)
        .withExponentialBackoff(
            Duration.ofSeconds(5), 2.0, Duration.ofHours(1), 0.2);
  }

  /** Starts a new {@link FailureHandlerBuilder}. */
  public static <T> FailureHandlerBuilder<T> builder() {
    return new FailureHandlerBuilder<>();
  }
}
