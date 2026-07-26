/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.dispatch;

import io.github.bams22.outboxer.core.config.UnknownHandlerPolicy;
import java.time.Duration;
import java.util.Objects;
import lombok.Builder;

/**
 * Configuration for the {@link HandlerDispatcher} — knobs that do not belong to per-event-type
 * settings.
 *
 * @param unknownHandlerPolicy how to handle a claimed event whose type has no registered handler
 *     (ADR-0013)
 * @param unknownHandlerRetryDelay delay applied when {@code unknownHandlerPolicy=SKIP}
 * @param lockBusyRetryDelay delay applied when {@code EntityLocker.tryLock} returns empty —
 *     short, so the event is tried again soon after the competing worker releases the lock
 * @param dispatchRejectedRetryDelay delay applied when the per-type handler executor rejects a
 *     dispatch (pool and queue saturated); the claimed event is released back to {@code PENDING}
 *     without burning an attempt and becomes eligible again after this delay
 */
@Builder
public record DispatcherConfig(
    UnknownHandlerPolicy unknownHandlerPolicy,
    Duration unknownHandlerRetryDelay,
    Duration lockBusyRetryDelay,
    Duration dispatchRejectedRetryDelay) {

  public DispatcherConfig {
    Objects.requireNonNull(unknownHandlerPolicy, "unknownHandlerPolicy must not be null");
    Objects.requireNonNull(unknownHandlerRetryDelay, "unknownHandlerRetryDelay must not be null");
    Objects.requireNonNull(lockBusyRetryDelay, "lockBusyRetryDelay must not be null");
    Objects.requireNonNull(
        dispatchRejectedRetryDelay, "dispatchRejectedRetryDelay must not be null");
    if (unknownHandlerRetryDelay.isNegative()) {
      throw new IllegalArgumentException(
          "unknownHandlerRetryDelay must not be negative, got " + unknownHandlerRetryDelay);
    }
    if (lockBusyRetryDelay.isNegative()) {
      throw new IllegalArgumentException(
          "lockBusyRetryDelay must not be negative, got " + lockBusyRetryDelay);
    }
    if (dispatchRejectedRetryDelay.isNegative()) {
      throw new IllegalArgumentException(
          "dispatchRejectedRetryDelay must not be negative, got " + dispatchRejectedRetryDelay);
    }
  }

  public static DispatcherConfig defaults() {
    return DispatcherConfig.builder()
        .unknownHandlerPolicy(UnknownHandlerPolicy.SKIP)
        .unknownHandlerRetryDelay(Duration.ofMinutes(1))
        .lockBusyRetryDelay(Duration.ofSeconds(1))
        .dispatchRejectedRetryDelay(Duration.ofSeconds(1))
        .build();
  }
}
