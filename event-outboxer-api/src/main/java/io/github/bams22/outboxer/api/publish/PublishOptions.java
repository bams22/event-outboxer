/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.publish;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Optional per-call tuning for {@link OutboxEventPublisher#publish(String, Object,
 * PublishOptions)}. All fields are nullable; {@code null} means "use the engine default".
 *
 * <p>There is no {@code lockKey} field here on purpose: lock keys are derived from the payload
 * at handle time by {@code EventHandler.extractLockKey(payload)} and are not stored alongside
 * the event (see ADR-0012).
 *
 * @param runAt earliest time the event may be claimed; defaults to {@code now}
 * @param priority explicit priority; defaults to 0
 * @param traceContext W3C traceparent/baggage to attach; normally the publisher captures this
 *     from the current MDC/Observation context, but callers may override it
 */
@Builder
public record PublishOptions(
    @Nullable Instant runAt,
    @Nullable Short priority,
    @Nullable Map<String, String> traceContext) {

  public PublishOptions {
    traceContext =
        traceContext == null ? null : Collections.unmodifiableMap(Map.copyOf(traceContext));
  }

  /** Canonical empty instance — all defaults. */
  public static PublishOptions defaults() {
    return new PublishOptions(null, null, null);
  }
}
