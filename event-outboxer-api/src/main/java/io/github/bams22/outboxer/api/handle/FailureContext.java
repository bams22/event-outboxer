/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.handle;

import io.github.bams22.outboxer.domain.ClaimedEvent;
import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Context passed into {@link FailureHandler#onFailure(FailureContext)}. Carries everything a
 * failure handler might need to decide what to do: the event itself (via {@code ClaimedEvent}), the
 * deserialized payload, the outcome or cause, the attempt counter, and the current time.
 *
 * @param event the event that failed, as returned from claim
 * @param payload the deserialized payload; null when the failure occurred before deserialization
 *     (for example, unknown event type)
 * @param outcome the outcome returned by the handler, if any; null when the failure came from an
 *     uncaught exception before the handler returned
 * @param cause underlying exception, if any; null when the handler returned an explicit non-null
 *     {@code outcome}
 * @param attempt 1-based counter, including the current failed attempt
 * @param now wall-clock time at which the failure handler is invoked
 * @param <T> payload type corresponding to the {@code EventHandler} that failed
 */
public record FailureContext<T>(
    ClaimedEvent event,
    @Nullable T payload,
    @Nullable EventOutcome outcome,
    @Nullable Throwable cause,
    int attempt,
    Instant now) {

  public FailureContext {
    Objects.requireNonNull(event, "event must not be null");
    Objects.requireNonNull(now, "now must not be null");
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be >= 1, got " + attempt);
    }
  }
}
