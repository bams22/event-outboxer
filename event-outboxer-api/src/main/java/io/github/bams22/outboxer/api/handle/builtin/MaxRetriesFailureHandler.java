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

import io.github.bams22.outboxer.api.handle.FailureContext;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import java.util.Objects;

/**
 * Decorator that caps the number of attempts. On the {@code maxAttempts}th failure the event is
 * finalized according to {@link ExhaustedAction}; earlier failures are delegated to the inner
 * handler.
 *
 * <p>The {@code EventOutcome.Fail} case is also finalized immediately regardless of attempt count —
 * an explicit {@code Fail} from the handler bypasses any remaining retries.
 *
 * @param <T> payload type
 */
public final class MaxRetriesFailureHandler<T> implements FailureHandler<T> {

  /** What to do when retries are exhausted. */
  public enum ExhaustedAction {
    /** Move the event to {@code DISABLED}. The default. */
    DISABLE,
    /** Permanently remove the event from the outbox. */
    DELETE
  }

  private final int maxAttempts;
  private final ExhaustedAction exhaustedAction;
  private final FailureHandler<T> delegate;

  public MaxRetriesFailureHandler(
      int maxAttempts, ExhaustedAction exhaustedAction, FailureHandler<T> delegate) {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
    }
    this.maxAttempts = maxAttempts;
    this.exhaustedAction =
        Objects.requireNonNull(exhaustedAction, "exhaustedAction must not be null");
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
  }

  public int maxAttempts() {
    return maxAttempts;
  }

  public ExhaustedAction exhaustedAction() {
    return exhaustedAction;
  }

  @Override
  public FailureDecision onFailure(FailureContext<T> ctx) {
    // An explicit Fail from the handler bypasses retry counting and finalizes immediately.
    if (ctx.outcome() instanceof io.github.bams22.outboxer.api.handle.EventOutcome.Fail) {
      return finalize("explicit Fail", ctx);
    }
    if (ctx.attempt() >= maxAttempts) {
      return finalize("max attempts (" + maxAttempts + ") exhausted", ctx);
    }
    return delegate.onFailure(ctx);
  }

  private FailureDecision finalize(String suffix, FailureContext<T> ctx) {
    String reason = FailureUtil.reason(ctx) + " — " + suffix;
    return switch (exhaustedAction) {
      case DISABLE -> new FailureDecision.Disable(reason);
      case DELETE -> new FailureDecision.Delete(reason);
    };
  }
}
