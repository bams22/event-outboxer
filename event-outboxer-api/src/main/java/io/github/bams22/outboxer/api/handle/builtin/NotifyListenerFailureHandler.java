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
import io.github.bams22.outboxer.api.observer.EventDeletedInfo;
import io.github.bams22.outboxer.api.observer.EventDisabledInfo;
import io.github.bams22.outboxer.api.observer.EventRetryScheduledInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorator that forwards the decision produced by the inner handler to an {@link OutboxListener}.
 * Produces one of {@code onEventRetryScheduled}, {@code onEventDisabled}, or {@code
 * onEventDeleted} depending on the decision. The listener is notified before the engine actually
 * applies the decision to storage, so a subsequent storage error (observed separately through
 * {@code onStorageError}) means the decision was announced but not yet persisted.
 *
 * @param <T> payload type
 */
public final class NotifyListenerFailureHandler<T> implements FailureHandler<T> {

  private static final Logger LOG = LoggerFactory.getLogger(NotifyListenerFailureHandler.class);

  private final OutboxListener listener;
  private final FailureHandler<T> delegate;

  public NotifyListenerFailureHandler(OutboxListener listener, FailureHandler<T> delegate) {
    this.listener = Objects.requireNonNull(listener, "listener must not be null");
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
  }

  @Override
  public FailureDecision onFailure(FailureContext<T> ctx) {
    FailureDecision decision = delegate.onFailure(ctx);
    try {
      switch (decision) {
        case FailureDecision.RetryAt r ->
            listener.onEventRetryScheduled(
                new EventRetryScheduledInfo(
                    ctx.event().id(),
                    ctx.event().eventType(),
                    ctx.attempt(),
                    r.when(),
                    r.reason(),
                    ctx.cause()));
        case FailureDecision.Disable d ->
            listener.onEventDisabled(
                new EventDisabledInfo(
                    ctx.event().id(),
                    ctx.event().eventType(),
                    ctx.attempt(),
                    d.reason(),
                    ctx.cause()));
        case FailureDecision.Delete d ->
            listener.onEventDeleted(
                new EventDeletedInfo(
                    ctx.event().id(), ctx.event().eventType(), ctx.attempt(), d.reason()));
      }
    } catch (Throwable t) {
      // Listener failures must not affect the decision flow.
      LOG.warn("OutboxListener.on*Event* callback threw", t);
    }
    return decision;
  }
}
