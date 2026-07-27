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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Decorator that logs each failure at a configurable {@link Level} and then delegates to the inner
 * handler. Does not affect the decision; purely diagnostic.
 *
 * @param <T> payload type
 */
public final class LogFailureHandler<T> implements FailureHandler<T> {

  private static final Logger LOG = LoggerFactory.getLogger(LogFailureHandler.class);

  private final Level level;
  private final FailureHandler<T> delegate;

  /** Creates a decorator that logs at {@link Level#WARN}. */
  public LogFailureHandler(FailureHandler<T> delegate) {
    this(Level.WARN, delegate);
  }

  public LogFailureHandler(Level level, FailureHandler<T> delegate) {
    this.level = Objects.requireNonNull(level, "level must not be null");
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
  }

  public Level level() {
    return level;
  }

  @Override
  public FailureDecision onFailure(FailureContext<T> ctx) {
    if (LOG.isEnabledForLevel(level)) {
      LOG.atLevel(level)
          .addArgument(ctx.event().id())
          .addArgument(ctx.event().eventType())
          .addArgument(ctx.attempt())
          .addArgument(FailureUtil.reason(ctx))
          .setCause(ctx.cause())
          .log("Outbox handler failure: event={} type={} attempt={} reason={}");
    }
    return delegate.onFailure(ctx);
  }
}
