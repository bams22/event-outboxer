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

import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Resolves the effective {@link FailureHandler} for a failed attempt, using the priority order from
 * {@code FailureHandler}'s Javadoc:
 *
 * <ol>
 *   <li>{@code EventHandler.failureHandler()} if non-null;
 *   <li>the per-type failure handler registered through this resolver;
 *   <li>the supplied library-wide default.
 * </ol>
 */
public final class FailureHandlerResolver {

  private final Map<String, FailureHandler<?>> perType;
  private final FailureHandler<?> defaultHandler;

  public FailureHandlerResolver(
      Map<String, FailureHandler<?>> perType, FailureHandler<?> defaultHandler) {
    Objects.requireNonNull(perType, "perType must not be null");
    this.perType = Map.copyOf(perType);
    this.defaultHandler = Objects.requireNonNull(defaultHandler, "defaultHandler must not be null");
  }

  /**
   * Resolve the failure handler to invoke for this failed event. Returns a raw-typed handler
   * because the payload type {@code T} is only known to the caller and the dispatcher sometimes
   * does not even have a deserialized payload (unknown-type / pre-handle failures).
   */
  public FailureHandler<?> resolve(EventHandler<?> handler) {
    Objects.requireNonNull(handler, "handler must not be null");
    @Nullable FailureHandler<?> onHandler = handler.failureHandler();
    if (onHandler != null) {
      return onHandler;
    }
    FailureHandler<?> byType = perType.get(handler.eventType());
    if (byType != null) {
      return byType;
    }
    return defaultHandler;
  }
}
