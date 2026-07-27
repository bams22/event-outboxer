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
import io.github.bams22.outboxer.domain.exception.DuplicateHandlerException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Immutable registry of {@link EventHandler} instances keyed by {@code eventType}. Built once from
 * the set of handlers discovered at engine startup; duplicate event-type registrations fail fast
 * with {@link DuplicateHandlerException}.
 */
public final class EventHandlerResolver {

  private final Map<String, EventHandler<?>> byType;

  public EventHandlerResolver(List<EventHandler<?>> handlers) {
    Objects.requireNonNull(handlers, "handlers must not be null");
    Map<String, EventHandler<?>> map = new HashMap<>(handlers.size() * 2);
    for (EventHandler<?> h : handlers) {
      Objects.requireNonNull(h, "handler element must not be null");
      String type = h.eventType();
      if (type == null || type.isBlank()) {
        throw new IllegalArgumentException(
            "handler " + h.getClass().getName() + " returned null/blank eventType");
      }
      EventHandler<?> previous = map.putIfAbsent(type, h);
      if (previous != null) {
        throw new DuplicateHandlerException(
            type,
            "two handlers registered for eventType '"
                + type
                + "': "
                + previous.getClass().getName()
                + " and "
                + h.getClass().getName());
      }
    }
    this.byType = Map.copyOf(map);
  }

  /** Lookup a handler by event type, or {@link Optional#empty()} if none is registered. */
  public Optional<EventHandler<?>> find(String eventType) {
    Objects.requireNonNull(eventType, "eventType must not be null");
    return Optional.ofNullable(byType.get(eventType));
  }

  /** Set of registered event types — used by the engine to build per-type pollers and pools. */
  public Set<String> registeredTypes() {
    return byType.keySet();
  }

  public int size() {
    return byType.size();
  }
}
