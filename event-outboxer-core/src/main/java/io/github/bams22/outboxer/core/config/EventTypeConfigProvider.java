/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.config;

import java.util.Map;
import java.util.Objects;

/**
 * Supplies a resolved {@link EventTypeConfig} for a given event type. Returns the default
 * configuration when no explicit override is registered.
 *
 * <p>The starter (P9) owns the thin-merge logic: YAML binding reads every field with a fallback to
 * the default, producing a fully-populated {@code EventTypeConfig} that we store here as a plain
 * map.
 */
public final class EventTypeConfigProvider {

  private final EventTypeConfig defaults;
  private final Map<String, EventTypeConfig> overrides;

  public EventTypeConfigProvider(EventTypeConfig defaults, Map<String, EventTypeConfig> overrides) {
    this.defaults = Objects.requireNonNull(defaults, "defaults must not be null");
    Objects.requireNonNull(overrides, "overrides must not be null");
    this.overrides = Map.copyOf(overrides);
  }

  /** Provider with no overrides — every type gets the same {@code defaults}. */
  public static EventTypeConfigProvider uniform(EventTypeConfig defaults) {
    return new EventTypeConfigProvider(defaults, Map.of());
  }

  /** Resolve the effective config for {@code eventType}. */
  public EventTypeConfig forType(String eventType) {
    Objects.requireNonNull(eventType, "eventType must not be null");
    EventTypeConfig override = overrides.get(eventType);
    return override != null ? override : defaults;
  }

  public EventTypeConfig defaults() {
    return defaults;
  }

  public Map<String, EventTypeConfig> overrides() {
    return overrides;
  }
}
