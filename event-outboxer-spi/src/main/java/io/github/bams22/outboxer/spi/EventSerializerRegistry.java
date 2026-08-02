/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import io.github.bams22.outboxer.domain.exception.InvariantViolationException;
import io.github.bams22.outboxer.domain.exception.UnknownPayloadFormatException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Immutable lookup of {@link EventSerializer}s by their stable {@link EventSerializer#format()} id
 * (ADR-0025).
 *
 * <p>The engine writes every event with a single configured serializer, but deserializes by the
 * {@code payloadFormat} stored on the claimed event — so a registry holding yesterday's format next
 * to today's makes format migrations and rolling deploys safe: in-flight events keep routing to the
 * serializer that wrote them.
 *
 * <p>Thread-safe: the registry is immutable after construction.
 */
public final class EventSerializerRegistry {

  private final Map<String, EventSerializer> byFormat;

  private EventSerializerRegistry(Map<String, EventSerializer> byFormat) {
    this.byFormat = byFormat;
  }

  /**
   * Build a registry from the given serializers.
   *
   * @throws InvariantViolationException if two serializers declare the same format id, or a format
   *     id is blank or longer than 64 characters
   */
  public static EventSerializerRegistry of(Collection<? extends EventSerializer> serializers) {
    Objects.requireNonNull(serializers, "serializers must not be null");
    Map<String, EventSerializer> byFormat = new LinkedHashMap<>();
    for (EventSerializer serializer : serializers) {
      Objects.requireNonNull(serializer, "serializer must not be null");
      String format = serializer.format();
      if (format == null || format.isBlank() || format.length() > 64) {
        throw new InvariantViolationException(
            "serializer format id must be non-blank and at most 64 characters, got '"
                + format
                + "' from "
                + serializer.getClass().getName());
      }
      EventSerializer previous = byFormat.putIfAbsent(format, serializer);
      if (previous != null && previous != serializer) {
        throw new InvariantViolationException(
            "duplicate serializer format '"
                + format
                + "': "
                + previous.getClass().getName()
                + " and "
                + serializer.getClass().getName());
      }
    }
    if (byFormat.isEmpty()) {
      throw new InvariantViolationException("at least one EventSerializer is required");
    }
    // Not Map.copyOf: registration order must survive into formats() (documented contract).
    return new EventSerializerRegistry(Collections.unmodifiableMap(byFormat));
  }

  /** Look up a serializer by format id, empty if none is registered. */
  public Optional<EventSerializer> find(String format) {
    Objects.requireNonNull(format, "format must not be null");
    return Optional.ofNullable(byFormat.get(format));
  }

  /**
   * Look up a serializer by format id.
   *
   * @throws UnknownPayloadFormatException if no serializer is registered for {@code format}
   */
  public EventSerializer require(String format) {
    Objects.requireNonNull(format, "format must not be null");
    EventSerializer serializer = byFormat.get(format);
    if (serializer == null) {
      throw new UnknownPayloadFormatException(
          "no EventSerializer registered for payload format '"
              + format
              + "'; registered formats: "
              + byFormat.keySet());
    }
    return serializer;
  }

  /** The registered format ids, in registration order. */
  public Set<String> formats() {
    return byFormat.keySet();
  }
}
