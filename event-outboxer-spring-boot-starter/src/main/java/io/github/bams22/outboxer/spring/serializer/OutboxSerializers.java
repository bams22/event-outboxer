/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.serializer;

import io.github.bams22.outboxer.domain.exception.InvariantViolationException;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spi.EventSerializerRegistry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Starter-internal resolution of the serializer roster (ADR-0025): every registered {@link
 * EventSerializer} bean deserializes (routed by the stored {@code payload_format}), exactly one —
 * {@link #write()} — serializes new events, except for event types listed in {@link
 * #writePerType()} which write with their mapped serializer instead (ADR-0025 amendment).
 *
 * @param write the serializer that writes new events by default
 * @param writePerType per-event-type write overrides (event type → serializer)
 * @param readOnly the remaining serializers, registered for deserialization only
 */
public record OutboxSerializers(
    EventSerializer write,
    Map<String, EventSerializer> writePerType,
    List<EventSerializer> readOnly) {

  /** Bean name of the documented user-override serializer (CONFIGURATION.md §Serialization). */
  public static final String OVERRIDE_BEAN_NAME = "outboxEventSerializer";

  public OutboxSerializers {
    Objects.requireNonNull(write, "write must not be null");
    writePerType = Map.copyOf(writePerType);
    readOnly = List.copyOf(readOnly);
  }

  /**
   * Resolve the write serializer from the registered beans (keyed by bean name):
   *
   * <ol>
   *   <li>{@code event-outboxer.serializer.write-format} set → the serializer with that format
   *       (fail-fast if absent);
   *   <li>exactly one bean → it (the zero-config default);
   *   <li>several beans, one named {@code outboxEventSerializer} → it, so the documented override
   *       keeps winning even next to extra read-only serializers;
   *   <li>otherwise → {@link InvariantViolationException} listing the registered formats.
   * </ol>
   *
   * <p>{@code writeFormatPerType} ({@code event-outboxer.serializer.write-format-per-type}) maps
   * event types to the format that writes them instead of the default — each entry must name a
   * registered format (fail-fast otherwise).
   *
   * <p>Building the registry up front also fail-fasts duplicate or invalid format ids at startup.
   */
  public static OutboxSerializers resolve(
      Map<String, EventSerializer> beansByName,
      @org.jspecify.annotations.Nullable String writeFormat,
      Map<String, String> writeFormatPerType) {
    if (beansByName.isEmpty()) {
      throw new InvariantViolationException(
          "no EventSerializer beans registered — add event-outboxer-serializer-jackson or"
              + " register an EventSerializer bean");
    }
    EventSerializerRegistry registry = EventSerializerRegistry.of(beansByName.values());
    EventSerializer write = resolveWrite(beansByName, registry, writeFormat);
    Map<String, EventSerializer> writePerType = resolveWritePerType(registry, writeFormatPerType);
    List<EventSerializer> readOnly = beansByName.values().stream().filter(s -> s != write).toList();
    return new OutboxSerializers(write, writePerType, readOnly);
  }

  private static Map<String, EventSerializer> resolveWritePerType(
      EventSerializerRegistry registry, Map<String, String> writeFormatPerType) {
    Map<String, EventSerializer> resolved = new LinkedHashMap<>();
    writeFormatPerType.forEach(
        (eventType, format) -> {
          if (eventType.isBlank()) {
            throw new InvariantViolationException(
                "event-outboxer.serializer.write-format-per-type keys must be event types —"
                    + " found a blank key");
          }
          if (format == null || format.isBlank()) {
            throw new InvariantViolationException(
                "event-outboxer.serializer.write-format-per-type."
                    + eventType
                    + " must name a serializer format — found a blank value");
          }
          resolved.put(
              eventType,
              registry
                  .find(format)
                  .orElseThrow(
                      () ->
                          new InvariantViolationException(
                              "event-outboxer.serializer.write-format-per-type."
                                  + eventType
                                  + "='"
                                  + format
                                  + "' matches no registered EventSerializer; registered"
                                  + " formats: "
                                  + registry.formats())));
        });
    return resolved;
  }

  private static EventSerializer resolveWrite(
      Map<String, EventSerializer> beansByName,
      EventSerializerRegistry registry,
      @org.jspecify.annotations.Nullable String writeFormat) {
    if (writeFormat != null && !writeFormat.isBlank()) {
      return registry
          .find(writeFormat)
          .orElseThrow(
              () ->
                  new InvariantViolationException(
                      "event-outboxer.serializer.write-format='"
                          + writeFormat
                          + "' matches no registered EventSerializer; registered formats: "
                          + registry.formats()));
    }
    if (beansByName.size() == 1) {
      return beansByName.values().iterator().next();
    }
    EventSerializer override = beansByName.get(OVERRIDE_BEAN_NAME);
    if (override != null) {
      return override;
    }
    throw new InvariantViolationException(
        "multiple EventSerializer beans "
            + beansByName.keySet()
            + " with formats "
            + registry.formats()
            + " — set event-outboxer.serializer.write-format to pick the one that writes new"
            + " events, or name your primary serializer bean '"
            + OVERRIDE_BEAN_NAME
            + "'");
  }
}
