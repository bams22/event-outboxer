/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.serializer.jackson;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * Static factory for the {@link ObjectMapper} the outbox uses when the caller does not supply its
 * own. The starter's {@code JacksonSerializerAutoConfiguration} consults, in priority order:
 *
 * <ol>
 *   <li>a bean named {@code outboxObjectMapper};
 *   <li>Spring Boot's primary {@code ObjectMapper};
 *   <li>{@link #defaults()} below.
 * </ol>
 *
 * <p>The defaults are chosen to make a round-trip predictable for the kinds of DTOs users write in
 * production services: {@code java.time.*} via {@link JavaTimeModule}, Optional/Stream via {@link
 * Jdk8Module}, record component names via {@link ParameterNamesModule}, ISO-8601 timestamps
 * (instead of epoch-millis) preserved in their original zone, and an evolution-friendly {@code
 * FAIL_ON_UNKNOWN_PROPERTIES=false} (ADR-0011): unknown fields are ignored and absent fields take
 * defaults, so mixed-version replicas can read each other's payloads during a rolling deploy —
 * adding or removing a DTO field is a zero-downtime change in both directions. {@code
 * FAIL_ON_NULL_FOR_PRIMITIVES} is deliberately NOT enabled: for record DTOs (the norm per ADR-0003)
 * Jackson routes an <em>absent</em> primitive component through the null path, so enabling it would
 * break exactly the add-a-primitive-field evolution this mapper is meant to survive.
 *
 * <p>Applications that want strict deserialization back opt in by providing their own mapper — a
 * bean named {@code outboxObjectMapper} in the starter, or a customized instance passed to {@link
 * JacksonEventSerializer} in plain-Java setups.
 */
public final class JacksonObjectMapperFactory {

  private JacksonObjectMapperFactory() {}

  /**
   * Canonical outbox {@link ObjectMapper}. Each call returns a fresh instance — callers are free to
   * further customize it via {@code objectMapper.registerModule(...)} before handing it to {@link
   * JacksonEventSerializer}.
   */
  public static ObjectMapper defaults() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .registerModule(new Jdk8Module())
        .registerModule(new ParameterNamesModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
  }
}
