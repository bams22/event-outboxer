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
 * production services: {@code java.time.*} via {@link JavaTimeModule}, Optional/Stream via
 * {@link Jdk8Module}, record component names via {@link ParameterNamesModule}, ISO-8601
 * timestamps (instead of epoch-millis) and a strict {@code FAIL_ON_UNKNOWN_PROPERTIES=true} so
 * schema drift is caught at deserialize time rather than silently dropped.
 */
public final class JacksonObjectMapperFactory {

  private JacksonObjectMapperFactory() {}

  /**
   * Canonical outbox {@link ObjectMapper}. Each call returns a fresh instance — callers are free
   * to further customize it via {@code objectMapper.registerModule(...)} before handing it to
   * {@link JacksonEventSerializer}.
   */
  public static ObjectMapper defaults() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .registerModule(new Jdk8Module())
        .registerModule(new ParameterNamesModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID)
        .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
  }
}
