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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bams22.outboxer.serializer.jackson.JacksonEventSerializer;
import io.github.bams22.outboxer.serializer.jackson.JacksonObjectMapperFactory;
import io.github.bams22.outboxer.spi.EventSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration for the Jackson {@link EventSerializer}. Resolves the underlying
 * {@link ObjectMapper} in priority order (see ADR follow-up in the implementation plan, §7a):
 *
 * <ol>
 *   <li>A bean named {@code outboxObjectMapper} — explicit opt-in for a dedicated mapper.
 *   <li>Spring Boot's primary {@link ObjectMapper} (set up by {@code JacksonAutoConfiguration}).
 *   <li>{@link JacksonObjectMapperFactory#defaults()} — library fallback.
 * </ol>
 */
@AutoConfiguration
@ConditionalOnClass({JacksonEventSerializer.class, ObjectMapper.class})
public class JacksonSerializerAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(name = "outboxEventSerializer")
  public EventSerializer outboxEventSerializer(
      @Autowired(required = false)
          @Qualifier("outboxObjectMapper") @Nullable ObjectMapper qualified,
      ObjectProvider<ObjectMapper> primary) {
    ObjectMapper mapper =
        qualified != null ? qualified : primary.getIfAvailable(JacksonObjectMapperFactory::defaults);
    return new JacksonEventSerializer(mapper);
  }
}
