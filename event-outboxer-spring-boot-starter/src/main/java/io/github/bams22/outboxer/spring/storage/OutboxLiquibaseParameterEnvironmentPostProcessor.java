/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.storage;

import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Feeds {@code outbox.storage.schema} into Liquibase as
 * {@code spring.liquibase.parameters.eventOutboxerSchema} so the library's classpath changelog
 * ({@code classpath:db/changelog/outbox/core/changelog.xml}) picks up the same schema name as
 * the PG adapter at runtime.
 *
 * <p>The value is contributed via a {@code MapPropertySource} appended with {@code addLast(...)},
 * so any user-supplied entry from {@code application.yml} /
 * {@code spring.liquibase.parameters.eventOutboxerSchema} or an earlier property source takes
 * precedence.
 *
 * <p>Registered via {@code META-INF/spring.factories}; runs at
 * {@link Ordered#LOWEST_PRECEDENCE} so user config is fully loaded before we read
 * {@code outbox.storage.schema}.
 */
public class OutboxLiquibaseParameterEnvironmentPostProcessor
    implements EnvironmentPostProcessor, Ordered {

  static final String PROPERTY_SOURCE_NAME = "event-outboxer-liquibase-parameters";
  static final String SCHEMA_PROPERTY = "event-outboxer.storage.schema";
  static final String DEFAULT_SCHEMA = "event_outboxer";
  static final String LIQUIBASE_PARAMETER_KEY =
      "spring.liquibase.parameters.eventOutboxerSchema";

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication application) {
    String schema = env.getProperty(SCHEMA_PROPERTY, DEFAULT_SCHEMA);
    env.getPropertySources()
        .addLast(
            new MapPropertySource(
                PROPERTY_SOURCE_NAME, Map.of(LIQUIBASE_PARAMETER_KEY, schema)));
  }
}
