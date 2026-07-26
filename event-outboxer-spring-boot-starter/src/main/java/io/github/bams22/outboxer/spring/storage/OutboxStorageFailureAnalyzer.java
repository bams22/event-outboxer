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

import io.github.bams22.outboxer.spi.EventStore;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.diagnostics.FailureAnalyzer;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

/**
 * Turns the cryptic "No qualifying bean of type EventStore" startup failure into an actionable
 * diagnosis. With no default storage (ADR-0020: in-memory is deliberately unreachable through
 * configuration) this failure is the expected outcome of an unconfigured outbox, so the message
 * must carry the fix, not a stack trace puzzle.
 */
public class OutboxStorageFailureAnalyzer implements FailureAnalyzer, EnvironmentAware, Ordered {

  private static final String STORAGE_TYPE_PROPERTY = "event-outboxer.storage.type";
  private static final String PG_STORE_CLASS =
      "io.github.bams22.outboxer.storage.postgres.PostgresEventStore";

  private @Nullable Environment environment;

  @Override
  public void setEnvironment(Environment environment) {
    this.environment = environment;
  }

  @Override
  public int getOrder() {
    // Ahead of Boot's generic NoSuchBeanDefinitionFailureAnalyzer.
    return Ordered.HIGHEST_PRECEDENCE + 100;
  }

  @Override
  public @Nullable FailureAnalysis analyze(Throwable failure) {
    NoSuchBeanDefinitionException noBean = findEventStoreFailure(failure);
    if (noBean == null) {
      return null;
    }
    String configuredType =
        environment == null
            ? null
            : Binder.get(environment).bind(STORAGE_TYPE_PROPERTY, String.class).orElse(null);
    return new FailureAnalysis(describe(configuredType), action(configuredType), failure);
  }

  private static String describe(@Nullable String configuredType) {
    if (configuredType == null) {
      return "The outbox has no storage configured: '"
          + STORAGE_TYPE_PROPERTY
          + "' is not set and no EventStore bean is defined. There is deliberately no default "
          + "and no in-memory fallback — a silently non-durable outbox would not participate "
          + "in your transactions and would lose events on restart (ADR-0020).";
    }
    return "The outbox storage type is set to '"
        + configuredType
        + "' but no EventStore bean was created.";
  }

  private String action(@Nullable String configuredType) {
    if (configuredType == null) {
      return "Set "
          + STORAGE_TYPE_PROPERTY
          + "=postgres (with a DataSource and the event-outboxer-storage-postgres dependency). "
          + "For tests without a database, add event-outboxer-storage-inmemory (test scope) and "
          + "@Import(OutboxInMemoryTestConfiguration.class).";
    }
    if ("postgres".equalsIgnoreCase(configuredType)
        && !ClassUtils.isPresent(PG_STORE_CLASS, getClass().getClassLoader())) {
      return "Add the event-outboxer-storage-postgres dependency — it is not on the classpath.";
    }
    return "Ensure a javax.sql.DataSource bean exists (e.g. add spring-boot-starter-jdbc and "
        + "spring.datasource.* properties): the PostgreSQL storage auto-configuration only "
        + "activates when a DataSource is present.";
  }

  private static @Nullable NoSuchBeanDefinitionException findEventStoreFailure(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof NoSuchBeanDefinitionException noBean
          && noBean.getBeanType() != null
          && EventStore.class.equals(noBean.getBeanType())) {
        return noBean;
      }
    }
    return null;
  }
}
