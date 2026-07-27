/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.diagnostics.FailureAnalyzer;
import org.springframework.core.Ordered;

/**
 * Renders {@link AmbiguousOutboxDataSourceException} — several {@code DataSource} beans and no way
 * to pick the outbox one (ADR-0024) — as an actionable startup diagnosis instead of a stack-trace
 * puzzle.
 */
public class OutboxDataSourceFailureAnalyzer implements FailureAnalyzer, Ordered {

  @Override
  public int getOrder() {
    // Ahead of Boot's generic NoUniqueBeanDefinitionException analyzer.
    return Ordered.HIGHEST_PRECEDENCE + 100;
  }

  @Override
  public @Nullable FailureAnalysis analyze(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof AmbiguousOutboxDataSourceException ambiguous) {
        return new FailureAnalysis(ambiguous.getMessage(), action(), failure);
      }
    }
    return null;
  }

  private static String action() {
    return "Mark exactly one DataSource bean with "
        + "@io.github.bams22.outboxer.spring.OutboxDataSource so the outbox knows which database "
        + "holds its tables, or declare one DataSource @Primary. Defining your own "
        + "ConnectionSupplier / EntityLocker beans also overrides the outbox JDBC wiring entirely.";
  }
}
