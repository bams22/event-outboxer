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

import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;

/**
 * Thrown when the outbox cannot pick a {@link DataSource} unambiguously (ADR-0024): either several
 * beans carry {@link OutboxDataSource @OutboxDataSource}, or several {@code DataSource} beans exist
 * and none is qualified or {@code @Primary}.
 *
 * <p>Extends {@link NoUniqueBeanDefinitionException} so Spring Boot's stock failure analyzer still
 * applies as a fallback; {@link OutboxDataSourceFailureAnalyzer} renders the dedicated diagnosis.
 */
public class AmbiguousOutboxDataSourceException extends NoUniqueBeanDefinitionException {

  private final List<String> candidates;

  private AmbiguousOutboxDataSourceException(String message, Collection<String> candidates) {
    super(DataSource.class, candidates.size(), message);
    this.candidates = List.copyOf(candidates);
  }

  /** Two or more beans carry {@code @OutboxDataSource} — the qualifier must be unique. */
  static AmbiguousOutboxDataSourceException multipleQualified(Collection<String> candidates) {
    return new AmbiguousOutboxDataSourceException(
        "Found "
            + candidates.size()
            + " DataSource beans marked with @OutboxDataSource"
            + (candidates.isEmpty() ? "" : ": " + candidates)
            + " — exactly one bean may carry the qualifier, the outbox cannot choose between them.",
        candidates);
  }

  /**
   * Several {@code DataSource} beans exist and neither {@code @OutboxDataSource} nor
   * {@code @Primary} singles one out.
   */
  static AmbiguousOutboxDataSourceException noneQualified(Collection<String> candidates) {
    return new AmbiguousOutboxDataSourceException(
        "Found "
            + candidates.size()
            + " DataSource beans and none is @Primary or marked with @OutboxDataSource: "
            + candidates
            + ". The outbox cannot choose which database holds its tables — mark exactly one "
            + "bean with io.github.bams22.outboxer.spring.@OutboxDataSource (or declare one "
            + "@Primary).",
        candidates);
  }

  /** Bean names of the conflicting {@code DataSource} candidates. */
  public List<String> getCandidates() {
    return candidates;
  }
}
