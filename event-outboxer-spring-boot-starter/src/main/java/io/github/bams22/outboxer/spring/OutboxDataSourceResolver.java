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
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

/**
 * Resolves the {@link DataSource} the outbox uses, shared by the storage and lock
 * auto-configurations (ADR-0024): {@link OutboxDataSource @OutboxDataSource}-qualified bean first,
 * then Spring's unique/{@code @Primary} bean, then a descriptive failure.
 *
 * <p>Both lookups go through {@link ObjectProvider} so nothing is injected eagerly — with several
 * unmarked {@code DataSource} beans a qualified one still resolves cleanly instead of tripping
 * Spring's stock {@code NoUniqueBeanDefinitionException} on a fallback parameter.
 */
public final class OutboxDataSourceResolver {

  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(OutboxDataSourceResolver.class);

  private OutboxDataSourceResolver() {}

  /**
   * Strict resolution for wiring that cannot proceed without a {@code DataSource}: the
   * {@code @OutboxDataSource}-qualified bean, else the unique or {@code @Primary} bean, else {@link
   * AmbiguousOutboxDataSourceException} naming the candidates.
   *
   * @param qualified provider injected with {@code @OutboxDataSource ObjectProvider<DataSource>}
   * @param all provider injected without a qualifier (all {@code DataSource} beans)
   * @param beanFactory used only to name the candidates in the failure message
   */
  public static DataSource resolve(
      ObjectProvider<DataSource> qualified,
      ObjectProvider<DataSource> all,
      ListableBeanFactory beanFactory) {
    DataSource qualifiedBean;
    try {
      qualifiedBean = qualified.getIfAvailable();
    } catch (NoUniqueBeanDefinitionException ex) {
      throw AmbiguousOutboxDataSourceException.multipleQualified(namesFrom(ex));
    }
    if (qualifiedBean != null) {
      return qualifiedBean;
    }
    DataSource unique = all.getIfUnique();
    if (unique != null) {
      return unique;
    }
    List<String> candidates =
        List.of(beanFactory.getBeanNamesForType(DataSource.class, true, false));
    if (candidates.size() < 2) {
      // Unreachable behind @ConditionalOnBean(DataSource.class) — getIfUnique() returns null
      // only for zero or 2+ candidates.
      throw new IllegalStateException("No DataSource bean available for the outbox.");
    }
    throw AmbiguousOutboxDataSourceException.noneQualified(candidates);
  }

  /**
   * Lenient resolution for best-effort diagnostics (the HikariCP pool-size warning): same order as
   * {@link #resolve}, but any ambiguity yields {@code null} instead of failing startup.
   */
  public static @Nullable DataSource resolveIfUnambiguous(
      ObjectProvider<DataSource> qualified, ObjectProvider<DataSource> all) {
    try {
      DataSource qualifiedBean = qualified.getIfAvailable();
      if (qualifiedBean != null) {
        return qualifiedBean;
      }
    } catch (NoUniqueBeanDefinitionException ex) {
      return null;
    }
    return all.getIfUnique();
  }

  /**
   * Strips {@link TransactionAwareDataSourceProxy} layers for the PostgreSQL entity lockers, whose
   * acquire/release statements must run as their own autocommit transactions on raw connections —
   * never bound to the caller's transaction (ADR-0022 §JDBC contract). Other {@code
   * DelegatingDataSource} wrappers pass through untouched.
   */
  public static DataSource unwrapTransactionAware(DataSource dataSource) {
    DataSource ds = dataSource;
    while (ds instanceof TransactionAwareDataSourceProxy proxy) {
      DataSource target = proxy.getTargetDataSource();
      if (target == null) {
        throw new IllegalStateException(
            "TransactionAwareDataSourceProxy has no target DataSource to unwrap.");
      }
      log.debug("Unwrapping TransactionAwareDataSourceProxy — outbox locks need raw connections");
      ds = target;
    }
    return ds;
  }

  private static Collection<String> namesFrom(NoUniqueBeanDefinitionException ex) {
    Collection<String> names = ex.getBeanNamesFound();
    return names != null ? names : List.of();
  }
}
