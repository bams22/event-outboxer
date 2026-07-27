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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.lock.postgres.advisory.PgAdvisoryLocker;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spring.lock.PostgresAdvisoryLockAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.PostgresLeaseLockAutoConfiguration;
import io.github.bams22.outboxer.spring.serializer.JacksonSerializerAutoConfiguration;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import io.github.bams22.outboxer.spring.storage.PostgresStorageAutoConfiguration;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

/**
 * DataSource selection per ADR-0024: the {@code @OutboxDataSource}-qualified bean wins (even over
 * an unrelated {@code @Primary}), otherwise the unique/{@code @Primary} bean, otherwise startup
 * fails fast naming the candidates — while the best-effort Hikari pool warning resolves leniently
 * and never fails on ambiguity.
 */
class OutboxDataSourceSelectionTest {

  private final ApplicationContextRunner strictRunner =
      new ApplicationContextRunner().withUserConfiguration(StrictCaptureConfiguration.class);

  // -------------------------------------------------------------------------------------------
  // resolver semantics
  // -------------------------------------------------------------------------------------------

  @Test
  @DisplayName("the @OutboxDataSource-qualified bean wins over a plain one")
  void qualifiedBeanWins() {
    strictRunner
        .withUserConfiguration(QualifiedPlusPlainConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(ResolvedHolder.class).dataSource)
                  .isSameAs(ctx.getBean("outboxDs", DataSource.class));
            });
  }

  @Test
  @DisplayName("the qualified bean wins even when another bean is @Primary")
  void qualifiedBeatsPrimary() {
    strictRunner
        .withUserConfiguration(QualifiedVsPrimaryConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(ResolvedHolder.class).dataSource)
                  .isSameAs(ctx.getBean("outboxDs", DataSource.class));
            });
  }

  @Test
  @DisplayName("a single unmarked DataSource resolves as-is")
  void singleBeanResolves() {
    strictRunner
        .withUserConfiguration(SingleDataSourceConfiguration.class)
        .run(
            ctx ->
                assertThat(ctx.getBean(ResolvedHolder.class).dataSource)
                    .isSameAs(ctx.getBean("onlyDs", DataSource.class)));
  }

  @Test
  @DisplayName("with several beans and no qualifier, @Primary wins")
  void primaryWins() {
    strictRunner
        .withUserConfiguration(PrimaryPlusPlainConfiguration.class)
        .run(
            ctx ->
                assertThat(ctx.getBean(ResolvedHolder.class).dataSource)
                    .isSameAs(ctx.getBean("primaryDs", DataSource.class)));
  }

  @Test
  @DisplayName("several beans, none primary or qualified — startup fails naming the candidates")
  void ambiguousFailsFast() {
    strictRunner
        .withUserConfiguration(TwoPlainDataSourcesConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("@OutboxDataSource")
                  .hasStackTraceContaining("dsA")
                  .hasStackTraceContaining("dsB");
            });
  }

  @Test
  @DisplayName("two beans both carrying @OutboxDataSource — startup fails with 'exactly one'")
  void twoQualifiedFailFast() {
    strictRunner
        .withUserConfiguration(TwoQualifiedConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure()).hasStackTraceContaining("exactly one");
            });
  }

  @Test
  @DisplayName("lenient resolution returns null on ambiguity instead of failing")
  void lenientReturnsNullOnAmbiguity() {
    new ApplicationContextRunner()
        .withUserConfiguration(
            LenientCaptureConfiguration.class, TwoPlainDataSourcesConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(LenientHolder.class).dataSource).isNull();
            });
  }

  @Test
  @DisplayName("lenient resolution still prefers the qualified bean")
  void lenientPrefersQualified() {
    new ApplicationContextRunner()
        .withUserConfiguration(
            LenientCaptureConfiguration.class, QualifiedPlusPlainConfiguration.class)
        .run(
            ctx ->
                assertThat(ctx.getBean(LenientHolder.class).dataSource)
                    .isSameAs(ctx.getBean("outboxDs", DataSource.class)));
  }

  // -------------------------------------------------------------------------------------------
  // auto-configuration wiring
  // -------------------------------------------------------------------------------------------

  @Test
  @DisplayName("advisory locker boots against the qualified DataSource among several")
  void advisoryLockerUsesQualifiedDataSource() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(PostgresAdvisoryLockAutoConfiguration.class))
        .withUserConfiguration(QualifiedPlusPlainConfiguration.class)
        .withPropertyValues("event-outboxer.lock.type=postgres-advisory")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(EntityLocker.class)).isInstanceOf(PgAdvisoryLocker.class);
            });
  }

  @Test
  @DisplayName("storage adapter boots against the qualified DataSource among several")
  void storageAdapterUsesQualifiedDataSource() {
    postgresStorageRunner()
        .withUserConfiguration(QualifiedPlusPlainConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(ConnectionSupplier.class);
            });
  }

  @Test
  @DisplayName("storage adapter fails fast on ambiguous DataSources")
  void storageAdapterFailsOnAmbiguity() {
    postgresStorageRunner()
        .withUserConfiguration(TwoPlainDataSourcesConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure()).hasStackTraceContaining("@OutboxDataSource");
            });
  }

  @Test
  @DisplayName("a user EntityLocker displacing the lease locker skips DataSource resolution")
  void displacedLeaseLockerSkipsResolution() {
    EntityLocker custom = (key, ttl) -> Optional.empty();
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(PostgresLeaseLockAutoConfiguration.class))
        .withUserConfiguration(TwoPlainDataSourcesConfiguration.class)
        .withBean("customLocker", EntityLocker.class, () -> custom)
        .withPropertyValues("event-outboxer.lock.type=postgres-lease")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(EntityLocker.class)).isSameAs(custom);
            });
  }

  @Test
  @DisplayName(
      "engine pool warning resolves leniently: user locker + ambiguous DataSources still boots")
  void engineWarningToleratesAmbiguity() {
    EntityLocker custom = (key, ttl) -> Optional.empty();
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                JacksonSerializerAutoConfiguration.class, OutboxEngineAutoConfiguration.class))
        .withUserConfiguration(
            OutboxInMemoryTestConfiguration.class, TwoPlainDataSourcesConfiguration.class)
        .withBean("customLocker", EntityLocker.class, () -> custom)
        .withBean(NoopHandler.class)
        .withPropertyValues("event-outboxer.lock.type=postgres-advisory")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  // -------------------------------------------------------------------------------------------
  // unwrap + failure analyzer
  // -------------------------------------------------------------------------------------------

  @Test
  @DisplayName("unwrapTransactionAware strips nested proxies and passes raw DataSources through")
  void unwrapStripsProxyLayers() {
    DataSource raw = stubDataSource();
    DataSource doubleWrapped =
        new TransactionAwareDataSourceProxy(new TransactionAwareDataSourceProxy(raw));
    assertThat(OutboxDataSourceResolver.unwrapTransactionAware(doubleWrapped)).isSameAs(raw);
    assertThat(OutboxDataSourceResolver.unwrapTransactionAware(raw)).isSameAs(raw);
  }

  @Test
  @DisplayName("failure analyzer renders candidates and the fix for the ambiguity error")
  void analyzerRendersAmbiguityDiagnosis() {
    Throwable failure =
        new IllegalStateException(
            "wrapped", AmbiguousOutboxDataSourceException.noneQualified(List.of("dsA", "dsB")));
    FailureAnalysis analysis = new OutboxDataSourceFailureAnalyzer().analyze(failure);
    assertThat(analysis).isNotNull();
    assertThat(analysis.getDescription()).contains("dsA").contains("dsB");
    assertThat(analysis.getAction()).contains("OutboxDataSource").contains("@Primary");
  }

  @Test
  @DisplayName("failure analyzer ignores unrelated failures")
  void analyzerIgnoresUnrelatedFailures() {
    assertThat(new OutboxDataSourceFailureAnalyzer().analyze(new IllegalStateException("boom")))
        .isNull();
  }

  // -------------------------------------------------------------------------------------------
  // fixtures
  // -------------------------------------------------------------------------------------------

  private static ApplicationContextRunner postgresStorageRunner() {
    return new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(PostgresStorageAutoConfiguration.class))
        .withBean(OutboxProperties.class)
        .withBean(Clock.class, Clock::system)
        .withBean(MetricsSnapshotCache.class, MetricsSnapshotCache::noop)
        .withPropertyValues("event-outboxer.storage.type=postgres");
  }

  static final class NoopHandler implements EventHandler<String> {

    @Override
    public String eventType() {
      return "TEST";
    }

    @Override
    public Class<String> payloadType() {
      return String.class;
    }

    @Override
    public EventOutcome handle(EventContext ctx, String payload) {
      return EventOutcome.Success.INSTANCE;
    }
  }

  static final class ResolvedHolder {
    final DataSource dataSource;

    ResolvedHolder(DataSource dataSource) {
      this.dataSource = dataSource;
    }
  }

  static final class LenientHolder {
    final @Nullable DataSource dataSource;

    LenientHolder(@Nullable DataSource dataSource) {
      this.dataSource = dataSource;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class StrictCaptureConfiguration {

    @Bean
    ResolvedHolder resolvedOutboxDataSource(
        @OutboxDataSource ObjectProvider<DataSource> qualified,
        ObjectProvider<DataSource> all,
        ListableBeanFactory beanFactory) {
      return new ResolvedHolder(OutboxDataSourceResolver.resolve(qualified, all, beanFactory));
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class LenientCaptureConfiguration {

    @Bean
    LenientHolder lenientOutboxDataSource(
        @OutboxDataSource ObjectProvider<DataSource> qualified, ObjectProvider<DataSource> all) {
      return new LenientHolder(OutboxDataSourceResolver.resolveIfUnambiguous(qualified, all));
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class TwoPlainDataSourcesConfiguration {

    @Bean
    DataSource dsA() {
      return stubDataSource();
    }

    @Bean
    DataSource dsB() {
      return stubDataSource();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class QualifiedPlusPlainConfiguration {

    @Bean
    @OutboxDataSource
    DataSource outboxDs() {
      return stubDataSource();
    }

    @Bean
    DataSource otherDs() {
      return stubDataSource();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class QualifiedVsPrimaryConfiguration {

    @Bean
    @OutboxDataSource
    DataSource outboxDs() {
      return stubDataSource();
    }

    @Bean
    @Primary
    DataSource primaryDs() {
      return stubDataSource();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class PrimaryPlusPlainConfiguration {

    @Bean
    @Primary
    DataSource primaryDs() {
      return stubDataSource();
    }

    @Bean
    DataSource otherDs() {
      return stubDataSource();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class SingleDataSourceConfiguration {

    @Bean
    DataSource onlyDs() {
      return stubDataSource();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class TwoQualifiedConfiguration {

    @Bean
    @OutboxDataSource
    DataSource qualifiedA() {
      return stubDataSource();
    }

    @Bean
    @OutboxDataSource
    DataSource qualifiedB() {
      return stubDataSource();
    }
  }

  /**
   * Recursive dynamic proxy standing in for a JDBC stack: every interface-returning method returns
   * a further stub, primitives return defaults. Nothing here ever reaches a real database.
   */
  private static DataSource stubDataSource() {
    return (DataSource) stub(DataSource.class);
  }

  private static Object stub(Class<?> type) {
    return Proxy.newProxyInstance(
        OutboxDataSourceSelectionTest.class.getClassLoader(),
        new Class<?>[] {type},
        (proxy, method, args) -> defaultValue(method));
  }

  private static @Nullable Object defaultValue(Method method) {
    Class<?> rt = method.getReturnType();
    if (rt == boolean.class) {
      return false;
    }
    if (rt == long.class) {
      return 0L;
    }
    if (rt == double.class) {
      return 0d;
    }
    if (rt == float.class) {
      return 0f;
    }
    if (rt == short.class) {
      return (short) 0;
    }
    if (rt == byte.class) {
      return (byte) 0;
    }
    if (rt == char.class) {
      return (char) 0;
    }
    if (rt == int.class) {
      return 0;
    }
    if (rt.isInterface()) {
      return stub(rt);
    }
    return null;
  }
}
