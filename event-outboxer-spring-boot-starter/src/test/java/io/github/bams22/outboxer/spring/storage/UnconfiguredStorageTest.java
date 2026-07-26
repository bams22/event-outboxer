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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spring.OutboxEngineAutoConfiguration;
import io.github.bams22.outboxer.spring.serializer.JacksonSerializerAutoConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mock.env.MockEnvironment;

/**
 * ADR-0020: there is no default storage and no in-memory fallback. An unconfigured outbox must
 * fail at startup, and the failure analyzer must turn the raw NoSuchBeanDefinitionException
 * into an actionable message.
 */
class UnconfiguredStorageTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  JacksonSerializerAutoConfiguration.class,
                  io.github.bams22.outboxer.spring.lock.NoOpLockAutoConfiguration.class,
                  OutboxEngineAutoConfiguration.class))
          .withUserConfiguration(HandlerOnly.class);

  @Test
  @DisplayName("no storage configured → context fails on the missing EventStore")
  void unconfiguredStorageFailsStartup() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasFailed();
          assertThat(ctx.getStartupFailure())
              .hasStackTraceContaining("EventStore");
        });
  }

  @Test
  @DisplayName("explicit @Import of the in-memory test configuration starts the engine")
  void importedTestConfigurationStarts() {
    runner
        .withUserConfiguration(OutboxInMemoryTestConfiguration.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(EventStore.class);
              assertThat(ctx)
                  .hasSingleBean(io.github.bams22.outboxer.core.engine.OutboxEngine.class);
            });
  }

  @Test
  @DisplayName("failure analyzer: type unset → points at storage.type and the test import")
  void analyzerForUnsetType() {
    OutboxStorageFailureAnalyzer analyzer = new OutboxStorageFailureAnalyzer();
    analyzer.setEnvironment(new MockEnvironment());

    FailureAnalysis analysis =
        analyzer.analyze(new NoSuchBeanDefinitionException(EventStore.class));

    assertThat(analysis).isNotNull();
    assertThat(analysis.getDescription()).contains("event-outboxer.storage.type");
    assertThat(analysis.getAction())
        .contains("postgres")
        .contains("OutboxInMemoryTestConfiguration");
  }

  @Test
  @DisplayName("failure analyzer: type=postgres without DataSource → DataSource hint")
  void analyzerForMissingDataSource() {
    OutboxStorageFailureAnalyzer analyzer = new OutboxStorageFailureAnalyzer();
    analyzer.setEnvironment(
        new MockEnvironment().withProperty("event-outboxer.storage.type", "postgres"));

    FailureAnalysis analysis =
        analyzer.analyze(new NoSuchBeanDefinitionException(EventStore.class));

    assertThat(analysis).isNotNull();
    assertThat(analysis.getAction()).contains("DataSource");
  }

  @Test
  @DisplayName("failure analyzer ignores unrelated failures")
  void analyzerIgnoresOtherFailures() {
    OutboxStorageFailureAnalyzer analyzer = new OutboxStorageFailureAnalyzer();
    analyzer.setEnvironment(new MockEnvironment());

    assertThat(analyzer.analyze(new NoSuchBeanDefinitionException(String.class))).isNull();
    assertThat(analyzer.analyze(new IllegalStateException("boom"))).isNull();
  }

  @Configuration
  static class HandlerOnly {

    @Bean
    EventHandler<String> handler() {
      return new EventHandler<String>() {
        @Override
        public String eventType() {
          return "T";
        }

        @Override
        public Class<String> payloadType() {
          return String.class;
        }

        @Override
        public EventOutcome handle(EventContext ctx, String payload) {
          return EventOutcome.Success.INSTANCE;
        }
      };
    }
  }
}
