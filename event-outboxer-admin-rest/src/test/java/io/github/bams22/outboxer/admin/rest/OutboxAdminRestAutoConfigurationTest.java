/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.admin.rest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryOutboxAdmin;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;

/**
 * Registration and the security fail-fast of the admin REST auto-configuration. Spring Security
 * IS on the test classpath, so with method security absent the context must refuse to start.
 */
class OutboxAdminRestAutoConfigurationTest {

  private final WebApplicationContextRunner runner =
      new WebApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(OutboxAdminRestAutoConfiguration.class))
          .withUserConfiguration(AdminBeans.class);

  @Test
  @DisplayName("disabled by default: no controller bean")
  void disabledByDefault() {
    runner.run(ctx -> assertThat(ctx).doesNotHaveBean(OutboxAdminController.class));
  }

  @Test
  @DisplayName("enabled + security on classpath + no method security → context fails fast")
  void failsFastWithoutMethodSecurity() {
    runner
        .withPropertyValues("event-outboxer.admin.rest.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("@EnableMethodSecurity");
            });
  }

  @Test
  @DisplayName("enabled + method security active → controller registered")
  void startsWithMethodSecurity() {
    runner
        .withUserConfiguration(MethodSecurityConfig.class)
        .withPropertyValues(
            "event-outboxer.admin.rest.enabled=true",
            "event-outboxer.admin.rest.required-authority=MY_PERMIT")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(OutboxAdminController.class);
              assertThat(ctx.getBean("outboxAdminRestProperties", OutboxAdminRestProperties.class)
                      .getRequiredAuthority())
                  .isEqualTo("MY_PERMIT");
            });
  }

  @Test
  @DisplayName("enabled + enforce-authority=false → starts without method security (explicit opt-out)")
  void startsWithExplicitOptOut() {
    runner
        .withPropertyValues(
            "event-outboxer.admin.rest.enabled=true",
            "event-outboxer.admin.rest.enforce-authority=false")
        .run(ctx -> assertThat(ctx).hasSingleBean(OutboxAdminController.class));
  }

  @Configuration
  static class AdminBeans {

    @Bean
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Bean
    OutboxAdmin outboxAdmin(EventStore store) {
      return new InMemoryOutboxAdmin((InMemoryEventStore) store);
    }
  }

  @Configuration
  @EnableMethodSecurity
  static class MethodSecurityConfig {}
}
