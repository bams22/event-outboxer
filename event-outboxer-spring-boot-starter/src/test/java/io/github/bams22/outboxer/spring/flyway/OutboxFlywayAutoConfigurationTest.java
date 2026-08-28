/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.flyway;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.spring.OutboxDataSource;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wiring tests without a database: the stub {@link DataSource} refuses connections, so a context
 * that reaches {@code flyway.migrate()} fails inside Flyway — which is exactly the proof that the
 * initializer was registered and ran.
 */
class OutboxFlywayAutoConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(OutboxFlywayAutoConfiguration.class))
                    .withPropertyValues("event-outboxer.storage.type=postgres");

    @Test
    @DisplayName("with a DataSource and postgres storage the initializer runs the migrations")
    void activeByDefault() {
        runner.withUserConfiguration(StubDataSourceConfiguration.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasRootCauseMessage("stub DataSource must not be connected");
                            assertThat(ctx.getStartupFailure())
                                    .hasStackTraceContaining("Unable to obtain connection");
                        });
    }

    @Test
    @DisplayName("event-outboxer.flyway.enabled=false backs off")
    void disabledBySwitch() {
        runner.withUserConfiguration(StubDataSourceConfiguration.class)
                .withPropertyValues("event-outboxer.flyway.enabled=false")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).doesNotHaveBean(OutboxFlywayMigrationInitializer.class);
                        });
    }

    @Test
    @DisplayName("the outbox master switch also disables the migrations")
    void disabledByMasterSwitch() {
        runner.withUserConfiguration(StubDataSourceConfiguration.class)
                .withPropertyValues("event-outboxer.enabled=false")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).doesNotHaveBean(OutboxFlywayMigrationInitializer.class);
                        });
    }

    @Test
    @DisplayName("nothing to migrate unless storage.type=postgres")
    void requiresPostgresStorage() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OutboxFlywayAutoConfiguration.class))
                .withUserConfiguration(StubDataSourceConfiguration.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).doesNotHaveBean(OutboxFlywayMigrationInitializer.class);
                        });
    }

    @Test
    @DisplayName("without Flyway on the classpath the auto-configuration is inert")
    void requiresFlyway() {
        runner.withUserConfiguration(StubDataSourceConfiguration.class)
                .withClassLoader(new FilteredClassLoader(Flyway.class))
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).doesNotHaveBean("outboxFlywayMigrationInitializer");
                        });
    }

    @Test
    @DisplayName("no DataSource and no url: back off, the storage analyzer explains the DataSource")
    void requiresConnection() {
        runner.run(
                ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx).doesNotHaveBean(OutboxFlywayMigrationInitializer.class);
                });
    }

    @Test
    @DisplayName("a dedicated url activates the instance even without a DataSource bean")
    void dedicatedUrlWithoutDataSource() {
        runner.withPropertyValues(
                        "event-outboxer.flyway.url=jdbc:postgresql://127.0.0.1:1/unreachable",
                        "event-outboxer.flyway.user=migrator")
                .run(
                        ctx -> {
                            // The URL is unreachable, so the context fails inside migrate() — the
                            // point is that the initializer was wired at all.
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasStackTraceContaining(
                                            OutboxFlywayMigrationInitializer.class.getName());
                        });
    }

    @Test
    @DisplayName("the @OutboxDataSource-qualified bean wins over an unqualified one")
    void qualifiedDataSourceWins() {
        runner.withUserConfiguration(TwoDataSourcesConfiguration.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasRootCauseMessage("outbox DataSource selected");
                        });
    }

    @Test
    @DisplayName("a user-defined initializer bean replaces the auto-configured one")
    void userInitializerWins() {
        OutboxFlywayMigrationInitializer custom =
                new OutboxFlywayMigrationInitializer(
                        Flyway.configure()
                                .dataSource(OutboxFlywayFactoryTest.stubDataSource())
                                .load(),
                        "custom") {
                    @Override
                    public void afterPropertiesSet() {
                        // no-op: the user owns the migration
                    }
                };
        runner.withUserConfiguration(StubDataSourceConfiguration.class)
                .withBean("myOutboxFlyway", OutboxFlywayMigrationInitializer.class, () -> custom)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(OutboxFlywayMigrationInitializer.class))
                                    .isSameAs(custom);
                        });
    }

    @Configuration(proxyBeanMethods = false)
    static class StubDataSourceConfiguration {

        @Bean
        DataSource dataSource() {
            return OutboxFlywayFactoryTest.stubDataSource();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoDataSourcesConfiguration {

        @Bean
        DataSource reportingDataSource() {
            return OutboxFlywayFactoryTest.stubDataSource();
        }

        @Bean
        @OutboxDataSource
        DataSource ordersDataSource() {
            return OutboxFlywayFactoryTest.stubDataSource("outbox DataSource selected");
        }
    }
}
