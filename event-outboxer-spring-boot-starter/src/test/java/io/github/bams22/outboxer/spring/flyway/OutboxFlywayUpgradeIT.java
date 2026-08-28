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

import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Upgrade path from installations that applied the outbox migrations through the application's
 * Flyway instance (history in {@code public.flyway_schema_history}, ≤ 0.4.0) to the starter-managed
 * instance (ADR-0028): the first boot fails with the baseline recipe, the documented one-time
 * baseline records the existing objects, and later boots need nothing.
 */
@Testcontainers
class OutboxFlywayUpgradeIT {

    @Container
    static PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:15")
                    .withDatabaseName("outboxer")
                    .withUsername("outboxer")
                    .withPassword("outboxer");

    static HikariDataSource dataSource;

    @BeforeAll
    static void legacyInstall() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        dataSource.setMaximumPoolSize(4);

        // What a ≤ 0.4.0 deployment left behind: every outbox migration recorded in the
        // application's own history table, tables present in event_outboxer.
        Flyway.configure()
                .dataSource(dataSource)
                .locations(
                        OutboxFlywayLocations.CORE,
                        OutboxFlywayLocations.ARCHIVE,
                        OutboxFlywayLocations.LOCK)
                .placeholders(Map.of("eventOutboxerSchema", "event_outboxer"))
                .load()
                .migrate();
    }

    @AfterAll
    static void close() {
        dataSource.close();
    }

    @Test
    @DisplayName("first boot explains the baseline; baseline once; subsequent boots are clean")
    void upgradeRecipe() {
        ApplicationContextRunner runner =
                new ApplicationContextRunner()
                        .withConfiguration(
                                AutoConfigurations.of(OutboxFlywayAutoConfiguration.class))
                        // Wrapped so the runner's context close does not shut the shared pool
                        // (Spring infers close() as the destroy method of a HikariDataSource).
                        .withBean(
                                "dataSource",
                                DataSource.class,
                                () -> new DelegatingDataSource(dataSource))
                        .withPropertyValues("event-outboxer.storage.type=postgres");

        runner.run(
                ctx -> {
                    assertThat(ctx).hasFailed();
                    assertThat(ctx.getStartupFailure())
                            .hasStackTraceContaining(
                                    "event-outboxer.flyway.baseline-on-migrate=true")
                            .hasStackTraceContaining("event-outboxer.flyway.baseline-version");
                });

        runner.withPropertyValues(
                        "event-outboxer.flyway.baseline-on-migrate=true",
                        "event-outboxer.flyway.baseline-version=7")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(history())
                                    .containsExactly(Map.of("version", "7", "type", "BASELINE"));
                        });

        runner.run(
                ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(history()).hasSize(1);
                });
    }

    private static List<Map<String, Object>> history() {
        return new JdbcTemplate(dataSource)
                .queryForList(
                        "SELECT version, type FROM event_outboxer.flyway_schema_history"
                                + " ORDER BY installed_rank");
    }
}
