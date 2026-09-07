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
import org.flywaydb.core.api.MigrationVersion;
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
 * baseline records the existing objects <em>and still applies the migrations the running jar adds
 * on top of them</em>, and later boots need nothing.
 *
 * <p>The fixture is frozen at the 0.4.0 migration set (V001…V007) rather than migrated with the
 * current one. Migrating with the current set would leave nothing for the upgrade to apply, and the
 * baseline assertions would hold for any baseline version — including one high enough to skip the
 * newest migration, the failure this test exists to catch.
 */
@Testcontainers
class OutboxFlywayUpgradeIT {

    @Container
    static PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:15")
                    .withDatabaseName("outboxer")
                    .withUsername("outboxer")
                    .withPassword("outboxer");

    /** Highest outbox migration a ≤ 0.4.0 install applied — the documented baseline version. */
    private static final String LEGACY_HIGHEST_VERSION = "7";

    static HikariDataSource dataSource;

    @BeforeAll
    static void legacyInstall() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        dataSource.setMaximumPoolSize(4);

        // What a ≤ 0.4.0 deployment left behind: the outbox migrations that shipped back then
        // (V001…V007) recorded in the application's own history table, tables present in
        // event_outboxer. target=7 freezes the fixture there — everything the running jar adds on
        // top (V008, V009) must be left for the upgrade to apply.
        Flyway.configure()
                .dataSource(dataSource)
                .locations(
                        OutboxFlywayLocations.CORE,
                        OutboxFlywayLocations.ARCHIVE,
                        OutboxFlywayLocations.LOCK)
                .target(MigrationVersion.fromVersion(LEGACY_HIGHEST_VERSION))
                .placeholders(Map.of("eventOutboxerSchema", "event_outboxer"))
                .load()
                .migrate();

        assertThat(archiveColumns()).doesNotContain("dedup_key");
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

        // Baseline at the highest migration the legacy install actually applied, never higher:
        // Flyway treats everything at or below the baseline as done, so baselining at 8 here would
        // skip V008 forever and break every archive-mode markProcessed at runtime.
        runner.withPropertyValues(
                        "event-outboxer.flyway.baseline-on-migrate=true",
                        "event-outboxer.flyway.baseline-version=" + LEGACY_HIGHEST_VERSION)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(history())
                                    .containsExactly(
                                            Map.of("version", "7", "type", "BASELINE"),
                                            // Flyway records an applied migration under the
                                            // version as written in its filename ("008"); the
                                            // BASELINE row carries the configured property value.
                                            Map.of("version", "008", "type", "SQL"),
                                            Map.of("version", "009", "type", "SQL"));
                            // The whole point of the recipe: the migration the running jar needs
                            // is applied on top of the baseline, not hidden by it.
                            assertThat(archiveColumns()).contains("dedup_key");
                        });

        runner.run(
                ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(history()).hasSize(3);
                });
    }

    private static List<String> archiveColumns() {
        return new JdbcTemplate(dataSource)
                .queryForList(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema ="
                                + " 'event_outboxer' AND table_name = 'event_archive'",
                        String.class);
    }

    private static List<Map<String, Object>> history() {
        return new JdbcTemplate(dataSource)
                .queryForList(
                        "SELECT version, type FROM event_outboxer.flyway_schema_history"
                                + " ORDER BY installed_rank");
    }
}
