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
import io.github.bams22.outboxer.lock.postgres.lease.PgLeaseEntityLocker;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A lane adopted late (ADR-0028): the application ran without {@code
 * event-outboxer-lock-postgres-lease} — core and archive migrations up to V009 applied — and adds
 * the module afterwards. V005 is then lower than the current schema version; the starter's instance
 * runs with {@code outOfOrder}, so it applies instead of failing validation.
 */
@Testcontainers
class OutboxFlywayLateLaneIT {

    @Container
    static PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:15")
                    .withDatabaseName("outboxer")
                    .withUsername("outboxer")
                    .withPassword("outboxer");

    static HikariDataSource dataSource;

    @BeforeAll
    static void pool() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        dataSource.setMaximumPoolSize(4);
    }

    @AfterAll
    static void close() {
        dataSource.close();
    }

    @Test
    @DisplayName("lock lane added after V009 → V005 applies out of order on the next start")
    void lockLaneAdoptedLate() {
        ApplicationContextRunner runner =
                new ApplicationContextRunner()
                        .withConfiguration(
                                AutoConfigurations.of(OutboxFlywayAutoConfiguration.class))
                        .withBean(
                                "dataSource",
                                DataSource.class,
                                () -> new DelegatingDataSource(dataSource))
                        .withPropertyValues("event-outboxer.storage.type=postgres");

        // 1. Deployed without the lease module: the lock lane is not on the classpath.
        runner.withClassLoader(new FilteredClassLoader(PgLeaseEntityLocker.class))
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(versions())
                                    .containsExactly(
                                            "001", "002", "003", "004", "006", "007", "008", "009");
                            assertThat(tables()).doesNotContain("entity_locks");
                        });

        // 2. The lease module is added later: V005 is below the schema's V009.
        runner.run(
                ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(versions())
                            .containsExactly(
                                    "001", "002", "003", "004", "006", "007", "008", "009", "005");
                    assertThat(tables()).contains("entity_locks");
                });

        // 3. Steady state: nothing left to apply.
        runner.run(ctx -> assertThat(versions()).hasSize(9));
    }

    private static List<String> versions() {
        return new JdbcTemplate(dataSource)
                .queryForList(
                        "SELECT version FROM event_outboxer.flyway_schema_history"
                                + " WHERE version IS NOT NULL ORDER BY installed_rank",
                        String.class);
    }

    private static List<String> tables() {
        return new JdbcTemplate(dataSource)
                .queryForList(
                        "SELECT table_name FROM information_schema.tables"
                                + " WHERE table_schema = 'event_outboxer'",
                        String.class);
    }
}
