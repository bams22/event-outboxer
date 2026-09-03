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

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.lock.postgres.lease.PgLeaseEntityLocker;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The starter-managed Flyway instance (ADR-0028) next to the application's own: the application
 * ships {@code db/migration/V001__app_orders.sql} (same version number as the library's V001), the
 * starter applies every outbox lane through its own history table, and neither instance notices the
 * other.
 */
@SpringBootTest(
        classes = OutboxFlywayIT.TestApp.class,
        properties = {
            "event-outboxer.storage.type=postgres",
            "event-outboxer.lock.type=postgres-lease",
            "event-outboxer.maintenance.shutdown-timeout=2s"
        })
@Testcontainers
class OutboxFlywayIT {

    @Container
    static PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:15")
                    .withDatabaseName("outboxer")
                    .withUsername("outboxer")
                    .withPassword("outboxer");

    @DynamicPropertySource
    static void dataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired ApplicationContext context;
    @Autowired DataSource dataSource;

    @Test
    @DisplayName("every shipped lane is applied through the outbox's own history table")
    void outboxSchemaMigrated() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        assertThat(tables(jdbc, "event_outboxer"))
                .contains("events", "workers", "event_archive", "entity_locks");
        assertThat(versions(jdbc, "event_outboxer"))
                .containsExactly("001", "002", "003", "004", "005", "006", "007", "008", "009");
        assertThat(context.getBean(OutboxFlywayMigrationInitializer.class)).isNotNull();
        // The lease probe is @DependsOnDatabaseInitialization: it only passed because the
        // detector ordered it after the outbox migrations.
        assertThat(context.getBean(PgLeaseEntityLocker.class)).isNotNull();
    }

    @Test
    @DisplayName(
            "the application's Flyway instance sees only db/migration, despite V001 on both sides")
    void applicationInstanceUntouched() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        assertThat(tables(jdbc, "public")).contains("orders").doesNotContain("events", "workers");
        assertThat(versions(jdbc, "public")).containsExactly("001");
        assertThat(
                        jdbc.queryForList(
                                "SELECT description FROM public.flyway_schema_history",
                                String.class))
                .containsExactly("app orders");
    }

    static List<String> tables(JdbcTemplate jdbc, String schema) {
        return jdbc.queryForList(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                String.class,
                schema);
    }

    static List<String> versions(JdbcTemplate jdbc, String schema) {
        return jdbc.queryForList(
                "SELECT version FROM "
                        + schema
                        + ".flyway_schema_history WHERE version IS NOT NULL ORDER BY"
                        + " installed_rank",
                String.class);
    }

    @SpringBootConfiguration
    // liquibase-core leaks onto the test classpath as an optional dep of the starter; without a
    // master changelog Boot's LiquibaseAutoConfiguration would fail the context.
    @EnableAutoConfiguration(exclude = LiquibaseAutoConfiguration.class)
    static class TestApp {

        @Bean
        NoopHandler noopHandler() {
            return new NoopHandler();
        }
    }

    /** The engine refuses to start without a handler; the tests only care about the schema. */
    static final class NoopHandler implements EventHandler<String> {
        @Override
        public EventType<String> type() {
            return EventType.of("NOOP", String.class);
        }

        @Override
        public EventOutcome handle(EventContext ctx, String payload) {
            return EventOutcome.success();
        }
    }
}
