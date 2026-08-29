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
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * {@code event-outboxer.flyway.url} / {@code user} / {@code password} point the outbox instance at
 * a dedicated DDL role while the engine keeps running under the application role (ADR-0028).
 */
@SpringBootTest(
        classes = OutboxFlywayDedicatedConnectionIT.TestApp.class,
        properties = {
            "event-outboxer.storage.type=postgres",
            "event-outboxer.maintenance.shutdown-timeout=2s"
        })
@Testcontainers
class OutboxFlywayDedicatedConnectionIT {

    @Container
    static PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:15")
                    .withDatabaseName("outboxer")
                    .withUsername("outboxer")
                    .withPassword("outboxer")
                    .withInitScript("outbox-flyway-init.sql");

    @DynamicPropertySource
    static void connections(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("event-outboxer.flyway.url", POSTGRES::getJdbcUrl);
        registry.add("event-outboxer.flyway.user", () -> "migrator");
        registry.add("event-outboxer.flyway.password", () -> "migrator");
    }

    @Autowired DataSource dataSource;

    @Test
    @DisplayName("the outbox migrations ran as the dedicated role; the application's as its own")
    void migratedThroughDedicatedRole() {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        assertThat(OutboxFlywayIT.tables(jdbc, "event_outboxer")).contains("events", "workers");
        assertThat(
                        jdbc.queryForList(
                                "SELECT DISTINCT installed_by FROM"
                                        + " event_outboxer.flyway_schema_history",
                                String.class))
                .containsExactly("migrator");
        assertThat(
                        jdbc.queryForList(
                                "SELECT DISTINCT installed_by FROM public.flyway_schema_history",
                                String.class))
                .containsExactly("outboxer");
    }

    @SpringBootConfiguration
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
