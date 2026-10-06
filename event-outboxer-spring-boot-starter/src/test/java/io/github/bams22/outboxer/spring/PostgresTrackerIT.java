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

import io.github.bams22.outboxer.api.track.AwaitResult;
import io.github.bams22.outboxer.api.track.TrackedState;
import io.github.bams22.outboxer.core.engine.OutboxEngine;
import java.time.Duration;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * {@link PostgresTrackerITBase} with the archive off — and without the archive table, as for an
 * application that manages the schema itself and never applied the optional archive DDL
 * (STORAGE.md). The tracker must not touch that table when the archive is disabled.
 */
@Testcontainers
class PostgresTrackerIT extends PostgresTrackerITBase {

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
        registry.add("event-outboxer.storage.archive-enabled", () -> "false");
    }

    @Autowired DataSource dataSource;
    @Autowired OutboxEngine engine;

    @BeforeEach
    void dropArchiveTable() {
        new JdbcTemplate(dataSource).execute("DROP TABLE IF EXISTS event_outboxer.event_archive");
    }

    @Override
    boolean archiveEnabled() {
        return false;
    }

    @Test
    void engineTrackerAlsoSkipsTheMissingArchive() {
        UUID id = service.publish(new Tracked("engine-tracker", false));

        assertThat(engine.tracker().await(id, Duration.ofSeconds(10)))
                .isEqualTo(new AwaitResult.Completed(id, null));
        assertThat(engine.tracker().state(id)).isEqualTo(TrackedState.ABSENT);
    }
}
