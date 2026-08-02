/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.postgres.advisory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.contracts.AbstractEntityLockerContractTest;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

class PgAdvisoryLockerIT extends AbstractEntityLockerContractTest {

    private static PostgreSQLContainer<?> container;
    private static HikariDataSource dataSource;

    @BeforeAll
    static void boot() {
        container =
                new PostgreSQLContainer<>("postgres:15")
                        .withDatabaseName("lockerit")
                        .withUsername("lockerit")
                        .withPassword("lockerit");
        container.start();

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(container.getJdbcUrl());
        cfg.setUsername(container.getUsername());
        cfg.setPassword(container.getPassword());
        // Pool >= threads used by the contract's concurrent exclusivity test.
        cfg.setMaximumPoolSize(64);
        dataSource = new HikariDataSource(cfg);
    }

    @AfterAll
    static void shutdown() {
        if (dataSource != null) {
            dataSource.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    @Override
    protected EntityLocker newLocker() {
        return new PgAdvisoryLocker(dataSource());
    }

    private static DataSource dataSource() {
        return dataSource;
    }
}
