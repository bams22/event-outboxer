/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

class FlywayMigrationIT {

    private static final String SCHEMA = PostgresTestEnvironment.SCHEMA;

    @Test
    void tablesAndPartialIndexesExist() throws Exception {
        try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
                Statement st = conn.createStatement()) {
            assertTableExists(st, SCHEMA, "events");
            assertTableExists(st, SCHEMA, "workers");
            assertTableExists(st, SCHEMA, "event_archive");
            assertColumnExists(st, "event_archive", "dedup_key");
            assertIndexExists(st, "idx_events_ready");
            assertIndexExists(st, "idx_events_processing_by_worker");
            assertIndexExists(st, "idx_events_processing_claimed_at");
            assertIndexExists(st, "idx_workers_heartbeat");
            assertIndexExists(st, "idx_archive_event_type_archived_at");
        }
    }

    @Test
    void rerunningMigrationIsIdempotent() {
        Flyway flyway =
                Flyway.configure()
                        .dataSource(PostgresTestEnvironment.dataSource())
                        .locations(
                                "classpath:event-outboxer/migration/core",
                                "classpath:event-outboxer/migration/archive")
                        .placeholders(Map.of("eventOutboxerSchema", SCHEMA))
                        .load();

        // Second migrate call should be a no-op and succeed.
        flyway.migrate();
        flyway.migrate();
    }

    private static void assertTableExists(Statement st, String schema, String table)
            throws Exception {
        try (ResultSet rs =
                st.executeQuery(
                        "SELECT 1 FROM information_schema.tables WHERE table_schema = '"
                                + schema
                                + "' AND table_name = '"
                                + table
                                + "'")) {
            assertThat(rs.next()).as("table %s.%s must exist", schema, table).isTrue();
        }
    }

    private static void assertColumnExists(Statement st, String table, String column)
            throws Exception {
        try (ResultSet rs =
                st.executeQuery(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema = '"
                                + SCHEMA
                                + "' AND table_name = '"
                                + table
                                + "' AND column_name = '"
                                + column
                                + "'")) {
            assertThat(rs.next()).as("column %s.%s must exist", table, column).isTrue();
        }
    }

    private static void assertIndexExists(Statement st, String indexName) throws Exception {
        try (ResultSet rs =
                st.executeQuery(
                        "SELECT 1 FROM pg_indexes WHERE schemaname = '"
                                + SCHEMA
                                + "' AND indexname = '"
                                + indexName
                                + "'")) {
            assertThat(rs.next()).as("index %s must exist", indexName).isTrue();
        }
    }
}
