/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.storage.postgres.PostgresTestEnvironment;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Checks the shared column lists against the schema the migrations actually produce. Every other
 * user of {@link EventRows} builds both sides of its statement from the same constant, so a typo or
 * a column that exists in the constant but not in the table stays invisible until that statement
 * runs — which, for the archive copies, means "only once archiving is enabled". Comparing the lists
 * with {@code information_schema.columns} moves that failure to the build.
 */
class EventRowsSchemaIT {

    @Test
    @DisplayName("EVENT_COLUMNS names exactly the columns of the events table")
    void eventColumns_matchTheMigratedTable() throws SQLException {
        assertThat(columnsOf(EventRows.EVENT_COLUMNS))
                .containsExactlyInAnyOrderElementsOf(tableColumns("events"));
    }

    @Test
    @DisplayName("ARCHIVE_COLUMNS names exactly the columns of the event_archive table")
    void archiveColumns_matchTheMigratedTable() throws SQLException {
        assertThat(columnsOf(EventRows.ARCHIVE_COLUMNS))
                .containsExactlyInAnyOrderElementsOf(tableColumns("event_archive"));
    }

    @Test
    @DisplayName("the immutable columns exist in both tables, so the archive copies can move them")
    void immutableColumns_existInBothTables() throws SQLException {
        Set<String> immutable = columnsOf(EventRows.IMMUTABLE_COLUMNS);

        assertThat(tableColumns("events")).containsAll(immutable);
        assertThat(tableColumns("event_archive")).containsAll(immutable);
    }

    private static Set<String> columnsOf(String list) {
        return new LinkedHashSet<>(List.of(list.split("\\s*,\\s*")));
    }

    private static Set<String> tableColumns(String table) throws SQLException {
        String sql =
                "SELECT column_name FROM information_schema.columns"
                        + " WHERE table_schema = ? AND table_name = ?";
        Set<String> columns = new LinkedHashSet<>();
        try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, PostgresTestEnvironment.SCHEMA);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("column_name"));
                }
            }
        }
        assertThat(columns).as("columns of %s (is the migration applied?)", table).isNotEmpty();
        return columns;
    }
}
