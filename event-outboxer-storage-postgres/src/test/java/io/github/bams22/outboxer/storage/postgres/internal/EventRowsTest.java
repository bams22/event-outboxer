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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit-level guards for the shared column lists. What these constants must agree with — the
 * migrated schema, and the mappers that read a row back — is checked against a real database by
 * {@code EventRowsSchemaIT}; what is checkable without one is here: the arity that the hand-written
 * placeholder lists depend on, and the input {@link EventRows#qualified} accepts.
 */
class EventRowsTest {

    @Test
    @DisplayName("qualified() prefixes every column of the list with the alias")
    void qualified_prefixesEveryColumn() {
        assertThat(EventRows.qualified("e", "id, event_type,dedup_key"))
                .isEqualTo("e.id, e.event_type, e.dedup_key");
        assertThat(EventRows.qualified("e", EventRows.IMMUTABLE_COLUMNS))
                .startsWith("e.id, e.event_type, e.payload, e.payload_binary")
                .endsWith("e.trace_context, e.dedup_key")
                .doesNotContain(", id")
                .doesNotContain(",dedup_key");
    }

    @Test
    @DisplayName("qualified() refuses anything that is not a plain column identifier")
    void qualified_rejectsExpressions() {
        // Splitting on a bare comma would turn these into malformed SQL that only fails when the
        // statement reaches PostgreSQL.
        assertThatThrownBy(() -> EventRows.qualified("e", "trace_context::text"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trace_context::text");
        assertThatThrownBy(() -> EventRows.qualified("e", "id, coalesce(a, b)"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventRows.qualified("e", "id AS event_id"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("the column count of each list is pinned: adding a column is a four-step change")
    void lists_haveTheExpectedArity() {
        // A tripwire, not a description. Changing one of these numbers is the moment to also:
        //   1. add the placeholder to PostgresEventStore.sqlInsert's hand-written VALUES list and
        //      bind it in bindPending() — nothing else fails loudly if you forget;
        //   2. add the column to both tables in the migrations (V001/V002);
        //   3. read it in the mapper below the constants.
        // EventRowsSchemaIT then confirms 2 and 3 against a migrated database.
        assertThat(columnsOf(EventRows.IMMUTABLE_COLUMNS))
                .as("columns set at publish and never updated")
                .hasSize(9);
        assertThat(columnsOf(EventRows.EVENT_COLUMNS)).as("every events column").hasSize(17);
        assertThat(columnsOf(EventRows.ARCHIVE_COPY_COLUMNS))
                .as("the part of an events row the archive keeps")
                .hasSize(13);
        assertThat(columnsOf(EventRows.ARCHIVE_COLUMNS))
                .as("every event_archive column")
                .hasSize(15);
    }

    @Test
    @DisplayName("every list is made of distinct plain identifiers")
    void lists_holdDistinctIdentifiers() {
        for (String list :
                new String[] {
                    EventRows.IMMUTABLE_COLUMNS,
                    EventRows.EVENT_COLUMNS,
                    EventRows.ARCHIVE_COPY_COLUMNS,
                    EventRows.ARCHIVE_COLUMNS
                }) {
            assertThat(columnsOf(list))
                    .doesNotHaveDuplicates()
                    .allMatch(EventRowsTest::isIdentifier);
        }
    }

    private static String[] columnsOf(String list) {
        return list.split("\\s*,\\s*");
    }

    private static boolean isIdentifier(String column) {
        return column.matches("[a-z_][a-z0-9_]*");
    }
}
