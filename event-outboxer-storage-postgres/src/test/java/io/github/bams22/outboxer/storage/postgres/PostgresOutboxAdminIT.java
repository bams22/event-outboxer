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

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.spi.contracts.AbstractOutboxAdminContractTest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PostgresOutboxAdminIT extends AbstractOutboxAdminContractTest {

    private PostgresEventStore archiveStore;

    @BeforeEach
    void truncateBetweenTests() {
        PostgresTestEnvironment.truncate();
    }

    @Override
    protected EventStore newStore() {
        return new PostgresEventStore(
                PostgresTestEnvironment.connectionSupplier(),
                PostgresStorageProperties.defaults(),
                Clock.system(),
                MetricsSnapshotCache.noop());
    }

    @Override
    protected OutboxAdmin newAdmin() {
        return new PostgresOutboxAdmin(
                PostgresTestEnvironment.connectionSupplier(), PostgresStorageProperties.defaults());
    }

    // ---------------------------------------------------------------------------------------------
    // archive-specific cases (PostgreSQL-only feature, ADR-0008)
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("findInArchive() returns the archived row after an archive-mode markProcessed")
    void findInArchive_afterArchiveModeFinalize() {
        ClaimedEvent claimed = publishAndClaimArchiveMode("archived-payload");
        assertThat(archiveStore.markProcessed(claimed.id(), WORKER, claimed.claimedVersion()))
                .isTrue();

        Optional<ArchivedEvent> archived = admin.findInArchive(claimed.id());

        assertThat(archived).isPresent();
        assertThat(archived.orElseThrow().eventType()).isEqualTo("ARCH_T");
        assertThat(archived.orElseThrow().archivedBy()).isEqualTo(WORKER.value());
        assertThat(archived.orElseThrow().archivedAt()).isNotNull();
        // Not in the active table any more.
        assertThat(store.findById(claimed.id())).isEmpty();
    }

    @Test
    @DisplayName("purgeArchive() deletes rows archived before the threshold, capped by limit")
    void purgeArchive_respectsThresholdAndLimit() {
        for (int i = 0; i < 4; i++) {
            ClaimedEvent claimed = publishAndClaimArchiveMode("p-" + i);
            assertThat(archiveStore.markProcessed(claimed.id(), WORKER, claimed.claimedVersion()))
                    .isTrue();
        }

        assertThat(admin.purgeArchive(Instant.now().minus(Duration.ofDays(1)), 100)).isZero();
        assertThat(admin.purgeArchive(Instant.now().plus(Duration.ofDays(1)), 3)).isEqualTo(3);
        assertThat(admin.purgeArchive(Instant.now().plus(Duration.ofDays(1)), 100)).isEqualTo(1);
        assertThat(countArchiveRows()).isZero();
    }

    @Test
    @DisplayName("V003 partial index over DISABLED rows exists")
    void disabledIndexExists() {
        try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
                Statement st = conn.createStatement()) {
            var rs =
                    st.executeQuery(
                            "SELECT indexdef FROM pg_indexes WHERE schemaname = '"
                                    + PostgresTestEnvironment.SCHEMA
                                    + "' AND indexname = 'idx_events_disabled_created_at'");
            assertThat(rs.next()).as("idx_events_disabled_created_at must exist").isTrue();
            assertThat(rs.getString(1)).contains("DISABLED");
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private ClaimedEvent publishAndClaimArchiveMode(String payload) {
        if (archiveStore == null) {
            archiveStore =
                    new PostgresEventStore(
                            PostgresTestEnvironment.connectionSupplier(),
                            PostgresStorageProperties.builder()
                                    .schema(PostgresTestEnvironment.SCHEMA)
                                    .tablePrefix("")
                                    .archiveEnabled(true)
                                    .metricsCacheTtl(Duration.ofSeconds(30))
                                    .build(),
                            Clock.system(),
                            MetricsSnapshotCache.noop());
        }
        var p = pending("ARCH_T", payload);
        archiveStore.save(p);
        return archiveStore
                .claim(new io.github.bams22.outboxer.spi.ClaimRequest("ARCH_T", WORKER, 10))
                .stream()
                .filter(ce -> ce.id().equals(p.id()))
                .findFirst()
                .orElseThrow();
    }

    private static int countArchiveRows() {
        try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
                Statement st = conn.createStatement()) {
            var rs =
                    st.executeQuery(
                            "SELECT count(*) FROM "
                                    + PostgresTestEnvironment.SCHEMA
                                    + ".event_archive");
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
