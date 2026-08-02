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
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Archive-mode {@code markProcessed} behaviour. The finalize is a single atomic CTE (DELETE ...
 * RETURNING feeding the archive INSERT), so a lost optimistic-lock race must leave zero archive
 * rows — an orphan archive row would permanently block every future {@code markProcessed} of the
 * same event on the archive PK.
 */
class PostgresEventStoreArchiveIT {

    private static final WorkerId WORKER = new WorkerId("archive-worker");

    private PostgresEventStore store;

    @BeforeEach
    void setUp() {
        PostgresTestEnvironment.truncate();
        store =
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

    @Test
    @DisplayName("happy path: markProcessed() moves the row to the archive table")
    void markProcessed_archivesRow() {
        ClaimedEvent claimed = publishAndClaim("payload-1");

        boolean ok = store.markProcessed(claimed.id(), WORKER, claimed.claimedVersion());

        assertThat(ok).isTrue();
        assertThat(store.findById(claimed.id())).isEmpty();
        assertThat(countArchiveRows(claimed.id())).isEqualTo(1);
    }

    @Test
    @DisplayName(
            "lost race (stale version) → false, no orphan archive row, event stays finalizable")
    void markProcessed_lostRace_leavesNoArchiveRow() {
        ClaimedEvent claimed = publishAndClaim("payload-2");

        // Simulate the watchdog / orphan recovery having taken the row back: the caller's version
        // is stale. The guarded DELETE inside the CTE matches nothing, so the archive INSERT must
        // insert nothing — false return, zero archive rows.
        boolean stale = store.markProcessed(claimed.id(), WORKER, claimed.claimedVersion() - 1);

        assertThat(stale).isFalse();
        assertThat(countArchiveRows(claimed.id())).isZero();
        assertThat(store.findById(claimed.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PROCESSING);

        // The event must remain finalizable with the correct version.
        boolean ok = store.markProcessed(claimed.id(), WORKER, claimed.claimedVersion());
        assertThat(ok).isTrue();
        assertThat(countArchiveRows(claimed.id())).isEqualTo(1);
    }

    @Test
    @DisplayName("markProcessedAll() archives winners in one statement; race loser leaves no row")
    void markProcessedAll_archivesWinnersOnly() {
        ClaimedEvent a = publishAndClaim("batch-a");
        ClaimedEvent b = publishAndClaim("batch-b");
        ClaimedEvent stale = publishAndClaim("batch-stale");

        Set<UUID> applied =
                store.markProcessedAll(
                        List.of(
                                new EventStore.ProcessedMark(a.id(), a.claimedVersion()),
                                new EventStore.ProcessedMark(b.id(), b.claimedVersion()),
                                new EventStore.ProcessedMark(
                                        stale.id(), stale.claimedVersion() - 1)),
                        WORKER);

        assertThat(applied).containsExactlyInAnyOrder(a.id(), b.id());
        assertThat(countArchiveRows(a.id())).isEqualTo(1);
        assertThat(countArchiveRows(b.id())).isEqualTo(1);
        // No orphan archive row for the loser — same atomicity guarantee as the single-row CTE.
        assertThat(countArchiveRows(stale.id())).isZero();
        assertThat(store.findById(stale.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PROCESSING);
    }

    @Test
    @DisplayName("binary payload survives the archive byte-exact, format preserved (ADR-0025)")
    void markProcessed_archivesBinaryPayloadByteExact() {
        byte[] raw = new byte[] {0x00, (byte) 0xFF, 0x42, 0x00, (byte) 0x80};
        ClaimedEvent claimed =
                claimOne(
                        PendingEvent.builder()
                                .id(UUID.randomUUID())
                                .eventType("ARCHIVE_BIN")
                                .payload(SerializedPayload.ofBytes(raw))
                                .payloadFormat("test-binary")
                                .payloadClass("java.lang.String")
                                .priority((short) 0)
                                .runAt(Instant.now().minusSeconds(1))
                                .traceContext(Map.of())
                                .build());

        assertThat(store.markProcessed(claimed.id(), WORKER, claimed.claimedVersion())).isTrue();

        PostgresOutboxAdmin admin =
                new PostgresOutboxAdmin(
                        PostgresTestEnvironment.connectionSupplier(),
                        PostgresStorageProperties.builder()
                                .schema(PostgresTestEnvironment.SCHEMA)
                                .tablePrefix("")
                                .archiveEnabled(true)
                                .metricsCacheTtl(Duration.ofSeconds(30))
                                .build());
        ArchivedEvent archived = admin.findInArchive(claimed.id()).orElseThrow();
        assertThat(archived.payload().isText()).isFalse();
        assertThat(archived.payload().requireBytes()).isEqualTo(raw);
        assertThat(archived.payloadFormat()).isEqualTo("test-binary");
    }

    @Test
    @DisplayName("release() after claim leaves no archive row and the event is re-claimable")
    void release_leavesNoArchiveRow() {
        ClaimedEvent claimed = publishAndClaim("payload-3");

        boolean released =
                store.release(
                        claimed.id(),
                        WORKER,
                        claimed.claimedVersion(),
                        "backpressure",
                        Instant.now());

        assertThat(released).isTrue();
        assertThat(countArchiveRows(claimed.id())).isZero();
        assertThat(store.findById(claimed.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PENDING);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private ClaimedEvent publishAndClaim(String payload) {
        return claimOne(
                PendingEvent.builder()
                        .id(UUID.randomUUID())
                        .eventType("ARCHIVE_T")
                        .payload(SerializedPayload.ofText("\"" + payload + "\""))
                        .payloadFormat("test-json")
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(Instant.now().minusSeconds(1))
                        .traceContext(Map.of())
                        .build());
    }

    private ClaimedEvent claimOne(PendingEvent p) {
        store.save(p);
        List<ClaimedEvent> claimed = store.claim(new ClaimRequest(p.eventType(), WORKER, 10));
        return claimed.stream()
                .filter(ce -> ce.id().equals(p.id()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("event not claimed: " + p.id()));
    }

    private static int countArchiveRows(UUID id) {
        String sql =
                "SELECT count(*) FROM "
                        + PostgresTestEnvironment.SCHEMA
                        + ".event_archive WHERE id = '"
                        + id
                        + "'";
        try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new IllegalStateException("failed to count archive rows", e);
        }
    }
}
