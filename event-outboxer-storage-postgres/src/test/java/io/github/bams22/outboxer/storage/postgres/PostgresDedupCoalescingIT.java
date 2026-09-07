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

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.CoalescedEvent;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The transactional half of claim-time coalescing (ADR-0037) on real PostgreSQL — what the contract
 * tests cannot express without transactions: an uncommitted twin is invisible to the sweep and runs
 * on its own after commit (the ADR-0021 visibility guarantee, now without a pin), a twin another
 * worker's claim holds locked is skipped rather than waited on, and with archiving on the swept
 * duplicates land in the archive marked as coalesced.
 */
class PostgresDedupCoalescingIT {

    private static final WorkerId WORKER = new WorkerId("dedup-it");
    private static final WorkerId OTHER = new WorkerId("dedup-it-other");
    private static final String TYPE = "SYNC_ORDER";
    private static final String EVENTS = PostgresTestEnvironment.SCHEMA + ".events";

    private EventStore store;
    private EventStore archivingStore;
    private Connection manualTx;

    @BeforeEach
    void setUp() throws SQLException {
        PostgresTestEnvironment.truncate();
        store = storeOver(PostgresTestEnvironment.connectionSupplier(), false);
        archivingStore = storeOver(PostgresTestEnvironment.connectionSupplier(), true);
        manualTx = PostgresTestEnvironment.dataSource().getConnection();
        manualTx.setAutoCommit(false);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (manualTx != null && !manualTx.isClosed()) {
            manualTx.rollback();
            manualTx.close();
        }
    }

    @Test
    @DisplayName("an uncommitted twin is invisible to the sweep and runs on its own after commit")
    void uncommittedTwinIsNotSwept() throws SQLException {
        PendingEvent committed = due("v1", "8", 3);
        store.save(committed);
        // The application transaction: every statement of this store runs on the single manual
        // connection, exactly like ConnectionSupplier -> DataSourceUtils in the starter.
        EventStore txStore = storeOver(single(manualTx), false);
        PendingEvent inFlight = due("v2", "8", 2);
        txStore.save(inFlight);

        // THE CRUX: the claim sees only the committed row. Sweeping the in-flight twin would cover
        // a publish whose transaction has not committed — the handler could run against a snapshot
        // without its changes.
        assertThat(store.claim(new ClaimRequest(TYPE, WORKER, 10)))
                .singleElement()
                .satisfies(
                        ce -> {
                            assertThat(ce.id()).isEqualTo(committed.id());
                            assertThat(ce.coalesced()).isEmpty();
                        });
        manualTx.commit();

        // After the commit it is a row of its own and runs.
        assertThat(store.claim(new ClaimRequest(TYPE, OTHER, 10)))
                .singleElement()
                .satisfies(ce -> assertThat(ce.id()).isEqualTo(inFlight.id()));
    }

    @Test
    @DisplayName("a twin locked by another worker's claim is skipped, never waited on")
    void lockedTwinIsSkipped() throws SQLException {
        PendingEvent first = due("v1", "5", 3);
        PendingEvent second = due("v2", "5", 2);
        store.save(first);
        store.save(second);
        // Another worker is mid-claim on the second row: its FOR UPDATE lock is held.
        try (PreparedStatement ps =
                manualTx.prepareStatement(
                        "SELECT id FROM " + EVENTS + " WHERE id = ? FOR UPDATE")) {
            ps.setObject(1, second.id());
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
            }
        }

        assertThat(store.claim(new ClaimRequest(TYPE, WORKER, 10)))
                .singleElement()
                .satisfies(
                        ce -> {
                            assertThat(ce.id()).isEqualTo(first.id());
                            assertThat(ce.coalesced()).isEmpty();
                        });
        assertThat(status(second.id())).isEqualTo(EventStatus.PENDING);
    }

    @Test
    @DisplayName("the sweep is capped per claim; the remainder is swept by the next claim")
    void sweepIsCappedPerClaim() {
        int total = PostgresEventStore.CLAIM_DEDUP_SWEEP_CAP + 50;
        Instant base = Instant.now().minusSeconds(2 * total);
        PendingEvent first = pending("v0", "cap", base);
        store.save(first);
        for (int i = 1; i < total; i++) {
            store.save(pending("v" + i, "cap", base.plusSeconds(i)));
        }

        ClaimedEvent representative = store.claim(new ClaimRequest(TYPE, WORKER, 10)).getFirst();

        assertThat(representative.id()).isEqualTo(first.id());
        assertThat(representative.coalesced()).hasSize(PostgresEventStore.CLAIM_DEDUP_SWEEP_CAP);
        assertThat(rowCount()).isEqualTo(1 + 49);
        // The next claim of the type takes the oldest survivor and sweeps the rest.
        assertThat(store.claim(new ClaimRequest(TYPE, OTHER, 10)))
                .singleElement()
                .satisfies(ce -> assertThat(ce.coalesced()).hasSize(48));
    }

    @Test
    @DisplayName("with archiving on, swept duplicates land in the archive marked as coalesced")
    void archivingStoreArchivesSweptDuplicates() throws SQLException {
        PendingEvent keep = due("v1", "6", 3);
        PendingEvent dup = due("v2", "6", 2);
        archivingStore.save(keep);
        archivingStore.save(dup);

        assertThat(archivingStore.claim(new ClaimRequest(TYPE, WORKER, 10)))
                .singleElement()
                .satisfies(
                        ce -> {
                            assertThat(ce.id()).isEqualTo(keep.id());
                            assertThat(ce.coalesced())
                                    .extracting(CoalescedEvent::id)
                                    .containsExactly(dup.id());
                        });
        assertThat(rowCount()).isEqualTo(1);
        try (Connection c = PostgresTestEnvironment.dataSource().getConnection();
                PreparedStatement ps =
                        c.prepareStatement(
                                "SELECT last_fail_reason, dedup_key, archived_by FROM "
                                        + PostgresTestEnvironment.SCHEMA
                                        + ".event_archive WHERE id = ?")) {
            ps.setObject(1, dup.id());
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo(PostgresEventStore.COALESCED_REASON);
                assertThat(rs.getString(2)).isEqualTo("6");
                assertThat(rs.getString(3)).isEqualTo(WORKER.value());
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static EventStore storeOver(ConnectionSupplier connections, boolean archive) {
        return new PostgresEventStore(
                connections,
                PostgresStorageProperties.defaults().toBuilder().archiveEnabled(archive).build(),
                Clock.system(),
                MetricsSnapshotCache.noop());
    }

    private static ConnectionSupplier single(Connection connection) {
        return new ConnectionSupplier() {
            @Override
            public Connection get() {
                return connection;
            }

            @Override
            public void release(Connection ignored) {
                // owned by the test
            }
        };
    }

    private static PendingEvent due(String payload, String key, int secondsAgo) {
        return pending(payload, key, Instant.now().minusSeconds(secondsAgo));
    }

    private static PendingEvent pending(String payload, String key, Instant runAt) {
        return PendingEvent.builder()
                .id(UUID.randomUUID())
                .eventType(TYPE)
                .payload(SerializedPayload.ofText("\"" + payload + "\""))
                .payloadFormat("test-json")
                .payloadClass("java.lang.String")
                .priority((short) 0)
                .runAt(runAt)
                .traceContext(Map.of())
                .dedupKey(key)
                .build();
    }

    private EventStatus status(UUID id) {
        return store.findById(id).orElseThrow().status();
    }

    private static long rowCount() {
        try (Connection c = PostgresTestEnvironment.dataSource().getConnection();
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM " + EVENTS)) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
