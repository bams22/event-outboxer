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

import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The crux of ADR-0021 on real PostgreSQL: a publisher transaction that coalesces into an existing
 * PENDING event pins that row with {@code SELECT ... FOR UPDATE}, so the claim query ({@code FOR
 * UPDATE SKIP LOCKED}) must skip it until the publisher commits — guaranteeing the handler sees the
 * coalesced transaction's data. Without the pin this scenario is a lost update: the old event would
 * be processed against a pre-commit snapshot, and the coalesced publish would never produce another
 * event.
 */
class PostgresDedupCoalescingIT {

    private static final WorkerId WORKER = new WorkerId("dedup-it");
    private static final String TYPE = "SYNC_ORDER";

    private EventStore pooledStore;
    private Connection publisherTx;
    private EventStore publisherTxStore;

    @BeforeEach
    void setUp() throws SQLException {
        PostgresTestEnvironment.truncate();
        pooledStore = storeOver(PostgresTestEnvironment.connectionSupplier());
        publisherTx = PostgresTestEnvironment.dataSource().getConnection();
        publisherTx.setAutoCommit(false);
        // Simulates the application transaction: every statement of this store runs on the single
        // manual connection, exactly like ConnectionSupplier -> DataSourceUtils in the starter.
        publisherTxStore =
                storeOver(
                        new ConnectionSupplier() {
                            @Override
                            public Connection get() {
                                return publisherTx;
                            }

                            @Override
                            public void release(Connection connection) {
                                // owned by the test — released in tearDown
                            }
                        });
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (publisherTx != null && !publisherTx.isClosed()) {
            publisherTx.rollback();
            publisherTx.close();
        }
    }

    @Test
    @DisplayName("pinned coalesced-into row is invisible to claims until the publisher commits")
    void pinnedRowIsSkippedByClaimsUntilCommit() throws SQLException {
        // An earlier transaction published SYNC_ORDER:42 — committed, PENDING.
        PendingEvent original = pendingWithKey("v1", "42");
        assertThat(pooledStore.save(original)).isTrue();

        // The current business transaction modifies the order and publishes the same key:
        // conditional insert conflicts, then the existing row is locked inside the transaction.
        assertThat(publisherTxStore.save(pendingWithKey("v2", "42"))).isFalse();
        assertThat(publisherTxStore.lockPendingByDedupKey(TYPE, "42")).contains(original.id());

        // THE CRUX: a concurrent worker must NOT be able to claim the pinned row now — otherwise
        // it would process the order without seeing this transaction's uncommitted changes.
        assertThat(pooledStore.claim(new ClaimRequest(TYPE, WORKER, 10))).isEmpty();

        publisherTx.commit();

        // After the commit the row is claimable, and the handler sees the committed data.
        assertThat(pooledStore.claim(new ClaimRequest(TYPE, WORKER, 10)))
                .singleElement()
                .satisfies(ce -> assertThat(ce.id()).isEqualTo(original.id()));
    }

    @Test
    @DisplayName(
            "a key whose event is already PROCESSING inserts a new event instead of coalescing")
    void processingKeyDoesNotCoalesce() {
        PendingEvent original = pendingWithKey("v1", "77");
        assertThat(pooledStore.save(original)).isTrue();
        assertThat(pooledStore.claim(new ClaimRequest(TYPE, WORKER, 10))).hasSize(1);

        // The first event is mid-handling: coalescing into it would lose our update, so a fresh
        // event must insert and run afterwards.
        assertThat(pooledStore.save(pendingWithKey("v2", "77"))).isTrue();
        assertThat(pooledStore.lockPendingByDedupKey(TYPE, "77")).isPresent();
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static EventStore storeOver(ConnectionSupplier connections) {
        return new PostgresEventStore(
                connections,
                PostgresStorageProperties.defaults(),
                Clock.system(),
                MetricsSnapshotCache.noop());
    }

    private static PendingEvent pendingWithKey(String payload, String key) {
        return PendingEvent.builder()
                .id(UUID.randomUUID())
                .eventType(TYPE)
                .payload(SerializedPayload.ofText("\"" + payload + "\""))
                .payloadFormat("test-json")
                .payloadClass("java.lang.String")
                .priority((short) 0)
                .runAt(Instant.now().minusSeconds(1))
                .traceContext(Map.of())
                .dedupKey(key)
                .build();
    }
}
