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
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.spi.ArchiveCursor;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.spi.OutboxAdmin.ReplayAllResult;
import io.github.bams22.outboxer.spi.OutboxAdmin.ReplayOutcome;
import io.github.bams22.outboxer.spi.contracts.AbstractOutboxAdminContractTest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
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
        assertThat(archiveStore().markProcessed(claimed.id(), WORKER, claimed.claimedVersion()))
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
            assertThat(archiveStore().markProcessed(claimed.id(), WORKER, claimed.claimedVersion()))
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
    // replay from archive (ADR-0033)
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("replayFromArchive() moves the row back as a fresh PENDING with fields reset")
    void replayFromArchive_movesRowBackAsFreshPending() {
        UUID id = archiveOne("replay-me", "replay-key");
        // A year-old publish time: had the replay copied created_at instead of stamping it, the
        // row would re-enter the hot table already past every retention and paging threshold that
        // keys on that column — purgeDisabled, reenableAll, findByStatus (ADR-0033).
        backdateArchivedCreatedAt(id, Duration.ofDays(365));
        Instant before = Instant.now().minusSeconds(1);

        assertThat(admin.replayFromArchive(id)).isEqualTo(ReplayOutcome.REPLAYED);

        Event replayed = store.findById(id).orElseThrow();
        assertThat(replayed.status()).isEqualTo(EventStatus.PENDING);
        assertThat(replayed.attempts()).isZero();
        assertThat(replayed.version()).isZero();
        assertThat(replayed.claimedBy()).isNull();
        assertThat(replayed.claimedAt()).isNull();
        assertThat(replayed.lastFailReason()).isEqualTo("replayed from archive");
        assertThat(replayed.dedupKey()).isEqualTo("replay-key");
        assertThat(replayed.runAt()).isAfterOrEqualTo(before);
        // The replay is a new lifecycle: created_at is the replay moment, not the year-old
        // publish time it carried in the archive. The archive row itself is gone.
        assertThat(replayed.createdAt()).isAfterOrEqualTo(before);
        assertThat(admin.findInArchive(id)).isEmpty();
        assertThat(countArchiveRows()).isZero();
    }

    @Test
    @DisplayName("replayFromArchive() keeps a binary payload byte-exact")
    void replayFromArchive_binaryPayloadSurvives() {
        byte[] raw = new byte[] {0x00, (byte) 0xFF, 0x42, 0x00, (byte) 0x80};
        ClaimedEvent claimed =
                claimArchiveMode(
                        PendingEvent.builder()
                                .id(UUID.randomUUID())
                                .eventType("ARCH_T")
                                .payload(SerializedPayload.ofBytes(raw))
                                .payloadFormat("test-binary")
                                .payloadClass("java.lang.String")
                                .priority((short) 0)
                                .runAt(Instant.now().minusSeconds(1))
                                .traceContext(Map.of())
                                .build());
        assertThat(archiveStore().markProcessed(claimed.id(), WORKER, claimed.claimedVersion()))
                .isTrue();

        assertThat(admin.replayFromArchive(claimed.id())).isEqualTo(ReplayOutcome.REPLAYED);

        Event replayed = store.findById(claimed.id()).orElseThrow();
        assertThat(replayed.payload().isText()).isFalse();
        assertThat(replayed.payload().requireBytes()).isEqualTo(raw);
        assertThat(replayed.payloadFormat()).isEqualTo("test-binary");
    }

    @Test
    @DisplayName("replayFromArchive() with an unknown id reports NOT_FOUND")
    void replayFromArchive_unknownId_notFound() {
        assertThat(admin.replayFromArchive(UUID.randomUUID())).isEqualTo(ReplayOutcome.NOT_FOUND);
    }

    @Test
    @DisplayName("replay against a live PENDING with the same key coalesces, archive row kept")
    void replayFromArchive_pendingSameKey_coalescesAndKeepsArchiveRow() {
        UUID archivedId = archiveOne("processed-first", "shared-key");
        // A fresh PENDING event with the same (type, key) is already scheduled.
        var pendingSameKey = pendingKeyed("scheduled-later", "shared-key");
        archiveStore().save(pendingSameKey);

        assertThat(admin.replayFromArchive(archivedId)).isEqualTo(ReplayOutcome.COALESCED);

        // Nothing inserted, the audit row survived, the live event is untouched.
        assertThat(store.findById(archivedId)).isEmpty();
        assertThat(admin.findInArchive(archivedId)).isPresent();
        assertThat(store.findById(pendingSameKey.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PENDING);
    }

    @Test
    @DisplayName("replayAllFromArchive() counts replayed vs coalesced-and-kept rows")
    void replayAllFromArchive_countsReplayedVsCoalesced() {
        archiveOne("a", "key-a");
        archiveOne("b", "key-b");
        archiveOne("c", null);
        UUID blocked = archiveOne("d", "key-live");
        archiveStore().save(pendingKeyed("live", "key-live"));

        ReplayAllResult result = admin.replayAllFromArchive("ARCH_T", null, null, 100, null);

        assertThat(result.replayed()).isEqualTo(3);
        assertThat(result.coalesced()).isEqualTo(1);
        assertThat(result.idInUse()).isZero();
        assertThat(result.next()).isNotNull();
        assertThat(admin.findInArchive(blocked)).isPresent();
        assertThat(countArchiveRows()).isEqualTo(1);
    }

    @Test
    @DisplayName("duplicate key inside one bulk batch: oldest archived row replays, newer stays")
    void replayAllFromArchive_intraBatchDuplicateKey() {
        UUID first = archiveOne("first", "dup-key");
        UUID second = archiveOne("second", "dup-key");
        // Compute the expected winner from the actual archived_at order the SQL uses, so the
        // assertion cannot flake on a timestamp tie (broken by id in that case).
        ArchivedEvent a = admin.findInArchive(first).orElseThrow();
        ArchivedEvent b = admin.findInArchive(second).orElseThrow();
        UUID older =
                Comparator.comparing(ArchivedEvent::archivedAt)
                                        .thenComparing(ArchivedEvent::id)
                                        .compare(a, b)
                                <= 0
                        ? first
                        : second;
        UUID newer = older.equals(first) ? second : first;

        ReplayAllResult result = admin.replayAllFromArchive("ARCH_T", null, null, 100, null);

        assertThat(result.replayed()).isEqualTo(1);
        assertThat(result.coalesced()).isEqualTo(1);
        assertThat(store.findById(older)).isPresent();
        assertThat(admin.findInArchive(older)).isEmpty();
        assertThat(store.findById(newer)).isEmpty();
        assertThat(admin.findInArchive(newer)).isPresent();
    }

    @Test
    @DisplayName("replayAllFromArchive() respects the archived_at window and the limit")
    void replayAllFromArchive_respectsWindowAndLimit() {
        UUID first = archiveOne("w-1", null);
        UUID second = archiveOne("w-2", null);
        UUID third = archiveOne("w-3", null);
        Instant firstAt = admin.findInArchive(first).orElseThrow().archivedAt();
        Instant thirdAt = admin.findInArchive(third).orElseThrow().archivedAt();

        // Exclusive bounds cut off the first and the third row.
        ReplayAllResult windowed =
                admin.replayAllFromArchive("ARCH_T", firstAt, thirdAt, 100, null);
        assertThat(windowed.replayed()).isEqualTo(1);
        assertThat(windowed.coalesced()).isZero();
        assertThat(store.findById(second)).isPresent();

        // The limit caps how many of the remaining rows are considered.
        ReplayAllResult limited = admin.replayAllFromArchive("ARCH_T", null, null, 1, null);
        assertThat(limited.replayed()).isEqualTo(1);
        assertThat(countArchiveRows()).isEqualTo(1);
    }

    @Test
    @DisplayName("a live id does not abort the batch: that row is counted and the rest replays")
    void replayAllFromArchive_liveIdIsSkippedNotFatal() {
        UUID clean = archiveOne("clean", null);
        UUID collides = archiveOne("collides", null);
        // The application re-published the archived event's explicit UUID; the live row is
        // PROCESSING, so the (event_type, dedup_key) arbiter cannot catch the id conflict.
        ClaimedEvent live = claimArchiveMode(pendingWithId(collides, "republished", null));
        assertThat(live.id()).isEqualTo(collides);

        ReplayAllResult result = admin.replayAllFromArchive("ARCH_T", null, null, 100, null);

        assertThat(result.replayed()).isEqualTo(1);
        assertThat(result.idInUse()).isEqualTo(1);
        assertThat(result.coalesced()).isZero();
        // The clean row moved; the colliding one stayed archived and the live event is untouched.
        assertThat(store.findById(clean).orElseThrow().status()).isEqualTo(EventStatus.PENDING);
        assertThat(admin.findInArchive(clean)).isEmpty();
        assertThat(admin.findInArchive(collides)).isPresent();
        assertThat(store.findById(collides).orElseThrow().status())
                .isEqualTo(EventStatus.PROCESSING);
    }

    @Test
    @DisplayName("single replay onto a live id reports ID_IN_USE and keeps the archive row")
    void replayFromArchive_liveId_reportsIdInUse() {
        UUID id = archiveOne("collides", null);
        claimArchiveMode(pendingWithId(id, "republished", null));

        assertThat(admin.replayFromArchive(id)).isEqualTo(ReplayOutcome.ID_IN_USE);

        assertThat(admin.findInArchive(id)).isPresent();
        assertThat(store.findById(id).orElseThrow().status()).isEqualTo(EventStatus.PROCESSING);
    }

    @Test
    @DisplayName("the cursor walks past rows that stay archived, so a sweep terminates")
    void replayAllFromArchive_cursorAdvancesPastRowsThatStay() {
        // Two rows that cannot move — one coalescing, one whose id is live — bracketing one that
        // can. Without a cursor the same window would return them forever.
        UUID coalescing = archiveOne("keyed", "live-key");
        UUID movable = archiveOne("movable", null);
        UUID collides = archiveOne("collides", null);
        // Make the id live first: claim() takes a batch, so doing this after the PENDING row below
        // would drag that row into PROCESSING too and it would no longer coalesce.
        claimArchiveMode(pendingWithId(collides, "republished", null));
        archiveStore().save(pendingKeyed("already-scheduled", "live-key"));

        int replayed = 0;
        int considered = 0;
        int batches = 0;
        ArchiveCursor cursor = null;
        ReplayAllResult batch;
        do {
            batch = admin.replayAllFromArchive("ARCH_T", null, null, 1, cursor);
            cursor = batch.next();
            replayed += batch.replayed();
            considered += batch.replayed() + batch.coalesced() + batch.idInUse();
            batches++;
            assertThat(batches).isLessThan(10); // a stalled sweep must fail, not hang
        } while (cursor != null);

        // Every archive row was visited exactly once across the sweep, one per batch, and the
        // fourth batch came back empty to end it.
        assertThat(considered).isEqualTo(3);
        assertThat(replayed).isEqualTo(1);
        assertThat(batches).isEqualTo(4);
        assertThat(store.findById(movable)).isPresent();
        assertThat(admin.findInArchive(coalescing)).isPresent();
        assertThat(admin.findInArchive(collides)).isPresent();
    }

    @Test
    @DisplayName("V009 index covers the bulk-replay access path (event_type, archived_at, id)")
    void replayIndexExists() {
        try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
                Statement st = conn.createStatement()) {
            var rs =
                    st.executeQuery(
                            "SELECT indexdef FROM pg_indexes WHERE schemaname = '"
                                    + PostgresTestEnvironment.SCHEMA
                                    + "' AND indexname = 'idx_archive_event_type_archived_at'");
            assertThat(rs.next()).as("idx_archive_event_type_archived_at must exist").isTrue();
            // Column order is the point of the index, not its mere existence: event_type must lead
            // so the scan starts inside the type, and (archived_at, id) must follow in that order
            // so the ORDER BY and the keyset predicate are both index-resolvable.
            assertThat(rs.getString(1)).contains("(event_type, archived_at, id)");
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    /** Publishes a keyed event in archive mode, claims it and finalizes it into the archive. */
    private UUID archiveOne(String payload, @Nullable String dedupKey) {
        ClaimedEvent claimed = claimArchiveMode(pendingKeyed(payload, dedupKey));
        assertThat(archiveStore().markProcessed(claimed.id(), WORKER, claimed.claimedVersion()))
                .isTrue();
        return claimed.id();
    }

    private PendingEvent pendingKeyed(String payload, @Nullable String dedupKey) {
        return pendingWithId(UUID.randomUUID(), payload, dedupKey);
    }

    /** A publish that reuses an explicit UUID — how a replayed id ends up live again. */
    private PendingEvent pendingWithId(UUID id, String payload, @Nullable String dedupKey) {
        return PendingEvent.builder()
                .id(id)
                .eventType("ARCH_T")
                .payload(SerializedPayload.ofText("\"" + payload + "\""))
                .payloadFormat("test-json")
                .payloadClass("java.lang.String")
                .priority((short) 0)
                .runAt(Instant.now().minusSeconds(1))
                .traceContext(Map.of())
                .dedupKey(dedupKey)
                .build();
    }

    private ClaimedEvent publishAndClaimArchiveMode(String payload) {
        var p = pending("ARCH_T", payload);
        archiveStore().save(p);
        return archiveStore().claim(new ClaimRequest("ARCH_T", WORKER, 10)).stream()
                .filter(ce -> ce.id().equals(p.id()))
                .findFirst()
                .orElseThrow();
    }

    private ClaimedEvent claimArchiveMode(PendingEvent p) {
        archiveStore().save(p);
        return archiveStore().claim(new ClaimRequest(p.eventType(), WORKER, 10)).stream()
                .filter(ce -> ce.id().equals(p.id()))
                .findFirst()
                .orElseThrow();
    }

    private PostgresEventStore archiveStore() {
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
        return archiveStore;
    }

    /** Ages an archived row's {@code created_at} so a copied timestamp would be unmistakable. */
    private static void backdateArchivedCreatedAt(UUID id, Duration age) {
        try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
                Statement st = conn.createStatement()) {
            int updated =
                    st.executeUpdate(
                            "UPDATE "
                                    + PostgresTestEnvironment.SCHEMA
                                    + ".event_archive SET created_at = now() - interval '"
                                    + age.toSeconds()
                                    + " seconds' WHERE id = '"
                                    + id
                                    + "'");
            assertThat(updated).isOne();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
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
