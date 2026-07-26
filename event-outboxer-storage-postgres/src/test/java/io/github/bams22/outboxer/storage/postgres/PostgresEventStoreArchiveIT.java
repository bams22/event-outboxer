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
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Archive-mode {@code markProcessed} behaviour. The finalize is a single atomic CTE
 * (DELETE ... RETURNING feeding the archive INSERT), so a lost optimistic-lock race must leave
 * zero archive rows — an orphan archive row would permanently block every future
 * {@code markProcessed} of the same event on the archive PK.
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
  @DisplayName("lost race (stale version) → false, no orphan archive row, event stays finalizable")
  void markProcessed_lostRace_leavesNoArchiveRow() {
    ClaimedEvent claimed = publishAndClaim("payload-2");

    // Simulate the watchdog / orphan recovery having taken the row back: the caller's version
    // is stale. The guarded DELETE inside the CTE matches nothing, so the archive INSERT must
    // insert nothing — false return, zero archive rows.
    boolean stale =
        store.markProcessed(claimed.id(), WORKER, claimed.claimedVersion() - 1);

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
  @DisplayName("release() after claim leaves no archive row and the event is re-claimable")
  void release_leavesNoArchiveRow() {
    ClaimedEvent claimed = publishAndClaim("payload-3");

    boolean released =
        store.release(
            claimed.id(), WORKER, claimed.claimedVersion(), "backpressure", Instant.now());

    assertThat(released).isTrue();
    assertThat(countArchiveRows(claimed.id())).isZero();
    assertThat(store.findById(claimed.id()).orElseThrow().status())
        .isEqualTo(EventStatus.PENDING);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private ClaimedEvent publishAndClaim(String payload) {
    PendingEvent p =
        PendingEvent.builder()
            .id(UUID.randomUUID())
            .eventType("ARCHIVE_T")
            .payload("\"" + payload + "\"")
            .payloadClass("java.lang.String")
            .priority((short) 0)
            .runAt(Instant.now().minusSeconds(1))
            .traceContext(Map.of())
            .build();
    store.save(p);
    List<ClaimedEvent> claimed = store.claim(new ClaimRequest("ARCHIVE_T", WORKER, 10));
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
