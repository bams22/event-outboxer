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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.contracts.AbstractEventStoreContractTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PostgresEventStoreIT extends AbstractEventStoreContractTest {

  private PostgresEventStore eventStore;

  @BeforeEach
  void truncateBetweenTests() {
    PostgresTestEnvironment.truncate();
  }

  @Override
  protected EventStore newStore() {
    // Tests want every metricsSnapshot() call to reflect the current store state, so use
    // noop() to bypass the TTL cache entirely.
    this.eventStore =
        new PostgresEventStore(
            PostgresTestEnvironment.connectionSupplier(),
            PostgresStorageProperties.defaults(),
            Clock.system(),
            MetricsSnapshotCache.noop());
    return eventStore;
  }

  @Override
  protected void backdateClaim(UUID id, Instant at) {
    String sql =
        "UPDATE " + PostgresTestEnvironment.SCHEMA + ".events SET claimed_at = ? WHERE id = ?";
    try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setTimestamp(1, Timestamp.from(at));
      ps.setObject(2, id);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new IllegalStateException("failed to backdate claim for " + id, e);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // PG-specific: dual payload lane (ADR-0025)
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("text save fills payload (JSONB), binary save fills payload_binary (BYTEA)")
  void payloadLandsInExactlyOneColumnPerLane() throws SQLException {
    PendingEvent text = pending(EVENT_TYPE_A, "lane-check", Instant.now());
    PendingEvent binary = pendingBinary(EVENT_TYPE_B, binaryPayloadFixture(), Instant.now());
    store.save(text);
    store.save(binary);

    assertThat(lane(text.id())).isEqualTo("text");
    assertThat(lane(binary.id())).isEqualTo("binary");
  }

  @Test
  @DisplayName("events_payload_exactly_one CHECK rejects both-null and both-set direct inserts")
  void checkConstraintEnforcesExactlyOneLane() {
    String bothNull = rawInsert("NULL", "NULL");
    String bothSet = rawInsert("'\"x\"'::jsonb", "'\\x00ff'::bytea");

    assertThatThrownBy(() -> execute(bothNull))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("events_payload_exactly_one");
    assertThatThrownBy(() -> execute(bothSet))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("events_payload_exactly_one");
  }

  private static String rawInsert(String payloadExpr, String payloadBinaryExpr) {
    return "INSERT INTO "
        + PostgresTestEnvironment.SCHEMA
        + ".events (id, event_type, payload, payload_binary, payload_format, payload_class,"
        + " status) VALUES ('"
        + UUID.randomUUID()
        + "', 'RAW', "
        + payloadExpr
        + ", "
        + payloadBinaryExpr
        + ", 'test-json', 'java.lang.String', 'PENDING')";
  }

  private static String lane(UUID id) throws SQLException {
    String sql =
        "SELECT (payload IS NOT NULL) AS has_text, (payload_binary IS NOT NULL) AS has_bin FROM "
            + PostgresTestEnvironment.SCHEMA
            + ".events WHERE id = '"
            + id
            + "'";
    try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      rs.next();
      boolean hasText = rs.getBoolean("has_text");
      boolean hasBin = rs.getBoolean("has_bin");
      if (hasText == hasBin) {
        return "both-or-neither";
      }
      return hasText ? "text" : "binary";
    }
  }

  private static void execute(String sql) throws SQLException {
    try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }
}
