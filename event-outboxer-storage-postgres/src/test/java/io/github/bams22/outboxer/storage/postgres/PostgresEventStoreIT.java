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

import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.contracts.AbstractEventStoreContractTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;

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
}
