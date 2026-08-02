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

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.spi.contracts.AbstractWorkerRegistryContractTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;

class PostgresWorkerRegistryIT extends AbstractWorkerRegistryContractTest {

    @BeforeEach
    void truncateBetweenTests() {
        PostgresTestEnvironment.truncate();
    }

    @Override
    protected WorkerRegistry newRegistry() {
        return new PostgresWorkerRegistry(
                PostgresTestEnvironment.connectionSupplier(), PostgresStorageProperties.defaults());
    }

    /**
     * The PG adapter stamps heartbeats with the database clock and ignores the {@code at} argument,
     * so staleness has to be injected with a direct UPDATE.
     */
    @Override
    protected void backdateHeartbeat(WorkerId id, Instant at) {
        String sql =
                "UPDATE "
                        + PostgresTestEnvironment.SCHEMA
                        + ".workers SET last_heartbeat = ? WHERE worker_id = ?";
        try (Connection conn = PostgresTestEnvironment.dataSource().getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(at));
            ps.setString(2, id.value());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to backdate heartbeat for " + id, e);
        }
    }
}
