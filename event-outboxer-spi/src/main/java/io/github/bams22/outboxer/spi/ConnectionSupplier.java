/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * SPI port that decouples the PostgreSQL storage adapter from Spring's transaction-management
 * machinery (see ADR-0002). The adapter calls {@link #get()} every time it needs a JDBC {@code
 * Connection} and calls {@link #release(Connection)} when it is done with it.
 *
 * <p>The plain-Java wiring backs the port with {@code dataSource::getConnection} and {@link
 * Connection#close()}; the Spring Boot starter substitutes an implementation that resolves the
 * connection through {@code DataSourceUtils.getConnection(dataSource)} on a {@code
 * TransactionAwareDataSourceProxy}, so that {@code OutboxEventPublisher.publish()} participates in
 * the caller's {@code @Transactional} boundary and shares the same physical connection as the
 * business INSERT/UPDATE.
 *
 * <p>Implementations must be thread-safe. The engine invokes the port concurrently from multiple
 * per-type pollers and maintenance tasks.
 */
public interface ConnectionSupplier {

    /**
     * Obtain a JDBC connection. In a transactional context the returned connection is bound to the
     * current transaction and must NOT be committed or rolled back by the caller. In a non-
     * transactional context the returned connection is a freshly leased one from the pool.
     *
     * @throws SQLException if the connection cannot be obtained from the underlying {@code
     *     DataSource}
     */
    Connection get() throws SQLException;

    /**
     * Release a connection obtained via {@link #get()}. In a transactional context this is a no-op
     * (Spring's transaction manager owns the connection lifecycle); in a non-transactional context
     * this closes the connection and returns it to the pool.
     *
     * <p>Callers typically invoke this in a {@code finally} block after every query. Idempotent —
     * releasing a connection twice must not throw.
     *
     * @throws SQLException if the connection cannot be returned to the pool
     */
    void release(Connection connection) throws SQLException;
}
