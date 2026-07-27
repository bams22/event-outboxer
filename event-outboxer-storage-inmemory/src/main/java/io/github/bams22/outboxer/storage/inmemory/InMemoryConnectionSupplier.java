/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.inmemory;

import io.github.bams22.outboxer.spi.ConnectionSupplier;
import java.sql.Connection;

/**
 * Stub {@link ConnectionSupplier} for the in-memory adapter. Always throws — the in-memory {@code
 * EventStore} and {@code WorkerRegistry} never open JDBC connections, so this supplier exists only
 * to satisfy DI wiring in setups that expect the port to be present.
 */
public final class InMemoryConnectionSupplier implements ConnectionSupplier {

  @Override
  public Connection get() {
    throw new UnsupportedOperationException(
        "InMemoryConnectionSupplier does not provide JDBC connections; "
            + "use event-outboxer-storage-postgres for a real backend.");
  }

  @Override
  public void release(Connection connection) {
    throw new UnsupportedOperationException(
        "InMemoryConnectionSupplier does not manage JDBC connections.");
  }
}
