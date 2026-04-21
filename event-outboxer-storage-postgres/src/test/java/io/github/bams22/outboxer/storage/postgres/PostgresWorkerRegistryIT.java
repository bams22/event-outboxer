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

import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.spi.contracts.AbstractWorkerRegistryContractTest;
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
}
