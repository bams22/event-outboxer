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
import io.github.bams22.outboxer.spi.contracts.AbstractEventStoreContractTest;
import org.junit.jupiter.api.BeforeEach;

class PostgresEventStoreIT extends AbstractEventStoreContractTest {

  private PostgresEventStore eventStore;

  @BeforeEach
  void truncateBetweenTests() {
    PostgresTestEnvironment.truncate();
  }

  @Override
  protected EventStore newStore() {
    this.eventStore =
        new PostgresEventStore(
            PostgresTestEnvironment.connectionSupplier(),
            PostgresStorageProperties.defaults(),
            Clock.system());
    return eventStore;
  }
}
