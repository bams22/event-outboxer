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

import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.contracts.AbstractEntityLockerContractTest;

class InMemoryEntityLockerTest extends AbstractEntityLockerContractTest {

    @Override
    protected EntityLocker newLocker() {
        return new InMemoryEntityLocker();
    }
}
