/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.testkit.assertions;

import io.github.bams22.outboxer.spi.EventStore;

/** Entry point for fluent {@link EventStore} assertions. Mirrors AssertJ's static-factory style. */
public final class EventAssertions {

    private EventAssertions() {}

    public static EventStoreAssert assertThatStore(EventStore store) {
        return new EventStoreAssert(store);
    }
}
