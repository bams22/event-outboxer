/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.polling;

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import java.util.List;

/**
 * Strategy executed by a {@link Poller} each tick to obtain the next batch of events. MVP ships a
 * single {@code LockAndFetchStrategy} that delegates to {@code EventStore.claim(...)}; future
 * implementations could add a LISTEN/NOTIFY-driven fast wake-up path (ADR-0006).
 */
@FunctionalInterface
public interface PollStrategy {

    /** Claim up to {@code batchSize} events for the given type. */
    List<ClaimedEvent> pollOnce(String eventType, WorkerId workerId, int batchSize);
}
