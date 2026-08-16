/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.observer;

import java.time.Duration;
import java.util.Objects;

/**
 * Payload of {@link OutboxListener#onStaleClaimsSwept(StaleClaimsSweptInfo)} — fired when the
 * stale-claim sweeper released events back to {@code PENDING} whose claims outlived the threshold
 * without a corresponding live in-flight registration. Fires only when at least one claim was
 * swept.
 *
 * @param count number of events released back to {@code PENDING}
 * @param threshold the staleness threshold that was applied
 */
public record StaleClaimsSweptInfo(long count, Duration threshold) {

    public StaleClaimsSweptInfo {
        Objects.requireNonNull(threshold, "threshold must not be null");
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative");
        }
    }
}
