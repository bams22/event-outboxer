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

import java.util.Objects;

/**
 * Payload of {@link OutboxListener#onPollerSaturated(PollerSaturatedInfo)} — fired when a per-type
 * poller skipped a claim cycle because the handler executor had no free capacity (pool and queue
 * budget exhausted). The poller parks and re-checks; each firing represents one skipped poll cycle,
 * so the rate of this callback approximates the fraction of time the type is saturated.
 *
 * @param eventType event type of the saturated poller
 */
public record PollerSaturatedInfo(String eventType) {

    public PollerSaturatedInfo {
        Objects.requireNonNull(eventType, "eventType must not be null");
    }
}
