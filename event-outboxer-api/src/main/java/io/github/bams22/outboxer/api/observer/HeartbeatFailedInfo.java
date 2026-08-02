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

import io.github.bams22.outboxer.domain.WorkerId;
import java.util.Objects;

/**
 * Payload of {@link OutboxListener#onHeartbeatFailed(HeartbeatFailedInfo)} — fired when the
 * periodic worker heartbeat could not reach storage. If heartbeats keep failing, this worker's
 * events are eventually reclaimed as orphans.
 *
 * @param workerId id of the worker whose heartbeat failed
 * @param cause underlying exception from storage
 */
public record HeartbeatFailedInfo(WorkerId workerId, Throwable cause) {

    public HeartbeatFailedInfo {
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(cause, "cause must not be null");
    }
}
