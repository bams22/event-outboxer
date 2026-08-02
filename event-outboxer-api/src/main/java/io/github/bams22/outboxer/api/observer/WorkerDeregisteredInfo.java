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
 * Payload of {@link OutboxListener#onWorkerDeregistered(WorkerDeregisteredInfo)} — fired at the end
 * of graceful shutdown, after the worker row has been removed from {@code event_outboxer.workers}.
 *
 * @param workerId id of the worker that just deregistered
 */
public record WorkerDeregisteredInfo(WorkerId workerId) {

    public WorkerDeregisteredInfo {
        Objects.requireNonNull(workerId, "workerId must not be null");
    }
}
