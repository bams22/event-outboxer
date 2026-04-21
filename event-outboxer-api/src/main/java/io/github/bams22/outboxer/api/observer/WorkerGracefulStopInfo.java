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
 * Payload of {@link OutboxListener#onWorkerGracefulStop(WorkerGracefulStopInfo)} — fired when the
 * worker set {@code graceful_stop=TRUE}, signalling orphan detection to leave it alone while
 * in-flight work drains.
 *
 * @param workerId id of the worker that is stopping gracefully
 */
public record WorkerGracefulStopInfo(WorkerId workerId) {

  public WorkerGracefulStopInfo {
    Objects.requireNonNull(workerId, "workerId must not be null");
  }
}
