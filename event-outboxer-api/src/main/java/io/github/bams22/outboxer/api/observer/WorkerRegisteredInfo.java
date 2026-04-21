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

import io.github.bams22.outboxer.domain.WorkerInfo;
import java.util.Objects;

/**
 * Payload of {@link OutboxListener#onWorkerRegistered(WorkerRegisteredInfo)} — fired once at
 * engine startup, after the worker has been inserted into the {@code outbox.workers} table.
 *
 * @param info full registration metadata
 */
public record WorkerRegisteredInfo(WorkerInfo info) {

  public WorkerRegisteredInfo {
    Objects.requireNonNull(info, "info must not be null");
  }
}
