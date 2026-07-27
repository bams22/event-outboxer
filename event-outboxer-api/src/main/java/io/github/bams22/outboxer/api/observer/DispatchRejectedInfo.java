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
import java.util.UUID;

/**
 * Payload of {@link OutboxListener#onDispatchRejected(DispatchRejectedInfo)} — fired when the
 * per-type handler executor rejected the task (queue saturation or shutdown). The engine responds
 * by returning the already-claimed event to PENDING so another worker may pick it up.
 *
 * @param eventId id of the event whose dispatch was rejected
 * @param eventType event type string
 * @param cause exception thrown by the executor (typically {@code RejectedExecutionException})
 */
public record DispatchRejectedInfo(UUID eventId, String eventType, Throwable cause) {

  public DispatchRejectedInfo {
    Objects.requireNonNull(eventId, "eventId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(cause, "cause must not be null");
  }
}
