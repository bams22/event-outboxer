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
 * Payload of {@link OutboxListener#onHandlerError(HandlerErrorInfo)} — fired when the handler threw
 * an exception. The engine still decides whether to retry or disable based on the failure handler
 * chain; this listener event is purely informational.
 *
 * @param eventId identifier of the event being processed
 * @param eventType event type string
 * @param attempts attempt counter including this failed attempt
 * @param cause exception thrown by the handler
 */
public record HandlerErrorInfo(UUID eventId, String eventType, int attempts, Throwable cause) {

    public HandlerErrorInfo {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(cause, "cause must not be null");
    }
}
