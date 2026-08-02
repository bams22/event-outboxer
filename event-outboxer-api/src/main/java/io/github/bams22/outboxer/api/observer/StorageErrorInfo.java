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
 * Payload of {@link OutboxListener#onStorageError(StorageErrorInfo)} — fired when a storage
 * operation failed (SQL error, connection timeout, deadlock, constraint violation, ...). The engine
 * catches the exception, emits this event, and continues; transient errors resolve on the next
 * cycle.
 *
 * @param operation a short label naming the operation that failed (for example {@code "claim"},
 *     {@code "markProcessed"}, {@code "heartbeat"})
 * @param cause underlying exception
 */
public record StorageErrorInfo(String operation, Throwable cause) {

    public StorageErrorInfo {
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(cause, "cause must not be null");
    }
}
