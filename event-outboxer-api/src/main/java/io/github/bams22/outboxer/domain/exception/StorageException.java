/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain.exception;

import org.jspecify.annotations.Nullable;

/**
 * Base class for storage-adapter failures: SQL errors, connection problems, deadlocks, constraint
 * violations, and anything else surfaced by the underlying {@code EventStore} or {@code
 * WorkerRegistry}. Adapters are required to wrap native exceptions (for example {@link
 * java.sql.SQLException}) into this hierarchy so the core engine stays storage-agnostic.
 *
 * <p>Concurrent-completion conflicts (the finalize operation seeing a mutated {@code version}) are
 * <strong>not</strong> surfaced through this exception — they are represented by a {@code false}
 * return value from finalize methods (see ADR-0014).
 */
public abstract class StorageException extends OutboxException {

    private static final long serialVersionUID = 1L;

    protected StorageException(String message) {
        super(message);
    }

    protected StorageException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
