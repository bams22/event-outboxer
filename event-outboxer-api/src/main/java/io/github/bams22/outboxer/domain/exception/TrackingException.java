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
 * Base class for misuse or interruption of {@code OutboxEventTracker} (ADR-0038). Catching this
 * category covers every tracking-side failure; lookup failures surface as {@link
 * StorageException}.
 */
public abstract class TrackingException extends OutboxException {

    private static final long serialVersionUID = 1L;

    protected TrackingException(String message) {
        super(message);
    }

    protected TrackingException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
