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
 * Raised by {@code publishAll(...)} when at least one event in the batch could not be persisted.
 * The fail-fast contract (ADR Q3) guarantees that either every event in the batch is saved or none
 * of them is: on failure the caller's transaction is expected to roll back and no events from the
 * batch remain in the outbox.
 */
public final class PublishFailedException extends PublishException {

    private static final long serialVersionUID = 1L;

    /** Message code used as a prefix in error text: {@value}. */
    public static final String CODE = "OUTBOX-104";

    public PublishFailedException(String message, @Nullable Throwable cause) {
        super(CODE + ": " + message, cause);
    }
}
