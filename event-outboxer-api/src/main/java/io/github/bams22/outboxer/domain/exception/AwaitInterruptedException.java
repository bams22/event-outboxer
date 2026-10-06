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

/**
 * The thread waiting in {@code OutboxEventTracker.await(...)} was interrupted. The interrupt flag
 * is restored before this exception is thrown, so callers further up still see it (ADR-0038).
 */
public final class AwaitInterruptedException extends TrackingException {

    private static final long serialVersionUID = 1L;

    /**
     * Message code used as a prefix in error text: {@value}.
     */
    public static final String CODE = "OUTBOX-502";

    public AwaitInterruptedException(String message, InterruptedException cause) {
        super(CODE + ": " + message, cause);
    }
}
