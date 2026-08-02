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
 * A cross-field configuration invariant was violated at startup. Examples: {@code deadThreshold < 3
 * * heartbeatInterval}, {@code handlerMaxRuntime <= 0}, {@code polling-interval >
 * max-idle-polling-interval}.
 */
public final class InvariantViolationException extends ConfigurationException {

    private static final long serialVersionUID = 1L;

    public InvariantViolationException(String message) {
        super(message);
    }
}
