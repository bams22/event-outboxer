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
 * Startup-time failure: a per-event-type configuration block is invalid. Examples: non-positive
 * {@code handler-pool-size} or {@code claim-batch-size}, negative {@code handler-queue-capacity},
 * {@code poll-multiplier <= 1.0}, or {@code poll-max-interval} smaller than {@code
 * poll-min-interval}.
 */
public final class InvalidEventTypeConfigException extends ConfigurationException {

    private static final long serialVersionUID = 1L;

    public InvalidEventTypeConfigException(String message) {
        super(message);
    }

    public InvalidEventTypeConfigException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
