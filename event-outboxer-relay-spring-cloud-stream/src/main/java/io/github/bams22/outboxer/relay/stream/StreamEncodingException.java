/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.relay.stream;

import io.github.bams22.outboxer.domain.exception.PublishException;
import org.jspecify.annotations.Nullable;

/**
 * The relay publisher could not encode the payload to its wire form. Thrown synchronously from
 * {@code StreamOutboxPublisher.publish(...)} in the caller's transaction — nothing is persisted.
 * Typically wraps a Jackson {@code JsonProcessingException} from the configured {@link
 * StreamPayloadEncoder}.
 */
public final class StreamEncodingException extends PublishException {

    private static final long serialVersionUID = 1L;

    /** Message code used as a prefix in error text: {@value}. */
    public static final String CODE = "OUTBOX-105";

    public StreamEncodingException(String message, @Nullable Throwable cause) {
        super(CODE + ": " + message, cause);
    }
}
