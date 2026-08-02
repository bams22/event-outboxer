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
 * Base class for every failure that happens while publishing an event from the client side. Thrown
 * synchronously from {@code OutboxEventPublisher.publish(...)} or {@code publishAll(...)}. Catching
 * this category is enough to roll back the caller's transaction on any publish-time problem without
 * having to enumerate the concrete subclasses.
 */
public abstract class PublishException extends OutboxException {

    private static final long serialVersionUID = 1L;

    protected PublishException(String message) {
        super(message);
    }

    protected PublishException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
