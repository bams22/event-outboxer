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
 * The engine was assembled without a single {@code EventHandler} and was not declared publish-only.
 * An outbox that persists events nobody processes is almost always a wiring mistake (component scan
 * missed the handlers, wrong profile), so the engine refuses to start unless the caller opts into
 * publish-only mode explicitly — {@code OutboxEngineBuilder.publishOnly(true)}, or {@code
 * event-outboxer.publish-only=true} with the Spring Boot starter (ADR-0029).
 */
public final class NoEventHandlersException extends ConfigurationException {

    private static final long serialVersionUID = 1L;

    public NoEventHandlersException(String message) {
        super(message);
    }
}
