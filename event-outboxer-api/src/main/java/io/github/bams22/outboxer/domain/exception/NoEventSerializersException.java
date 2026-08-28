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
 * The Spring Boot starter found no {@code EventSerializer} bean, so nothing can serialize event
 * payloads. JSON via Jackson ships with the starter by default, so this only happens when {@code
 * event-outboxer-serializer-jackson} was excluded from the starter (or its auto-configuration
 * disabled) without another serializer module or an {@code EventSerializer} bean taking its place
 * (ADR-0016, amendment 2026-08-29).
 */
public final class NoEventSerializersException extends ConfigurationException {

    private static final long serialVersionUID = 1L;

    public NoEventSerializersException(String message) {
        super(message);
    }
}
