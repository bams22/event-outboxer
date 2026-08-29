/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain;

import java.util.Objects;

/**
 * Typed key of an event type: the stable {@code event_type} string stored with every event plus the
 * Java class of its payload (ADR-0031). One constant, shared by the handler and the producer, is
 * the only place the pair is spelled out, so a typo or a wrong DTO fails at compile time — or at
 * publish, with a {@code PublishValidationException} — instead of at handle time:
 *
 * <pre>{@code
 * public class SendEmailHandler implements EventHandler<SendEmailPayload> {
 *     public static final EventType<SendEmailPayload> SEND_EMAIL =
 *             EventType.of("SEND_EMAIL", SendEmailPayload.class);
 *
 *     @Override public EventType<SendEmailPayload> type() { return SEND_EMAIL; }
 *     ...
 * }
 *
 * publisher.publish(SendEmailHandler.SEND_EMAIL, new SendEmailPayload(...));
 * }</pre>
 *
 * <p>Larger applications typically collect the constants in one {@code Events} holder class. {@code
 * name} is a natural key in the database (at most {@link #MAX_NAME_LENGTH} characters, the width of
 * the {@code event_type} column) and must never change once events exist. Storage-side records
 * ({@code PendingEvent}, {@code ClaimedEvent}, {@code EventContext}) and string-keyed configuration
 * (YAML {@code overrides.<TYPE>}, {@code failureHandlerFor(String, ...)}) keep using the plain
 * name. Use wrapper classes ({@code Integer.class}, not {@code int.class}) — the publish check is
 * {@link Class#isInstance}.
 *
 * @param name stable event-type identifier; non-blank, at most {@link #MAX_NAME_LENGTH} characters
 * @param payloadType class the publisher checks the payload against and the handler deserializes
 *     into (explicit, no reflection — ADR-0003)
 * @param <T> payload type
 */
public record EventType<T>(String name, Class<T> payloadType) {

    /** Maximum length of {@link #name()}, matching {@code events.event_type VARCHAR(128)}. */
    public static final int MAX_NAME_LENGTH = 128;

    public EventType {
        Objects.requireNonNull(name, "name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException(
                    "name must be at most "
                            + MAX_NAME_LENGTH
                            + " characters, got "
                            + name.length());
        }
        Objects.requireNonNull(payloadType, "payloadType must not be null");
    }

    /**
     * Creates a typed key. Prefer a {@code public static final} constant over calling this per use.
     */
    public static <T> EventType<T> of(String name, Class<T> payloadType) {
        return new EventType<>(name, payloadType);
    }

    /**
     * Escape hatch for producers that only know the name at runtime (gateways, replay tools): the
     * publish-time payload check degrades to "not null". Prefer {@link #of} wherever the payload
     * class is known at compile time.
     */
    public static EventType<Object> untyped(String name) {
        return new EventType<>(name, Object.class);
    }

    @Override
    public String toString() {
        return name;
    }
}
