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

import io.github.bams22.outboxer.domain.EventType;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * The persisted relay event: everything the built-in handler needs to deliver one message to a
 * Spring Cloud Stream binding (ADR-0032). Instances are created by {@code StreamOutboxPublisher}
 * with the user payload already encoded to its wire form — the handler ships the stored bytes/text
 * verbatim, so what was captured in the caller's transaction is exactly what reaches the broker.
 *
 * <p>The wire payload travels in exactly one of two lanes, mirroring {@code SerializedPayload}:
 * {@link #textPayload()} for textual wire formats (JSON), {@link #binaryPayload()} for binary ones.
 * Note that the envelope itself is stored through the engine's {@code jackson-json} write format,
 * so a binary lane is base64-encoded inside the JSONB column — a documented overhead, acceptable
 * for occasional pre-serialized payloads.
 *
 * <h2>Schema evolution contract</h2>
 *
 * <p>This record is a persisted schema, to be treated like a database migration: component names
 * are the JSON field names of stored events and must never be renamed; new components must be
 * {@code @Nullable} (or defaulted in the compact constructor) so rows written by older versions
 * still deserialize; the compact constructor must never start requiring a component added after the
 * module's first release. The engine's read-side mapper tolerates unknown fields, so newer writers
 * are safe for older readers.
 *
 * @param binding Spring Cloud Stream binding name the message is delivered to
 * @param key optional message key (for example an aggregate id); becomes the configured key header
 *     and, with per-key ordering enabled, part of the handler's lock key
 * @param headers broker message headers, copied verbatim onto the outgoing message; never {@code
 *     null} (normalized to an empty map)
 * @param contentType MIME type of the wire payload, set as the outgoing {@code contentType} header
 * @param textPayload textual wire form; {@code null} for binary payloads
 * @param binaryPayload binary wire form; {@code null} for textual payloads
 */
@Builder
public record StreamEnvelope(
        String binding,
        @Nullable String key,
        Map<String, String> headers,
        String contentType,
        @Nullable String textPayload,
        byte @Nullable [] binaryPayload) {

    /**
     * Stable {@code event_type} value of relay events — a library-reserved persisted natural key
     * (ADR-0032). It must never change once events exist, and is deliberately decoupled from the
     * module name. Applications tune the relay's retry policy through the regular per-type
     * configuration under this name.
     *
     * <p>Kebab-case with no dot on purpose: {@code event-outboxer.event-types.overrides} binds to a
     * {@code Map} with structured values, and Spring Boot's map binder splits keys on {@code .} — a
     * dotted type name would silently bind nothing unless written in brackets. This name works in
     * every notation.
     */
    public static final String EVENT_TYPE_NAME = "outboxer-stream-relay";

    /**
     * Typed key of the relay event type — the constant both the facade (producer side) and the
     * built-in handler register under (ADR-0031).
     */
    public static final EventType<StreamEnvelope> EVENT_TYPE =
            EventType.of(EVENT_TYPE_NAME, StreamEnvelope.class);

    public StreamEnvelope {
        Objects.requireNonNull(binding, "binding must not be null");
        if (binding.isBlank()) {
            throw new IllegalArgumentException("binding must not be blank");
        }
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        Objects.requireNonNull(contentType, "contentType must not be null");
        if (contentType.isBlank()) {
            throw new IllegalArgumentException("contentType must not be blank");
        }
        if ((textPayload == null) == (binaryPayload == null)) {
            throw new IllegalArgumentException(
                    "exactly one of textPayload/binaryPayload must be non-null");
        }
    }

    /** Value equality; the binary lane compares array contents, not identity. */
    @Override
    public boolean equals(@Nullable Object obj) {
        return obj instanceof StreamEnvelope other
                && binding.equals(other.binding)
                && Objects.equals(key, other.key)
                && headers.equals(other.headers)
                && contentType.equals(other.contentType)
                && Objects.equals(textPayload, other.textPayload)
                && Arrays.equals(binaryPayload, other.binaryPayload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                binding, key, headers, contentType, textPayload, Arrays.hashCode(binaryPayload));
    }

    /** Never dumps payload or header content — lane and sizes only (log hygiene). */
    @Override
    public String toString() {
        return "StreamEnvelope[binding="
                + binding
                + ", key="
                + key
                + ", headers="
                + headers.size()
                + ", contentType="
                + contentType
                + (textPayload != null
                        ? ", text,length=" + textPayload.length()
                        : ", bytes,length=" + (binaryPayload == null ? 0 : binaryPayload.length))
                + "]";
    }
}
